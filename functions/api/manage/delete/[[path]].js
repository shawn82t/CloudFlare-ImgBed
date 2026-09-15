import { S3Client, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { purgeCFCache, purgeRandomFileListCache, purgePublicFileListCache } from "../../../utils/purgeCache";
import { readIndex, removeFileFromIndex, batchRemoveFilesFromIndex } from "../../../utils/indexManager.js";
import { getDatabase } from '../../../utils/databaseAdapter.js';
import { DiscordAPI } from '../../../utils/storage/discordAPI.js';
import { HuggingFaceAPI } from '../../../utils/storage/huggingfaceAPI.js';
import { WebDAVAPI } from '../../../utils/storage/webdavAPI.js';
import {
    resolveDiscordCredentials,
    resolveHuggingFaceCredentials,
    resolveS3Credentials,
    resolveWebDAVCredentials,
} from '../../../utils/metadata/channelCredentials.js';

// CORS 跨域响应头
const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
};

export async function onRequest(context) {
    const { request, env, params, waitUntil } = context;

    const url = new URL(request.url);

    // 读取folder参数，判断是否为文件夹删除请求
    const folder = url.searchParams.get('folder');
    if (folder === 'true') {
        try {
            let folderPath = decodeURIComponent(Array.isArray(params.path) ? params.path.join('/') : (params.path || ''));
            folderPath = folderPath.replace(/\.\./g, '_').replace(/\\/g, '/');
            if (folderPath.startsWith('/')) folderPath = folderPath.substring(1);
            if (folderPath && !folderPath.endsWith('/')) folderPath += '/';

            if (!folderPath) {
                return new Response(JSON.stringify({
                    success: false,
                    error: 'Cannot delete root directory'
                }), {
                    status: 400,
                    headers: { 'Content-Type': 'application/json', ...corsHeaders }
                });
            }

            const db = getDatabase(env);
            const deletedFiles = [];
            const failedFiles = [];
            const allFilesToDelete = new Set();

            // 1. 尝试从索引中读取该目录及其所有子孙文件（包含子目录递归）
            try {
                const listData = await readIndex(context, {
                    directory: folderPath,
                    count: -1,
                    includeSubdirFiles: true
                });
                if (listData && Array.isArray(listData.files)) {
                    for (const f of listData.files) {
                        const fid = f.id || f.name;
                        if (fid) allFilesToDelete.add(fid);
                    }
                }
            } catch (indexErr) {
                console.warn('readIndex failed during folder delete, falling back to database scan:', indexErr);
            }

            // 2. 直接扫描底层数据库（KV / D1），彻底保证所有物理文件（包括未建索引的文件）都被找到
            try {
                let cursor = null;
                while (true) {
                    const listResp = await db.list({
                        prefix: folderPath,
                        limit: 1000,
                        cursor: cursor
                    });
                    if (listResp && Array.isArray(listResp.keys)) {
                        for (const item of listResp.keys) {
                            if (!item.name.startsWith('manage@') && !item.name.startsWith('chunk_')) {
                                allFilesToDelete.add(item.name);
                            }
                        }
                        cursor = listResp.cursor;
                        if (!cursor) break;
                    } else {
                        break;
                    }
                }
            } catch (dbErr) {
                console.warn('Database scan failed during folder delete:', dbErr);
            }

            // 3. 逐个彻底物理删除所有文件（远端渠道、数据库、CDN）
            for (const fileId of allFilesToDelete) {
                const cdnUrl = `https://${url.hostname}/file/${fileId}`;
                const success = await deleteFile(env, fileId, cdnUrl, url);
                if (success) {
                    deletedFiles.push(fileId);
                } else {
                    failedFiles.push(fileId);
                }
            }

            // 4. 清除数据库中可能存在的空目录占位键
            try {
                await db.delete(folderPath);
                await db.delete(folderPath.replace(/\/+$/, ''));
            } catch (e) {
                // ignore
            }

            // 5. 必须 await 从索引中批量清除已删除的文件，确保在响应返回前索引更新完成！
            if (deletedFiles.length > 0) {
                await batchRemoveFilesFromIndex(context, deletedFiles);
            }

            // 6. 清理 CDN 与 API 缓存
            await purgeRandomFileListCache(url.origin, folderPath);
            await purgePublicFileListCache(url.origin, folderPath);

            return new Response(JSON.stringify({
                success: true,
                deleted: deletedFiles,
                failed: failedFiles
            }), {
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });

        } catch (e) {
            return new Response(JSON.stringify({
                success: false,
                error: e.message
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }
    }

    // 单个文件删除处理
    try {
        // 解码params.path
        params.path = decodeURIComponent(Array.isArray(params.path) ? params.path.join('/') : params.path);
        const fileId = params.path.split(',').join('/');
        const cdnUrl = `https://${url.hostname}/file/${fileId}`;

        const success = await deleteFile(env, fileId, cdnUrl, url);
        if (!success) {
            throw new Error('Delete file failed');
        } else {
            // 必须 await 从索引中删除文件，确保索引状态与存储一致
            await removeFileFromIndex(context, fileId);
        }

        return new Response(JSON.stringify({
            success: true,
            fileId: fileId
        }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    } catch (e) {
        return new Response(JSON.stringify({
            success: false,
            error: e.message
        }), {
            status: 400,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    }
}

// 删除单个文件的核心函数
export async function deleteFile(env, fileId, cdnUrl, url) {
    try {
        // 读取图片信息
        const db = getDatabase(env);
        const img = await db.getWithMetadata(fileId);

        // 如果文件记录不存在，直接返回成功（幂等删除）
        if (!img) {
            console.warn(`File ${fileId} not found in database, skipping delete`);
            return true;
        }

        // 如果是R2渠道的图片，需要删除R2中对应的图片
        if (img.metadata?.Channel === 'CloudflareR2') {
            const R2DataBase = env.img_r2;
            await R2DataBase.delete(fileId);
        }

        // S3 渠道的图片，需要删除S3中对应的图片
        if (img.metadata?.Channel === 'S3') {
            await deleteS3File(env, img);
        }

        // Discord 渠道的图片，需要删除 Discord 中对应的消息
        if (img.metadata?.Channel === 'Discord') {
            await deleteDiscordFile(env, img);
        }

        // HuggingFace 渠道的图片，需要删除 HuggingFace 中对应的文件
        if (img.metadata?.Channel === 'HuggingFace') {
            await deleteHuggingFaceFile(env, img);
        }

        // WebDAV 渠道的图片，需要删除 WebDAV 中对应的文件
        if (img.metadata?.Channel === 'WebDAV') {
            await deleteWebDAVFile(env, img);
        }

        // 删除数据库中的记录
        // 注意：容量统计现在由索引自动维护，删除文件后索引更新时会自动重新计算
        await db.delete(fileId);

        // 清除CDN缓存
        await purgeCFCache(env, cdnUrl);

        // 清除 api/randomFileList 等API缓存
        const normalizedFolder = fileId.split('/').slice(0, -1).join('/');
        await purgeRandomFileListCache(url.origin, normalizedFolder);
        await purgePublicFileListCache(url.origin, normalizedFolder);

        return true;
    } catch (e) {
        console.error('Delete file failed:', e);
        return false;
    }
}

// 删除 S3 渠道的图片
async function deleteS3File(env, img) {
    const db = getDatabase(env);
    const s3Credentials = await resolveS3Credentials(db, env, img.metadata);
    const s3Client = new S3Client({
        region: s3Credentials.region || "auto",
        endpoint: s3Credentials.endpoint,
        credentials: {
            accessKeyId: s3Credentials.accessKeyId,
            secretAccessKey: s3Credentials.secretAccessKey
        },
        forcePathStyle: s3Credentials.pathStyle || false // 是否启用路径风格
    });

    const bucketName = s3Credentials.bucketName;
    const key = s3Credentials.key;

    try {
        await s3Client.send(new DeleteObjectCommand({
            Bucket: bucketName,
            Key: key,
        }));
        return true;
    } catch (error) {
        console.error("S3 Delete Failed:", error);
        return false;
    }
}

// 删除 Discord 渠道的图片（删除 Discord 消息）
async function deleteDiscordFile(env, img) {
    const db = getDatabase(env);
    const discordCredentials = await resolveDiscordCredentials(db, env, img.metadata);
    const botToken = discordCredentials.botToken;
    const channelId = discordCredentials.channelId;
    const messageId = discordCredentials.messageId;

    if (!botToken || !channelId || !messageId) {
        console.warn('Discord file missing required metadata for deletion');
        return false;
    }

    try {
        const discordAPI = new DiscordAPI(botToken);
        const success = await discordAPI.deleteMessage(channelId, messageId);
        if (!success) {
            console.error('Discord Delete Failed: API returned false');
        }
        return success;
    } catch (error) {
        console.error("Discord Delete Failed:", error);
        return false;
    }
}


// 删除 HuggingFace 渠道的图片
async function deleteHuggingFaceFile(env, img) {
    const db = getDatabase(env);
    const hfCredentials = await resolveHuggingFaceCredentials(db, env, img.metadata);
    const token = hfCredentials.token;
    const repo = hfCredentials.repo;
    const filePath = hfCredentials.filePath;
    const isPrivate = hfCredentials.isPrivate || false;

    if (!token || !repo || !filePath) {
        console.warn('HuggingFace file missing required metadata for deletion');
        return false;
    }

    try {
        const huggingfaceAPI = new HuggingFaceAPI(token, repo, isPrivate);
        const success = await huggingfaceAPI.deleteFile(filePath, `Delete ${filePath}`);
        if (!success) {
            console.error('HuggingFace Delete Failed: API returned false');
        }
        return success;
    } catch (error) {
        console.error("HuggingFace Delete Failed:", error);
        return false;
    }
}


// 删除 WebDAV 渠道的图片
async function deleteWebDAVFile(env, img) {
    const filePath = img.metadata?.WebDAVFilePath;

    if (!filePath) {
        console.warn('WebDAV file missing required metadata for deletion');
        return false;
    }

    try {
        const db = getDatabase(env);
        const webdavCredentials = await resolveWebDAVCredentials(db, env, img.metadata);
        if (!webdavCredentials.baseUrl) {
            console.warn('WebDAV channel config not found for deletion');
            return false;
        }

        const webdavAPI = new WebDAVAPI(webdavCredentials);
        return await webdavAPI.deleteFile(filePath);
    } catch (error) {
        console.error("WebDAV Delete Failed:", error);
        return false;
    }
}
