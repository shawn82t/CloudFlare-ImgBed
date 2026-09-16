import { addFileToIndex, mergeOperationsToIndex } from '../../../utils/indexManager.js';
import { getDatabase } from '../../../utils/databaseAdapter.js';
import { buildFileMetadataForManagement } from '../../../utils/metadata/metadataView.js';
import { cleanPersistedMetadata } from '../../../utils/metadata/metadataSecurity.js';
import {
    resolveDiscordCredentials,
    resolveHuggingFaceCredentials,
    resolveS3Credentials,
    resolveTelegramCredentials,
    resolveWebDAVCredentials,
} from '../../../utils/metadata/channelCredentials.js';
import { TelegramAPI } from '../../../utils/storage/telegramAPI.js';
import { DiscordAPI } from '../../../utils/storage/discordAPI.js';
import { HuggingFaceAPI } from '../../../utils/storage/huggingfaceAPI.js';
import { WebDAVAPI } from '../../../utils/storage/webdavAPI.js';
import { purgeCDNCache } from '../../../upload/uploadTools.js';
import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';

// CORS 跨域响应头
const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
};

// 单次在线编辑文本大小上限：5MB（防边缘函数内存溢出）
const MAX_EDIT_SIZE_BYTES = 5 * 1024 * 1024;

export async function onRequest(context) {
    const { request, env, params } = context;

    // OPTIONS 预检请求
    if (request.method === 'OPTIONS') {
        return new Response(null, {
            status: 204,
            headers: corsHeaders,
        });
    }

    // 从 params.path 解析 fileId
    let fileId = '';
    try {
        fileId = decodeURIComponent(params.path).split(',').join('/');
    } catch (e) {
        return new Response(JSON.stringify({
            success: false,
            message: 'Error: Decode File ID Failed',
        }), {
            status: 400,
            headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
    }

    if (!fileId) {
        return new Response(JSON.stringify({
            success: false,
            message: 'File ID is required.',
        }), {
            status: 400,
            headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
    }

    if (request.method === 'GET') {
        return await handleGetFileContent(context, fileId);
    } else if (request.method === 'PUT') {
        return await handleSaveFileContent(context, fileId);
    } else {
        return new Response(JSON.stringify({
            success: false,
            message: 'Method not allowed. Use GET or PUT.',
        }), {
            status: 405,
            headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
    }
}

/**
 * 获取文件文本内容
 */
async function handleGetFileContent(context, fileId) {
    const { env, request } = context;
    const db = getDatabase(env);

    try {
        const fileData = await db.getWithMetadata(fileId);
        if (!fileData || !fileData.metadata) {
            return new Response(JSON.stringify({
                success: false,
                message: 'File not found in database.',
            }), {
                status: 404,
                headers: { 'Content-Type': 'application/json', ...corsHeaders },
            });
        }

        const metadata = fileData.metadata;
        const fileSize = Number(metadata.FileSizeBytes || (Number(metadata.FileSize) * 1024 * 1024) || 0);
        if (fileSize > MAX_EDIT_SIZE_BYTES) {
            return new Response(JSON.stringify({
                success: false,
                message: `File size exceeds the 5MB online editing limit (${(fileSize / 1024 / 1024).toFixed(2)}MB).`,
            }), {
                status: 413,
                headers: { 'Content-Type': 'application/json', ...corsHeaders },
            });
        }

        let content = '';
        const channel = metadata.Channel || '';

        // 优先根据渠道直接拉取原始内容
        if (channel === 'Telegram' || channel === 'TelegramNew') {
            const tgCredentials = await resolveTelegramCredentials(db, env, metadata);
            const tgFileId = metadata.TgFileId || (channel === 'Telegram' ? fileId.split('.')[0] : null);
            if (!tgFileId) {
                throw new Error('Telegram file ID missing');
            }
            const tgApi = new TelegramAPI(tgCredentials.botToken, tgCredentials.proxyUrl || '');
            const filePath = await tgApi.getFilePath(tgFileId);
            if (!filePath) {
                throw new Error('Failed to resolve Telegram file path');
            }
            const fileDomain = tgCredentials.proxyUrl ? `https://${tgCredentials.proxyUrl}` : 'https://api.telegram.org';
            const targetUrl = `${fileDomain}/file/bot${tgCredentials.botToken}/${filePath}`;
            const fileRes = await fetch(targetUrl);
            if (!fileRes.ok) {
                throw new Error(`Telegram download failed with status: ${fileRes.status}`);
            }
            content = await fileRes.text();
        } else if (channel === 'CloudflareR2') {
            if (!env.img_r2) {
                throw new Error('Cloudflare R2 is not configured');
            }
            const r2Object = await env.img_r2.get(fileId);
            if (!r2Object) {
                throw new Error('File not found in Cloudflare R2');
            }
            content = await r2Object.text();
        } else if (channel === 'WebDAV') {
            const creds = await resolveWebDAVCredentials(db, env, metadata);
            const webdavAPI = new WebDAVAPI(creds);
            const targetPath = creds.filePath || fileId;
            const res = await webdavAPI.getFile(targetPath);
            if (!res.ok) {
                throw new Error(`WebDAV download failed with status: ${res.status}`);
            }
            content = await res.text();
        } else if (channel === 'HuggingFace') {
            const creds = await resolveHuggingFaceCredentials(db, env, metadata);
            const hfApi = new HuggingFaceAPI(creds.token, creds.repo, creds.isPrivate);
            const targetPath = creds.filePath || fileId;
            const res = await hfApi.getFile(targetPath);
            if (!res.ok) {
                throw new Error(`HuggingFace download failed with status: ${res.status}`);
            }
            content = await res.text();
        } else if (channel === 'S3') {
            const creds = await resolveS3Credentials(db, env, metadata);
            const s3Client = new S3Client({
                region: creds.region || 'auto',
                endpoint: creds.endpoint,
                credentials: {
                    accessKeyId: creds.accessKeyId,
                    secretAccessKey: creds.secretAccessKey,
                },
                forcePathStyle: creds.pathStyle,
            });
            const command = new GetObjectCommand({
                Bucket: creds.bucketName,
                Key: creds.key || fileId,
            });
            const s3Res = await s3Client.send(command);
            content = await s3Res.Body.transformToString();
        } else if (channel === 'External') {
            if (!metadata.ExternalLink) {
                throw new Error('External link missing');
            }
            const extRes = await fetch(metadata.ExternalLink);
            if (!extRes.ok) {
                throw new Error(`External fetch failed with status: ${extRes.status}`);
            }
            content = await extRes.text();
        } else {
            // 兜底：通过内部文件路由获取
            const url = new URL(request.url);
            const fallbackUrl = `${url.origin}/file/${fileId}`;
            const fallbackRes = await fetch(fallbackUrl);
            if (!fallbackRes.ok) {
                throw new Error(`Internal file fetch failed with status: ${fallbackRes.status}`);
            }
            content = await fallbackRes.text();
        }

        return new Response(JSON.stringify({
            success: true,
            content,
            fileName: metadata.FileName || fileId,
            fileType: metadata.FileType || 'text/plain',
            fileSize: metadata.FileSize || '0',
            channel: metadata.Channel || '',
        }), {
            status: 200,
            headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });

    } catch (error) {
        console.error('Error fetching file content:', error);
        return new Response(JSON.stringify({
            success: false,
            message: error.message || 'Failed to read file content.',
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
    }
}

/**
 * 覆写保存文件文本内容
 */
async function handleSaveFileContent(context, fileId) {
    const { env, request, url } = context;
    const db = getDatabase(env);

    try {
        let body;
        try {
            body = await request.json();
        } catch (e) {
            return new Response(JSON.stringify({
                success: false,
                message: 'Invalid request body. Expected JSON.',
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders },
            });
        }

        if (typeof body.content !== 'string') {
            return new Response(JSON.stringify({
                success: false,
                message: 'Field "content" (string) is required.',
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders },
            });
        }

        const newContent = body.content;
        const newSizeInBytes = new TextEncoder().encode(newContent).length;

        if (newSizeInBytes > MAX_EDIT_SIZE_BYTES) {
            return new Response(JSON.stringify({
                success: false,
                message: `Content size exceeds the 5MB online editing limit (${(newSizeInBytes / 1024 / 1024).toFixed(2)}MB).`,
            }), {
                status: 413,
                headers: { 'Content-Type': 'application/json', ...corsHeaders },
            });
        }

        const fileData = await db.getWithMetadata(fileId);
        if (!fileData || !fileData.metadata) {
            return new Response(JSON.stringify({
                success: false,
                message: 'File not found.',
            }), {
                status: 404,
                headers: { 'Content-Type': 'application/json', ...corsHeaders },
            });
        }

        const metadata = { ...fileData.metadata };
        const channel = metadata.Channel || 'TelegramNew';
        const pureFileName = metadata.FileName?.split('/')?.pop() || fileId.split('/')?.pop() || 'file.txt';
        const fileType = metadata.FileType || 'text/plain; charset=utf-8';

        // 根据渠道覆写物理存储
        if (channel === 'Telegram' || channel === 'TelegramNew') {
            const tgCredentials = await resolveTelegramCredentials(db, env, metadata);
            if (!tgCredentials.botToken || !tgCredentials.chatId) {
                throw new Error('Telegram credentials not properly configured');
            }
            const tgApi = new TelegramAPI(tgCredentials.botToken, tgCredentials.proxyUrl || '');
            const blob = new Blob([newContent], { type: fileType });
            const fileObj = new File([blob], pureFileName, { type: fileType });
            const response = await tgApi.sendFile(fileObj, tgCredentials.chatId, 'sendDocument', 'document', '', pureFileName);
            const fileInfo = tgApi.getFileInfo(response);
            if (!fileInfo || !fileInfo.file_id) {
                throw new Error('Failed to obtain new Telegram file ID after upload');
            }
            metadata.TgFileId = fileInfo.file_id;
            metadata.Channel = 'TelegramNew';
        } else if (channel === 'CloudflareR2') {
            if (!env.img_r2) {
                throw new Error('Cloudflare R2 is not configured');
            }
            await env.img_r2.put(fileId, newContent, {
                httpMetadata: { contentType: fileType },
            });
        } else if (channel === 'WebDAV') {
            const creds = await resolveWebDAVCredentials(db, env, metadata);
            const webdavAPI = new WebDAVAPI(creds);
            const targetPath = creds.filePath || fileId;
            const blob = new Blob([newContent], { type: fileType });
            await webdavAPI.putFile(targetPath, blob, fileType);
        } else if (channel === 'HuggingFace') {
            const creds = await resolveHuggingFaceCredentials(db, env, metadata);
            const hfApi = new HuggingFaceAPI(creds.token, creds.repo, creds.isPrivate);
            const targetPath = creds.filePath || fileId;
            const blob = new Blob([newContent], { type: fileType });
            await hfApi.uploadFile(blob, targetPath, `Update ${pureFileName}`);
        } else if (channel === 'Discord') {
            const creds = await resolveDiscordCredentials(db, env, metadata);
            const discordApi = new DiscordAPI(creds.botToken, creds.proxyUrl || '');
            const blob = new Blob([newContent], { type: fileType });
            const fileObj = new File([blob], pureFileName, { type: fileType });
            const response = await discordApi.sendFile(fileObj, creds.channelId, pureFileName);
            const fileInfo = discordApi.getFileInfo(response);
            if (fileInfo?.message_id) {
                metadata.DiscordMessageId = fileInfo.message_id;
            }
        } else if (channel === 'S3') {
            const creds = await resolveS3Credentials(db, env, metadata);
            const s3Client = new S3Client({
                region: creds.region || 'auto',
                endpoint: creds.endpoint,
                credentials: {
                    accessKeyId: creds.accessKeyId,
                    secretAccessKey: creds.secretAccessKey,
                },
                forcePathStyle: creds.pathStyle,
            });
            await s3Client.send(new PutObjectCommand({
                Bucket: creds.bucketName,
                Key: creds.key || fileId,
                Body: newContent,
                ContentType: fileType,
            }));
        } else {
            throw new Error(`Editing not supported for storage channel: ${channel}`);
        }

        // 更新元数据大小与修改时间
        metadata.FileSize = (newSizeInBytes / 1024 / 1024).toFixed(2);
        metadata.FileSizeBytes = newSizeInBytes;
        metadata.TimeStamp = Date.now();

        const metadataToSave = cleanPersistedMetadata(metadata);

        // 写入数据库持久化
        await db.put(fileId, fileData.value || '', { metadata: metadataToSave });

        // 更新索引并立即合并落盘
        await addFileToIndex(context, fileId, metadataToSave);
        await mergeOperationsToIndex(context);

        // 清除边缘 CDN 缓存
        const cdnUrl = `https://${url.hostname}/file/${fileId}`;
        const folderPart = fileId.includes('/') ? fileId.substring(0, fileId.lastIndexOf('/')) : '';
        await purgeCDNCache(env, cdnUrl, url, folderPart);

        return new Response(JSON.stringify({
            success: true,
            metadata: await buildFileMetadataForManagement(db, env, metadataToSave),
        }), {
            status: 200,
            headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });

    } catch (error) {
        console.error('Error saving file content:', error);
        return new Response(JSON.stringify({
            success: false,
            message: error.message || 'Internal server error while saving file.',
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
    }
}
