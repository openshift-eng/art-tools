import { Env, SiteConfig } from './types';
import { renderTemplFull } from './render';
import { getSiteConfig } from './config';
import { AccessChecker  } from './checkAccess';
import { Buffer } from "buffer";
import * as crypto from 'crypto';

const decode = (str: string):string => Buffer.from(str, 'base64').toString('binary');
const encode = (str: string):string => Buffer.from(str, 'binary').toString('base64');

// Helper function to sanitize paths and prevent path traversal attacks
function sanitizePath(path: string): string {
    // Decode URL-encoded characters that might hide path traversal
    let sanitized = decodeURIComponent(path);

    // Remove path traversal sequences
    sanitized = sanitized.replace(/\.\./g, '');

    // Normalize multiple slashes to single slash
    sanitized = sanitized.replace(/\/+/g, '/');

    // Remove any null bytes
    sanitized = sanitized.replace(/\0/g, '');

    // Ensure path starts with /
    if (!sanitized.startsWith('/')) {
        sanitized = '/' + sanitized;
    }

    return sanitized;
}

// Proxy function to forward the request to the target URL
async function proxyToTarget(request: Request, targetBase: string, remainingPath: string): Promise<Response> {
    // Sanitize the remaining path to prevent path traversal attacks
    const sanitizedPath = sanitizePath(remainingPath);

    // Construct the target URL
    const targetUrl = `${targetBase}${sanitizedPath}`;

    // Parse URLs for validation
    let parsedTarget: URL;
    let parsedBase: URL;

    try {
        parsedTarget = new URL(targetUrl);
        parsedBase = new URL(targetBase);
    } catch (e) {
        console.error('Invalid URL construction:', e);
        return new Response('Invalid URL', { status: 400 });
    }

    // Validation 1: Ensure the scheme is HTTPS (or HTTP if needed)
    const allowedSchemes = ['https:', 'http:'];
    if (!allowedSchemes.includes(parsedTarget.protocol)) {
        console.error('Blocked non-HTTP(S) scheme:', parsedTarget.protocol);
        return new Response('Invalid URL scheme', { status: 400 });
    }

    // Validation 2: Ensure the target URL's origin matches the base URL's origin
    // This prevents requests to arbitrary domains
    if (parsedTarget.origin !== parsedBase.origin) {
        console.error('Origin mismatch:', {
            target: parsedTarget.origin,
            base: parsedBase.origin
        });
        return new Response('Invalid target origin', { status: 400 });
    }

    // Validation 3: Ensure the path hasn't escaped the base path
    // This prevents path traversal attacks
    const normalizedTargetPath = parsedTarget.pathname;
    const normalizedBasePath = parsedBase.pathname.endsWith('/')
        ? parsedBase.pathname
        : parsedBase.pathname + '/';

    if (!normalizedTargetPath.startsWith(normalizedBasePath.replace(/\/+$/, ''))) {
        console.error('Path traversal attempt detected:', {
            targetPath: normalizedTargetPath,
            basePath: normalizedBasePath
        });
        return new Response('Invalid target path', { status: 400 });
    }

    // Filter headers - only allow safe headers
    const allowedHeaders = ["Content-Type", "Authorization", "Accept"];
    const filteredHeaders = new Headers();
    request.headers.forEach((value, key) => {
        if (allowedHeaders.includes(key)) {
            filteredHeaders.set(key, value);
        }
    });

    // Set up the new request
    const proxyRequest = new Request(targetUrl, {
        method: request.method,
        headers: filteredHeaders,
        body: request.method !== "GET" && request.method !== "HEAD" ? request.body : null,
    });

    try {
        // Fetch the response from the validated target URL
        const response = await fetch(proxyRequest);

        // Return the response to the client
        return new Response(response.body, response);
    } catch (error) {
        console.error('Proxy fetch failed:', error);
        return new Response('Proxy request failed', { status: 502 });
    }
}

async function listBucket(bucket: R2Bucket, options?: R2ListOptions, siteConfig: SiteConfig): Promise<R2Objects> {
    const requestOptions = {
        ...options,
        limit: siteConfig.limit,
    };

    const listed = await bucket.list({
        ...requestOptions,
    });

    let truncated = listed.truncated;
    let cursor = truncated ? listed.cursor : undefined;

    return {
        objects: listed.objects,
        delimitedPrefixes: listed.delimitedPrefixes,
        cursor: cursor,
    };
}

async function downloadFile(bucket: R2Bucket, filePath: string): Promise<Response> {
    // Fetch the file from the R2 bucket
    const object = await bucket.get(filePath);

    // Handle the case where the file is not found
    if (!object) {
        return new Response('File not found', { status: 404 });
    }

    // Create a response with the file content and appropriate headers
    const headers = new Headers();
    headers.set('Content-Type', object.httpMetadata?.contentType || 'application/octet-stream');
    headers.set('Content-Disposition', `attachment; filename="${filePath}"`);

    return new Response(object.body, {
        headers,
    });
}

function unauthorized(): Response {
    return new Response('Unauthorized', {
        status: 401,
        headers: {
            'WWW-Authenticate': 'Basic realm="Access to the site", charset="UTF-8"',
            'Content-Type': 'text/plain'
        }
    });
}

function shouldReturnOriginResponse(originResponse: Response, siteConfig: SiteConfig): boolean {
    // TODO: Add logic removing parameters from url and verifying that
    // the path is a dir and not a file
    // const isNotEndWithSlash = originResponse.url.slice(-1) !== '/';
    const isNotEndWithSlash = false;
    const is404 = originResponse.status === 404;
    const isZeroByte = originResponse.headers.get('Content-Length') === '0';
    const overwriteZeroByteObject = (siteConfig.dangerousOverwriteZeroByteObject ?? false) && isZeroByte;

    // order matters here
    if (isNotEndWithSlash) return true;
    if (is404) {
        return false;
    } else {
        return !overwriteZeroByteObject;
    }
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const originResponse = await fetch(request);

        const url = new URL(request.url);
        const domain = url.hostname;
        const cursor = url.searchParams.get("cursor") || undefined;

        let path = url.pathname;

        const siteConfig = getSiteConfig(env, domain);
        if (!siteConfig) {
            return originResponse;
        }

        // Proxy rule for CGW to Red Hat Content Gateway
        for (const [path, targetBase] of Object.entries(siteConfig.cgw)) {
            if (url.pathname.startsWith(path)) {
                return proxyToTarget(request, targetBase, url.pathname.replace(path, ""));
            }
        }

        // Prefixes that should be swapped on access
        for (const [prefix, link] of Object.entries(siteConfig.replace)) {
            if (path.startsWith(prefix)) {
                path = link + path.substring(prefix.length);
                break;
            }
        }

        // Strip off '/srv'. This was the original location I uploaded things to.
        // but it makes more sense for everything to be in the root.
        if (path.startsWith('/srv/enterprise/')) {
            path = path.substring(4); // Remove the first 4 characters
        }

        // If it is a private path, it needs to be authenticated
        // Anything not in /pub (or few exceptions) requires basic auth header
        if (!path.startsWith('/pub') && path !== '/favicon.ico' && path !== '/robots.txt' && path !== '/404.html') {
            // Anything not in /pub (or few exceptions) requires basic auth header
            const authorization = request.headers.get('authorization');
            if (!authorization) {
                if (path === '/') {
                    // Redirect to /pub if no authorization provided at root
                    return Response.redirect(`${url.protocol}//${url.host}/pub/`, 301);
                }

                return unauthorized();
            }

            // Split the authorization header (e.g., Basic <base64>)
            const auth_split = authorization.split(' ', 2);  // ['Basic', '<base64>']
            if (auth_split.length !== 2) {
                return unauthorized();
            }

            const [auth_schema, b64_auth_val] = auth_split;
            if (auth_schema.toLowerCase() !== "basic") {
                return unauthorized();
            }

            // Decode the base64 part of the authorization
            const auth_val = decode(b64_auth_val);
            const auth_val_split = auth_val.split(':', 2); // ['username', 'password']
            if (auth_val_split.length !== 2) {
                return unauthorized();
            }

            const [username, password] = auth_val_split;
            let authorized = false;
            const accessChecker = await AccessChecker.create(env);

            // Check if the URI is under enterprise or libra
            if (path.startsWith('/enterprise/') || path.startsWith('/libra/')) {
                const isAuthorized = await accessChecker.checkEnterpriseAccess(username, password);
                if (isAuthorized) {
                    authorized = true
                }
            }

            // Check if the URI is under pockets
            if (path.startsWith('/pockets/')) {
                const isAuthorized = await accessChecker.checkPocketAccess(username, password);
                if (isAuthorized) {
                    authorized = true
                }
            }

            if (!authorized) {
                return unauthorized();
            }
        }

        // remove the leading '/'
        const objectKey = siteConfig.decodeURI ? decodeURIComponent(path.slice(1)) : path.slice(1);
        const bucket = siteConfig.bucket;

        if (shouldReturnOriginResponse(originResponse, siteConfig)) {
            return downloadFile(bucket, objectKey);
        }

        const index = await listBucket(bucket, {
            prefix: objectKey,
            cursor,
            delimiter: '/',
            include: ['httpMetadata', 'customMetadata'],
        }, siteConfig);

        // filter out key===prefix, appears when dangerousOverwriteZeroByteObject===true
        const files = index.objects.filter((obj) => obj.key !== objectKey);
        const folders = index.delimitedPrefixes.filter((prefix) => prefix !== objectKey);

        // If no object found, return origin 404 response. Only return 404 because if there is a zero byte object,
        // user may want to show a empty folder.
        if (files.length === 0 && folders.length === 0 && originResponse.status === 404) {
            return originResponse;
        }

        return new Response(renderTemplFull(files, folders, '/' + objectKey, index.cursor, siteConfig), {
            headers: {
                'Content-Type': 'text/html; charset=utf-8',
            },
            status: 200,
        });
    },
};
