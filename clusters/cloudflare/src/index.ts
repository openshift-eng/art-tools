import { Env, SiteConfig } from './types';
import { renderTemplFull } from './render';
import { getSiteConfig } from './config';
import { AccessChecker  } from './checkAccess';
import { Buffer } from "buffer";
import * as crypto from 'crypto';

const decode = (str: string):string => Buffer.from(str, 'base64').toString('binary');
const encode = (str: string):string => Buffer.from(str, 'binary').toString('base64');

async function listBucket(bucket: R2Bucket, options?: R2ListOptions): Promise<R2Objects> {
    // List all objects in the bucket, launch new request if list is truncated
    const objects: R2Object[] = [];
    const delimitedPrefixes: string[] = [];

    // delete limit, cursor in passed options
    const requestOptions = {
        ...options,
        limit: undefined,
        cursor: undefined,
    };

    var cursor = undefined;
    while (true) {
        const index = await bucket.list({
            ...requestOptions,
            cursor,
        });
        objects.push(...index.objects);
        delimitedPrefixes.push(...index.delimitedPrefixes);
        if (!index.truncated) {
            break;
        }
        cursor = index.cursor;
    }
    return {
        objects,
        delimitedPrefixes,
        truncated: false,
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
    const isNotEndWithSlash = originResponse.url.slice(-1) !== '/';
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
        let path = url.pathname;

        const siteConfig = getSiteConfig(env, domain);
        if (!siteConfig) {
            // TODO: Should send a email to notify the admin
            return originResponse;
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

            // Check if the URI is under enterprise or libra
            if (path.startsWith('/enterprise/') || path.startsWith('/libra/')) {
                const accessChecker = await AccessChecker.create(env);
                const isAuthorized = await accessChecker.checkEnterpriseAccess(username, password);
                if (isAuthorized) {
                    authorized = true
                }
            }

            // Check if the URI is under pockets
            if (path.startsWith('/pockets/')) {
                const accessChecker = await AccessChecker.create(env);
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
            delimiter: '/',
            include: ['httpMetadata', 'customMetadata'],
        });
        // filter out key===prefix, appears when dangerousOverwriteZeroByteObject===true
        const files = index.objects.filter((obj) => obj.key !== objectKey);
        const folders = index.delimitedPrefixes.filter((prefix) => prefix !== objectKey);
        // If no object found, return origin 404 response. Only return 404 because if there is a zero byte object,
        // user may want to show a empty folder.
        if (files.length === 0 && folders.length === 0 && originResponse.status === 404) {
            return originResponse;
        }
        return new Response(renderTemplFull(files, folders, '/' + objectKey, siteConfig), {
            headers: {
                'Content-Type': 'text/html; charset=utf-8',
            },
            status: 200,
        });
    },
};
