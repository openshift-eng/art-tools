import base64
import hmac
import os
from typing import Dict, List, Optional
from urllib.parse import quote, unquote_plus

import boto3
from lambda_r2_lib import get_r2_s3_client, get_secrets_manager_secret_dict, S3_BUCKET_NAME

# Dicts of usernames and passwords which will be populated from SecretsManager
ENTERPRISE_SERVICE_ACCOUNTS = None
POCKET_SERVICE_ACCOUNTS = None


def unauthorized():
    return {
        'status': 401,
        'statusDescription': 'Unauthorized',
        'headers': {
            'www-authenticate': [{
                'key': 'WWW-Authenticate',
                'value': 'Basic'
            }],
        }
    }


def redirect(uri: str, code: int = 302, description="Found"):
    return {
        'status': code,
        'statusDescription': description,
        'headers': {
            "location": [{
                'key': 'Location',
                "value": str(uri)
            }],
        }
    }


class KeyifyList(object):
    """ bisect does not support key= until 3.10. Eliminate this when Lambda supports 3.10.
    """

    def __init__(self, inner, key):
        self.inner = inner
        self.key = key

    def __len__(self):
        return len(self.inner)

    def __getitem__(self, k):
        return self.key(self.inner[k])


def lambda_handler(event: Dict, context: Dict):
    global ENTERPRISE_SERVICE_ACCOUNTS
    global POCKET_SERVICE_ACCOUNTS

    request: Dict = event['Records'][0]['cf']['request']
    uri: str = request['uri']
    headers: Dict[str, List[Dict[str, str]]] = request['headers']

    if uri.startswith('/srv/enterprise/'):
        # Strip off '/srv'. This was the original location I uploaded things to.
        # but it makes more sense for everything to be in the root.
        uri = uri[4:]

    # prefixes that should be swapped on access; used to be done with symlinks on mirror.
    links = {
        '/pub/openshift-v4/amd64/': '/pub/openshift-v4/x86_64/',
        '/pub/openshift-v4/arm64/': '/pub/openshift-v4/aarch64/',
        '/pub/openshift-v4/clients/': '/pub/openshift-v4/x86_64/clients/',
        '/pub/openshift-v4/dependencies/': '/pub/openshift-v4/x86_64/dependencies/',
    }
    for prefix, link in links.items():
        if uri.startswith(prefix):
            uri = link + uri[len(prefix):]
            break

    if not uri.startswith('/pub') and uri != '/favicon.ico' and uri != '/robots.txt' and uri != '/404.html':
        # Anything not in /pub (or few exceptions) requires basic auth header
        authorization = headers.get("authorization", [])
        if not authorization:
            if uri == '/':
                # The one exception is if the user hits / without auth, we try to be friendly and redirect them..
                return redirect("/pub/")
            return unauthorized()
        auth_split = authorization[0]["value"].split(
            maxsplit=1)  # Basic <base64> => ['Basic', '<base64>']
        if len(auth_split) != 2:
            return unauthorized()
        auth_schema, b64_auth_val = auth_split
        if auth_schema.lower() != "basic":
            return unauthorized()
        auth_val: str = base64.b64decode(b64_auth_val).decode()
        auth_val_split = auth_val.split(':', maxsplit=1)
        if len(auth_val_split) != 2:
            return unauthorized()
        username, password = auth_val_split

        authorized = False

        # /libra is an ancient location on the old mirrors. It was synchronized
        # to the s3 bucket once in order to not break any service delivery
        # system which relied on it. It is not kept up-to-date.
        if uri.startswith('/enterprise/') or uri.startswith('/libra/'):
            if not ENTERPRISE_SERVICE_ACCOUNTS:
                ENTERPRISE_SERVICE_ACCOUNTS = get_secrets_manager_secret_dict(
                    'art_srv_request_basic_auth/ENTERPRISE_SERVICE_ACCOUNTS')

            if username in ENTERPRISE_SERVICE_ACCOUNTS:
                # like `==`, but in a timing-safe way
                if hmac.compare_digest(password, ENTERPRISE_SERVICE_ACCOUNTS[username]):
                    authorized = True

        # Pockets provide a means of authenticated / private access for users to a particular
        # set of mirror artifacts. A pocket user should only be able to access the pocket
        # associated with their service account and not all pockets.
        if uri.startswith('/pockets/'):
            # The username for pockets should be of the form '<pocketName>+<anonymized user id>' . Extract the pocket
            # name. The user must only have access to the pocket specified in their username.
            if username.index('+') > 0:
                if not POCKET_SERVICE_ACCOUNTS:
                    POCKET_SERVICE_ACCOUNTS = get_secrets_manager_secret_dict(
                        'art_srv_request_basic_auth/POCKET_SERVICE_ACCOUNTS')
                pocket_name = username.split('+')[0]
                if uri.startswith(f'/pockets/{pocket_name}/'):
                    if username in POCKET_SERVICE_ACCOUNTS:
                        if hmac.compare_digest(password, POCKET_SERVICE_ACCOUNTS[username]):
                            authorized = True

        if not authorized:
            return unauthorized()

    # Check whether the URI is missing a file name.
    if uri.endswith("/"):
        uri += 'index.html'
    elif uri.endswith('index.html'):
        # Allow the request to pass through to an origin
        # request function. That will be in the index.html
        # generator.
        pass
    elif uri == '/404.html':
        # We don't want the browser to redirect to an R2 URL whenever it needs
        # to display an error page. This page should, therefore reside on the
        # ACTUAL S3 bucket origin in AWS. The 404.html file should be read
        # from there and streamed back from CloudFRONT.
        pass
    else:
        # If we have not initialized an R2 client, do so now.
        s3_client = get_r2_s3_client()

        url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': S3_BUCKET_NAME,
                'Key': unquote_plus(uri[1:]),  # Strip '/'
            },
            ExpiresIn=20 * 60,  # Expire in 20 minutes
        )
        # Redirect the request to S3 bucket for cost management
        return redirect(url, code=307, description='S3Redirect')

    # Some clients may send in URL with literal '+' and other chars that need to be escaped
    # in order for the URL to resolve via an S3 HTTP request. decoding and then
    # re-encoding should ensure that clients that do or don't encode will always
    # head toward the S3 origin encoded.
    request['uri'] = quote(unquote_plus(uri))
    return request
