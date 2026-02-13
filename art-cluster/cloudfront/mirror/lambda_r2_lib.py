import boto3
from botocore.client import Config

# CloudFlare emulates the S3 API. "S3" is referenced regularly, but due to the
# ENDPOINT_URL, requests from boto3 are actually sent to CloudFlare and not
# AWS.
S3_BUCKET_NAME = "art-srv-enterprise"
S3_REGION_NAME = "us-east-1"


# Ensure s3v4 signature is used regardless of the region the lambda is executing in.
BOTO3_CLIENT_CONFIG = Config(signature_version="s3v4")
# According to https://docs.aws.amazon.com/codeguru/detector-library/python/lambda-client-reuse/
# s3 clients can and should be reused. This allows the client to be cached in an execution
# environment and reused if possible. Initialize these lazily so we can handle ANY s3 errors
# inline with the request.
s3_client = None
secrets_client = None


def get_secrets_manager_secret_dict(secret_name):
    global secrets_client

    import json  # lazy load to avoid a largish library if lambda does not need secret access

    if secrets_client is None:
        # We need to read in the secret from AWS SecretManager. No authentication
        # or endpoint information is required because the lambda is running with
        # a role that allows access to necessary secrets.
        secrets_client = boto3.client(
            service_name="secretsmanager",
            region_name="us-east-1",
        )

    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name,
        )
    except:
        raise

    # Assume it is a key/value pair secret and parse as json
    username_password_keypairs_str = get_secret_value_response["SecretString"]
    return json.loads(username_password_keypairs_str)


def get_r2_s3_client():
    global s3_client

    # If we have not initialized an R2 client, do so now.
    if s3_client is None:
        cloudflare_r2_bucket_info = get_secrets_manager_secret_dict(
            "prod/lambda/cloudflare-r2-art-srv-enterprise-read-only"
        )
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=cloudflare_r2_bucket_info["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=cloudflare_r2_bucket_info["AWS_SECRET_ACCESS_KEY"],
            endpoint_url=cloudflare_r2_bucket_info["AWS_ENDPOINT_URL"],
            region_name=S3_REGION_NAME,
            config=BOTO3_CLIENT_CONFIG,
        )

    return s3_client
