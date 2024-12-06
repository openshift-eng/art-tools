# rhcos.mirror.openshift.com

rhcos.mirror.openshift.com will supersede rhcos-redirector to serve
RHCOS boot image downloads through CloudFront CDN.

## How it works
An example request and response:
- A user requests https://rhcos.mirror.openshift.com/art/storage/releases/rhcos-4.11-aarch64/411.85.202205040359-0/aarch64/rhcos-411.85.202205040359-0-aws.aarch64.vmdk.gz;
- The request goes to CloudFront Distribution [E3DTOB2AA6Z2NE][1];
- A Lambda@Edge Function [srv_rhcos_mirror.py][2] is run to change the request URI (`/art/storage/releases/rhcos-4.11-aarch64/411.85.202205040359-0/aarch64/rhcos-411.85.202205040359-0-aws.aarch64.vmdk.gz`) and make it appropriate for the art-rhcos-ci S3 bucket (`/releases/rhcos-4.11-aarch64/411.85.202205040359-0/aarch64/rhcos-411.85.202205040359-0-aws.aarch64.vmdk.gz`). If the request comes from predefined EC2 IP ranges, it will redirect the request to the S3 bucket to help with cost management.
- If the requested file is not in the cache (a cache miss), then CloudFront sends a request to the origin (`art-rhcos-ci` S3 bucket) to get the file. After getting the file, CloudFront returns it to the user and stores it in the edge location’s cache.
- If there's a cache hit, CloudFront serves the cached object to the user immediately, without sending a request to the origin.

## Lambda@Edge function lambda_srv_rhcos_mirror
The source file is at <lambda_srv_rhcos_mirror.py>. If you made a change to this file, you must update it on CloudFront as well. Currently there is no automation to sync it to CloudFront.

## Generating EC2 IP ranges
<lambda_srv_rhcos_mirror.py> redirects viewer requests coming from predefined EC2 IP ranges to the S3 bucket to help with cost management. The IP ranges are defined by `AWS_EC2_REGION_IP_RANGES` in <lambda_srv_rhcos_mirror.py>. Script[generate_range_array.py](../generate_range_array.py) can be run locally to generate the IP ranges.

[1]: https://us-east-1.console.aws.amazon.com/cloudfront/v3/home?region=us-east-1#/distributions/E3DTOB2AA6Z2NE
[2]: https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/srv_rhcos_mirror/versions/1?tab=code
