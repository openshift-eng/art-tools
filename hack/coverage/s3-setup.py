#!/usr/bin/env python3
"""
Deploy the coverage S3 infrastructure via CloudFormation and write a
config file that can be fed to pull-secret-mod.py via -f.

The config is written to hack/coverage/configs/<bucket>.json (gitignored).

Usage:
  python3 s3-setup.py --bucket BUCKET --region REGION [--profile PROFILE]
                      [--stack-name NAME] [--base-path PATH]

Prerequisites:
  - AWS CLI credentials configured (aws configure) or environment variables.
  - boto3 installed (pip install boto3).
"""

import argparse
import json
import os
import sys
import time

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("ERROR: boto3 is required.  Install with: pip install boto3", file=sys.stderr)
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CFN_TEMPLATE = os.path.join(SCRIPT_DIR, "coverage-s3.cfn.yaml")

DEFAULTS = {
    "stack_name": "coverage-s3-infra",
    "base_path": "openshift-ci/coverage",
}

# Config files are written into this directory (gitignored).
CONFIGS_DIR = os.path.join(SCRIPT_DIR, "configs")


def parse_args():
    p = argparse.ArgumentParser(
        description="Deploy coverage S3 infrastructure and write a config file.",
    )
    p.add_argument("--bucket", required=True,
                    help="S3 bucket name (required)")
    p.add_argument("--region", required=True,
                    help="AWS region (required)")
    p.add_argument("--stack-name", default=DEFAULTS["stack_name"],
                    help=f"CloudFormation stack name (default: {DEFAULTS['stack_name']})")
    p.add_argument("--base-path", default=DEFAULTS["base_path"],
                    help=f"S3 base path prefix (default: {DEFAULTS['base_path']})")
    p.add_argument("--profile", default=None,
                    help="AWS CLI profile name (default: session default)")
    return p.parse_args()


def read_template():
    if not os.path.isfile(CFN_TEMPLATE):
        print(f"ERROR: CloudFormation template not found at {CFN_TEMPLATE}", file=sys.stderr)
        sys.exit(1)
    with open(CFN_TEMPLATE) as f:
        return f.read()


def wait_for_stack(cfn, stack_name: str):
    """Poll until the stack reaches a terminal state."""
    print(f"  Waiting for stack '{stack_name}'...", end="", flush=True)
    while True:
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
        except ClientError as e:
            print(f"\nERROR: {e}", file=sys.stderr)
            sys.exit(1)

        status = resp["Stacks"][0]["StackStatus"]
        if status.endswith("_COMPLETE"):
            print(f" {status}")
            return resp["Stacks"][0]
        if status.endswith("_FAILED") or status.endswith("ROLLBACK_COMPLETE"):
            reason = resp["Stacks"][0].get("StackStatusReason", "unknown")
            print(f" {status}")
            print(f"ERROR: Stack failed: {reason}", file=sys.stderr)
            # Print the events for debugging
            events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
            for ev in events[:10]:
                if "FAILED" in ev.get("ResourceStatus", ""):
                    print(f"  {ev['LogicalResourceId']}: {ev.get('ResourceStatusReason', '')}", file=sys.stderr)
            sys.exit(1)
        print(".", end="", flush=True)
        time.sleep(5)


def get_outputs(stack: dict) -> dict:
    """Convert stack Outputs list to a dict."""
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def main():
    args = parse_args()

    session_kwargs = {}
    if args.region:
        session_kwargs["region_name"] = args.region
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    session = boto3.Session(**session_kwargs)
    region = session.region_name
    if not region:
        print("ERROR: No AWS region configured. Use --region or aws configure.", file=sys.stderr)
        sys.exit(1)

    # Ensure the S3 bucket exists (idempotent — no error if it already exists)
    s3 = session.client("s3")
    print(f"Ensuring S3 bucket '{args.bucket}' exists in {region}...")
    try:
        s3.head_bucket(Bucket=args.bucket)
        print(f"  Bucket '{args.bucket}' already exists.")
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            create_kwargs = {"Bucket": args.bucket}
            # us-east-1 must NOT include a LocationConstraint
            if region != "us-east-1":
                create_kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": region,
                }
            s3.create_bucket(**create_kwargs)
            # Add lifecycle rule to expire old coverage data
            s3.put_bucket_lifecycle_configuration(
                Bucket=args.bucket,
                LifecycleConfiguration={
                    "Rules": [{
                        "ID": "ExpireOldCoverage",
                        "Status": "Enabled",
                        "Expiration": {"Days": 90},
                        "Filter": {"Prefix": ""},
                    }],
                },
            )
            print(f"  Created bucket '{args.bucket}' with 90-day lifecycle rule.")
        elif error_code == 403:
            # Bucket exists but is owned by another account
            print(f"ERROR: Bucket '{args.bucket}' exists but you don't have access.", file=sys.stderr)
            print("  Choose a different bucket name with --bucket.", file=sys.stderr)
            sys.exit(1)
        else:
            raise

    cfn = session.client("cloudformation")
    template_body = read_template()

    params = [
        {"ParameterKey": "BucketName", "ParameterValue": args.bucket},
        {"ParameterKey": "S3BasePath", "ParameterValue": args.base_path},
    ]

    # Check if stack already exists and its status
    stack_status = None
    try:
        resp = cfn.describe_stacks(StackName=args.stack_name)
        stack_status = resp["Stacks"][0]["StackStatus"]
    except ClientError:
        pass  # Stack does not exist

    # Stacks in a failed/rollback state cannot be updated — delete first.
    # Wait for any in-progress rollback to finish before deleting.
    if stack_status and ("ROLLBACK" in stack_status or "FAILED" in stack_status):
        if "IN_PROGRESS" in stack_status:
            print(f"Stack '{args.stack_name}' is in {stack_status}; waiting for it to settle...")
            waiter = cfn.get_waiter("stack_rollback_complete")
            try:
                waiter.wait(StackName=args.stack_name, WaiterConfig={"Delay": 5, "MaxAttempts": 60})
            except Exception:
                pass  # Best effort — proceed to delete regardless
            resp = cfn.describe_stacks(StackName=args.stack_name)
            stack_status = resp["Stacks"][0]["StackStatus"]
            print(f"  Stack is now {stack_status}.")

        print(f"Stack '{args.stack_name}' is in {stack_status}; deleting before re-creation...")
        cfn.delete_stack(StackName=args.stack_name)
        waiter = cfn.get_waiter("stack_delete_complete")
        waiter.wait(StackName=args.stack_name, WaiterConfig={"Delay": 5, "MaxAttempts": 60})
        print("  Deleted.")
        stack_status = None

    if stack_status:
        print(f"Stack '{args.stack_name}' already exists ({stack_status}). Updating...")
        try:
            cfn.update_stack(
                StackName=args.stack_name,
                TemplateBody=template_body,
                Parameters=params,
                Capabilities=["CAPABILITY_NAMED_IAM"],
            )
        except ClientError as e:
            if "No updates are to be performed" in str(e):
                print("  No changes detected; using existing outputs.")
                resp = cfn.describe_stacks(StackName=args.stack_name)
                stack = resp["Stacks"][0]
            else:
                raise
        else:
            stack = wait_for_stack(cfn, args.stack_name)
    else:
        print(f"Creating stack '{args.stack_name}' in {region}...")
        cfn.create_stack(
            StackName=args.stack_name,
            TemplateBody=template_body,
            Parameters=params,
            Capabilities=["CAPABILITY_NAMED_IAM"],
        )
        stack = wait_for_stack(cfn, args.stack_name)

    outputs = get_outputs(stack)
    if not outputs.get("AccessKeyId") or not outputs.get("SecretAccessKey"):
        print("ERROR: Stack outputs missing AccessKeyId or SecretAccessKey.", file=sys.stderr)
        print("  If the stack was previously created, the access key may be an older one.", file=sys.stderr)
        print("  Delete the stack and re-run, or create the key manually.", file=sys.stderr)
        sys.exit(1)

    # Build the s3.config file
    s3_config = {
        "service": "s3",
        "s3": {
            "accessKey": outputs["AccessKeyId"],
            "secretKey": outputs["SecretAccessKey"],
            "bucket": outputs.get("BucketName", args.bucket),
            "region": outputs.get("Region", region),
            "s3BasePath": outputs.get("S3BasePath", args.base_path),
        },
    }

    os.makedirs(CONFIGS_DIR, exist_ok=True)
    output_path = os.path.join(CONFIGS_DIR, f"{args.bucket}.json")
    with open(output_path, "w") as f:
        json.dump(s3_config, f, indent=2)
        f.write("\n")

    print(f"\nInfrastructure ready:")
    print(f"  Bucket:     {s3_config['s3']['bucket']}")
    print(f"  Region:     {s3_config['s3']['region']}")
    print(f"  IAM User:   {outputs.get('UserName', 'auto-generated')}")
    print(f"  Base Path:  {s3_config['s3']['s3BasePath']}")
    print(f"  Config:     {output_path}")
    print()
    print(f"Next steps:")
    print(f"  # Inject into a pull-secret (local):")
    print(f"  python3 pull-secret-mod.py local -f {output_path}")
    print(f"  # Or inject directly into a live cluster:")
    print(f"  python3 pull-secret-mod.py cluster -f {output_path}")
    print()
    print(f"WARNING: {output_path} contains AWS credentials. Do not commit it.")


if __name__ == "__main__":
    main()
