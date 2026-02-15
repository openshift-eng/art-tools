#!/usr/bin/env python3
"""
Inject the cluster-code-coverage.openshift.io registry auth entry into an OCP pull secret.

Two modes:

  local   - Read a pull-secret JSON from stdin, add the coverage entry, print
            the updated JSON to stdout (both single-line).

  cluster - Read the global pull secret from a live cluster (via oc), add the
            coverage entry, and apply it back.

Use -f <file> to supply S3 configuration from a JSON file instead of
interactive prompts.  The file format matches the output of s3-setup.py:

  {
    "service": "s3",
    "s3": {
      "accessKey": "...",
      "secretKey": "...",
      "bucket": "...",
      "region": "us-east-1",
      "s3BasePath": "openshift-ci/coverage/my-cluster"
    }
  }

Usage:
  python3 pull-secret-mod.py local   --s3-prefix openshift-ci/coverage/my-cluster
  python3 pull-secret-mod.py local   --s3-prefix openshift-ci/coverage/my-cluster -f configs/bucket.json
  python3 pull-secret-mod.py cluster --s3-prefix openshift-ci/coverage/my-cluster
  python3 pull-secret-mod.py cluster --s3-prefix openshift-ci/coverage/my-cluster -f configs/bucket.json
"""

import base64
import json
import subprocess
import sys

COVERAGE_SERVER = "cluster-code-coverage.openshift.io"
COVERAGE_USERNAME = "coverage"  # arbitrary; only the password matters


def load_s3_config_from_file(path: str) -> dict:
    """Load and validate S3 config from a JSON file."""
    with open(path) as f:
        cfg = json.load(f)

    if cfg.get("service") != "s3":
        print(f"ERROR: Unsupported service {cfg.get('service')!r} in {path} (expected \"s3\")", file=sys.stderr)
        sys.exit(1)

    s3 = cfg.get("s3", {})
    required = ["accessKey", "secretKey", "bucket", "s3BasePath"]
    missing = [k for k in required if not s3.get(k)]
    if missing:
        print(f"ERROR: Missing required S3 fields in {path}: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    if "region" not in s3 or not s3["region"]:
        s3["region"] = "us-east-1"

    print(f"  Loaded S3 config from {path}")
    print(f"    bucket={s3['bucket']}  region={s3['region']}  basePath={s3['s3BasePath']}")
    return cfg


def prompt_s3_config() -> dict:
    """Interactively prompt for S3 coverage configuration."""
    print("\n--- S3 Coverage Configuration ---")
    access_key = input("  AWS Access Key ID: ").strip()
    secret_key = input("  AWS Secret Access Key: ").strip()
    bucket = input("  S3 Bucket: ").strip()
    region = input("  AWS Region [us-east-1]: ").strip() or "us-east-1"
    base_path = input("  S3 base path (e.g. openshift-ci/coverage/my-cluster): ").strip()

    if not all([access_key, secret_key, bucket, base_path]):
        print("ERROR: All fields except region are required.", file=sys.stderr)
        sys.exit(1)

    return {
        "service": "s3",
        "s3": {
            "accessKey": access_key,
            "secretKey": secret_key,
            "bucket": bucket,
            "region": region,
            "s3BasePath": base_path,
        },
    }


def get_s3_config(config_file: str | None, s3_prefix: str) -> dict:
    """Return S3 config from file or interactive prompts, applying the
    required --s3-prefix override.
    """
    if config_file:
        cfg = load_s3_config_from_file(config_file)
    else:
        cfg = prompt_s3_config()
    cfg["s3"]["s3BasePath"] = s3_prefix
    print(f"  S3 prefix: {s3_prefix}")
    return cfg


def build_auth_entry(s3_config: dict) -> str:
    """Build the base64-encoded auth value for the registry entry.

    Format: base64("coverage:" + base64(json_config))
    The inner base64 is the "password" in the Docker auth convention.
    """
    config_json = json.dumps(s3_config, separators=(",", ":"))
    password_b64 = base64.b64encode(config_json.encode()).decode()
    user_pass = f"{COVERAGE_USERNAME}:{password_b64}"
    return base64.b64encode(user_pass.encode()).decode()


def inject_entry(pull_secret: dict, s3_config: dict) -> dict:
    """Add or replace the coverage entry in a pull-secret dict."""
    if "auths" not in pull_secret:
        pull_secret["auths"] = {}

    if COVERAGE_SERVER in pull_secret["auths"]:
        print(f"  NOTE: Replacing existing entry for {COVERAGE_SERVER}")

    pull_secret["auths"][COVERAGE_SERVER] = {
        "auth": build_auth_entry(s3_config),
    }
    return pull_secret


def mode_local(config_file: str | None, s3_prefix: str):
    """Local mode: read pull-secret from stdin, write updated to stdout."""
    print("Paste the pull-secret JSON (single line), then press Enter:")
    raw = input().strip()
    if not raw:
        print("ERROR: Empty input.", file=sys.stderr)
        sys.exit(1)

    try:
        pull_secret = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    s3_config = get_s3_config(config_file, s3_prefix)
    updated = inject_entry(pull_secret, s3_config)

    print("\n--- Updated pull-secret (single line) ---")
    print(json.dumps(updated, separators=(",", ":")))


def run_oc(*args) -> str:
    """Run an oc command and return stripped stdout."""
    result = subprocess.run(
        ["oc"] + list(args),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"ERROR: oc {' '.join(args)} failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def mode_cluster(config_file: str | None, s3_prefix: str):
    """Cluster mode: read/modify/apply the global pull secret via oc."""
    # Determine and confirm the target cluster
    server = run_oc("whoami", "--show-server")
    print(f"\nTarget cluster: {server}")
    confirm = input("Proceed? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        sys.exit(0)

    # Fetch the current global pull secret
    print("\nFetching global pull secret...")
    secret_json = run_oc(
        "get", "secret", "pull-secret",
        "-n", "openshift-config",
        "-o", "jsonpath={.data.\\.dockerconfigjson}",
    )
    if not secret_json:
        print("ERROR: Could not read .dockerconfigjson from pull-secret.", file=sys.stderr)
        sys.exit(1)

    try:
        pull_secret = json.loads(base64.b64decode(secret_json))
    except (json.JSONDecodeError, Exception) as e:
        print(f"ERROR: Failed to decode pull secret: {e}", file=sys.stderr)
        sys.exit(1)

    current_count = len(pull_secret.get("auths", {}))
    print(f"  Current pull secret has {current_count} registry entries.")

    s3_config = get_s3_config(config_file, s3_prefix)
    updated = inject_entry(pull_secret, s3_config)

    # Encode the updated secret back to base64
    updated_json = json.dumps(updated, separators=(",", ":"))

    # Apply via oc set data
    print("\nApplying updated pull secret to the cluster...")
    result = subprocess.run(
        [
            "oc", "set", "data", "secret/pull-secret",
            "-n", "openshift-config",
            f"--from-literal=.dockerconfigjson={updated_json}",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"ERROR: oc set data failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)

    print("Pull secret updated successfully.")
    print(f"  {COVERAGE_SERVER} entry is now present in the global pull secret.")
    print("  NOTE: Nodes will pick up the change automatically (MCO rollout).")


def _usage():
    print(f"Usage: {sys.argv[0]} <local|cluster> --s3-prefix PREFIX [-f <s3-config-file>]", file=sys.stderr)
    print(f"  Example: {sys.argv[0]} cluster --s3-prefix openshift-ci/coverage/my-cluster -f configs/bucket.json", file=sys.stderr)


def main():
    args = sys.argv[1:]
    if not args or args[0] not in ("local", "cluster"):
        _usage()
        sys.exit(1)

    mode = args[0]
    config_file = None
    s3_prefix = None

    i = 1
    while i < len(args):
        if args[i] == "-f" and i + 1 < len(args):
            config_file = args[i + 1]
            i += 2
        elif args[i] == "--s3-prefix" and i + 1 < len(args):
            s3_prefix = args[i + 1]
            i += 2
        elif args[i].startswith("--s3-prefix="):
            s3_prefix = args[i].split("=", 1)[1]
            i += 1
        else:
            print(f"Unknown argument: {args[i]}", file=sys.stderr)
            _usage()
            sys.exit(1)

    if not s3_prefix:
        print("ERROR: --s3-prefix is required.", file=sys.stderr)
        _usage()
        sys.exit(1)

    if mode == "local":
        mode_local(config_file, s3_prefix)
    else:
        mode_cluster(config_file, s3_prefix)


if __name__ == "__main__":
    main()
