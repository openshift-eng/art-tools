# OCP Code Coverage Tools

This directory contains the Go source files, Python utilities, and a test
harness for instrumenting OpenShift binaries with runtime code coverage
collection and uploading the results to S3.

## Files

| File / Directory | Description |
|------------------|-------------|
| `coverage_server.go` | Injected into every Go `package main` directory at build time. Starts an HTTP server that exposes coverage data. |
| `coverage_producer.go` | Injected only into the kubelet. Discovers coverage-instrumented containers on the node and uploads their data to S3. |
| `pull-secret-mod.py` | Injects the `cluster-code-coverage.openshift.io` auth entry into an OCP pull secret (local or live cluster). |
| `s3-setup.py` | Provisions S3 infrastructure via CloudFormation and writes a config file. |
| `coverage-s3.cfn.yaml` | CloudFormation template used by `s3-setup.py`. |
| `fake-kubelet/` | Test binary that exercises both `coverage_server.go` and `coverage_producer.go` on a live node. |
| `configs/` | gitignored directory where `s3-setup.py` writes credential files. |

## Quick Start

### 1. Provision S3 infrastructure

```bash
pip install boto3  # if not already installed

python3 s3-setup.py --bucket my-coverage-bucket --region us-east-1 --profile my-aws-profile
```

This creates an S3 bucket (idempotent), an IAM user with upload permissions,
and an access key.  The config file is written to
`configs/<bucket>.json`.

Options:

```
--bucket NAME        S3 bucket name (required)
--region REGION      AWS region (required)
--stack-name NAME    CloudFormation stack name (default: coverage-s3-infra)
--base-path PATH     S3 base path prefix (default: openshift-ci/coverage)
--profile PROFILE    AWS CLI profile name
```

### 2. Inject credentials into a cluster's pull secret

```bash
# Apply directly to a live cluster (requires oc login)
python3 pull-secret-mod.py cluster --s3-prefix openshift-ci/coverage/my-cluster -f configs/my-coverage-bucket.json

# Or modify a pull-secret JSON locally
echo '<pull-secret-json>' | python3 pull-secret-mod.py local --s3-prefix openshift-ci/coverage/my-cluster -f configs/my-coverage-bucket.json
```

`--s3-prefix` is required and sets the S3 base path for this cluster's
coverage data.  Without `-f`, the tool prompts interactively for S3
configuration.

### 3. Build with coverage enabled

Set `build_profiles.enable_go_cover: true` in the ocp-build-data group
config.  Doozer will then:

- Inject `coverage_server.go` into every `package main` directory (images
  and RPMs).
- Inject `coverage_producer.go` into `cmd/kubelet/` in the openshift RPM.
- Add `-cover -covermode=atomic` flags to `go build` and `go install`
  invocations via the Go compliance shim.

### 4. Testing with fake-kubelet

The `fake-kubelet/` directory contains a minimal binary that compiles both
`coverage_server.go` and `coverage_producer.go` into a single executable.
It can be copied to any OCP node to test coverage collection without
rebuilding the real kubelet.

```bash
# Build
cd fake-kubelet
make build          # produces ./fake-kubelet (statically linked, ~51MB)

# Copy to a node
scp fake-kubelet core@<node>:/tmp/

# SSH to the node and run
sudo /tmp/fake-kubelet                                        # uses default kubeconfig
sudo /tmp/fake-kubelet --kubeconfig=/var/lib/kubelet/kubeconfig  # explicit
```

Prerequisites on the node:
- A registry auth config containing the `cluster-code-coverage.openshift.io`
  entry must exist at one of the searched paths (see "Registry auth search
  paths" below).
- The kubelet kubeconfig must be readable (default:
  `/var/lib/kubelet/kubeconfig`).  If it doesn't exist yet (bootstrap),
  the producer polls for up to 5 minutes before falling back to
  bootstrap mode.
- `crictl` must be on `$PATH` (standard on RHCOS).

What fake-kubelet does:
1. Starts a coverage HTTP server (port 53700+).
2. Reads S3 credentials from the registry auth config.
3. Starts the host-network scanner immediately (collects coverage while
   waiting for the kubeconfig).
4. Polls for the kubeconfig (1/s, up to 5 minutes).
5. If found: uses SelfSubjectReview to discover the node name, watches
   pods, and scans containers (full mode).
6. If not found: continues in bootstrap mode (host-only scanning).
7. Blocks until SIGINT/SIGTERM.

## Coverage Server (`coverage_server.go`)

Starts automatically via `init()` in any instrumented binary.  Tries ports
53700--53749 (or from `COVERAGE_PORT` env var, up to 50 consecutive ports).

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/coverage` | Returns JSON with base64-encoded coverage data |
| `GET` | `/coverage?nometa=1` | Same but skips metadata (faster for repeated polls) |
| `GET` | `/health` | Returns 200 with "coverage server healthy" |
| `HEAD` | any | Returns identity headers only |

**Identity headers** (on every response):

| Header | Env Var | Always present |
|--------|---------|----------------|
| `X-Art-Coverage-Server` | — | Yes (always `1`) |
| `X-Art-Coverage-Pid` | — | Yes |
| `X-Art-Coverage-Binary` | — | Yes |
| `X-Art-Coverage-Source-Commit` | `SOURCE_GIT_COMMIT` | If set |
| `X-Art-Coverage-Source-Url` | `SOURCE_GIT_URL` | If set |
| `X-Art-Coverage-Software-Group` | `SOFTWARE_GROUP` or `__doozer_group` | If set |
| `X-Art-Coverage-Software-Key` | `SOFTWARE_KEY` or `__doozer_key` | If set |

The producer records ALL `X-Art-Coverage-*` headers (except Server, Pid,
Binary) into `info.json`, converting header names to `lowercase_underscore`
format.  New headers added in the future are automatically captured.

All imports use a `_cov` prefix and all identifiers use a `_cov` prefix to
avoid name collisions with the host package (e.g. many Go projects declare
`var log = ...` at the package level).

**Metadata hash caching:** The server caches the coverage metadata hash
from the first full request.  Subsequent `?nometa=1` requests use the
cached hash to produce correct counter filenames.  The producer also
caches the hash and substitutes "unknown" if encountered.

## Coverage Producer (`coverage_producer.go`)

Starts automatically via `init()`.  Intended to run inside the kubelet
(injected at build time by doozer), but works in any binary that has access
to the kubelet's kubeconfig and registry auth config.

### Registry auth search paths

The producer tries the following paths in order to find the S3 credentials.
The first file that exists, is readable, and contains the
`cluster-code-coverage.openshift.io` entry wins:

1. `/var/lib/kubelet/config.json` (normal node)
2. `/root/.docker/config.json` (bootstrap node)
3. `/run/containers/0/auth.json` (bootstrap node)
4. `/root/.config/containers/auth.json` (bootstrap node)

### Startup sequence

1. Reads S3 credentials from the registry auth config (tries multiple paths).
2. Starts the host-network scanner immediately.
3. Polls for the kubeconfig file (1/s, up to 5 minutes).
   - **Full mode** (kubeconfig found): creates a Kubernetes client,
     discovers the node name via SelfSubjectReview, starts a pod watcher,
     and enters the container scan loop.
   - **Bootstrap mode** (kubeconfig not found): continues with host-network
     scanning only.

### Container discovery

Uses `crictl ps`, `crictl inspect`, and `crictl inspectp` to enumerate
containers on the node.  For each container it extracts:
- Container name, ID, sandbox ID
- PID and image ref from `crictl inspect`
- Pod name, namespace, pod IP, and network mode from `crictl inspectp`
- `COVERAGE_PORT` override from `/proc/<pid>/environ`

### Network namespace handling

The producer must connect to each container's coverage server.  The approach
differs based on the pod's network mode
(`status.linux.namespaces.options.network` from `crictl inspectp`):

**Non-hostNetwork pods** (`"POD"`):
- The container has its own network namespace with a pod IP.
- `/proc/<pid>/net/tcp6` only shows sockets in that namespace.
- The producer connects to `<podIP>:<port>` from the host.

**hostNetwork pods** (`"NODE"`):
- The container shares the host's network namespace.
- `/proc/<pid>/net/tcp6` shows ALL host sockets, not just this process's.
- To find only this container's ports, the producer reads `/proc/<pid>/fd/`
  to build a set of socket inodes owned by the PID, then filters the
  tcp/tcp6 table entries by matching inode (field 9).
- The producer connects to `127.0.0.1:<port>`.

**Host-network scanner** (`_prodScanHostLoop`):
- Separately scans `/proc/net/tcp` and `/proc/net/tcp6` for all listening
  ports in the coverage range (no inode filter).
- Uses `X-Art-Coverage-Binary` from HEAD responses to identify each server.
- Uploads to `{basePath}/{hostname}/_host_/{discovery-time}/{binary}/`.
- Coverage data from hostNetwork containers may appear both under the
  container's path and under `_host_/` — this duplication is intentional.

### Adaptive polling

For each confirmed coverage server, the collection goroutine uses an
adaptive schedule:
- First 10 seconds: poll every 1 second (captures short-lived containers).
- Next 5 minutes: poll every 5 seconds.
- After that: poll every 10 seconds (steady state for long-lived processes).

The first request fetches full coverage data (metadata + counters).
Subsequent requests use `?nometa=1` to skip metadata since it doesn't change.

### S3 upload

Uses pure Go stdlib for AWS Signature V4 signing (`crypto/hmac`,
`crypto/sha256`, `net/http`).  No AWS SDK dependency.

**Path layout:**

```
{basePath}/{hostname}/{namespace}/{pod}/{container}/{discovery-time}/{imageRef}/{binary}/
  info.json                     # written once (server identity metadata)
  covmeta.<hash>                # written once
  covcounters.<hash>.<pid>.1    # toggled
  covcounters.<hash>.<pid>.2    # toggled

{basePath}/{hostname}/_host_/{discovery-time}/{binary}/
  info.json
  covmeta.<hash>
  covcounters.<hash>.<pid>.1
  covcounters.<hash>.<pid>.2
```

`info.json` captures all `X-Art-Coverage-*` headers from the server:

```json
{
  "binary": "kube-apiserver",
  "host": "10.128.2.8",
  "port": 53700,
  "pid": 2827,
  "source_commit": "abc123",
  "source_url": "https://github.com/openshift/kubernetes",
  "software_group": "openshift-4.21",
  "software_key": "ose-kube-apiserver"
}
```

Counter files alternate between `.1` and `.2` suffixes to avoid corruption
from interrupted uploads and to prevent unbounded file accumulation.

### Requirements

- A registry auth config with the `cluster-code-coverage.openshift.io`
  entry at one of the searched paths.
- Kubelet kubeconfig (default `/var/lib/kubelet/kubeconfig`) for full mode.
  Not required for bootstrap mode.
- `crictl` on `$PATH` (for container discovery in full mode).
- Host PID namespace access (standard for OCP kubelets; will not work if the
  kubelet is containerized without `hostPID: true`, e.g. some MicroShift
  configurations).

## Security Notes

- `configs/*.json` files contain AWS credentials and are gitignored.  Do not
  commit them.
- The `cluster-code-coverage.openshift.io` registry auth entry is a
  convention — it is not a real container registry.  The kubelet's container
  runtime will never contact it.
- The IAM user created by `s3-setup.py` has only `PutObject`, `GetObject`,
  and `ListBucket` permissions on the coverage bucket.
- The S3 bucket has a 90-day lifecycle expiration rule.
