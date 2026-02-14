# AGENTS.md — Coverage Instrumentation Knowledge Base

This file captures lessons learned, design decisions, and pitfalls
discovered while building the OCP code coverage instrumentation system.
It is intended for AI agents working on this code in the future.

## Architecture Overview

The system has two Go files injected at build time by doozer:

- **`coverage_server.go`** — injected into every `package main` directory.
  Starts an HTTP coverage server via `init()`.
- **`coverage_producer.go`** — injected only into the kubelet's
  `cmd/kubelet/` directory.  Discovers containers, collects coverage, and
  uploads to S3.

Injection is controlled by Python code in `doozer/doozerlib/util.py`
(`inject_coverage_server` and `inject_coverage_producer`), triggered from
`doozer/doozerlib/rpm_builder.py` (for RPMs) and
`doozer/doozerlib/distgit.py` / `doozer/doozerlib/backend/rebaser.py`
(for container images).

## coverage_server.go — Injection Pitfalls

### Import name collisions

The file is injected into arbitrary `package main` directories.  Many Go
projects declare `var log = ...` at the package level, which collides with
`import "log"`.  ALL imports must be aliased with a `_cov` prefix and ALL
top-level identifiers must use a `_cov` prefix.

**Real example**: `cluster-ingress-operator` declares
`var log = logf.Logger.WithName("main")` — caused a compile error before
we aliased all imports.

### Build-constrained files and mixed packages

Some directories have `.go` files with `//go:build plugins` or
`//go:build tools` that declare `package main` but coexist with library
files declaring a different package (e.g. `package plugins`).  Injecting
`coverage_server.go` (which is `package main`) into such a directory
causes a compile error.

**Real examples**:
- `prometheus/plugins/generate.go` — `//go:build plugins` + `package main`,
  alongside `minimum.go` with `package plugins`.
- `installer/data/assets_generate.go` — `//go:build tools` + `package main`,
  alongside `assets.go` with `package data`.

The `find_go_main_packages()` function in `util.py` handles this with a
**package consistency check**: a directory is only considered `package main`
if ALL of its compilable `.go` files (non-test, non-build-ignored)
consistently declare `package main`.

### Sub-module directories

Projects like `operator-framework-olm` use `replace` directives in `go.mod`
to reference sub-modules in `staging/` directories.  Each sub-module has its
own `go.mod`.  Injecting into these causes `go mod vendor` to copy
`coverage_server.go` into `vendor/`, breaking strict vendoring checks
(Cachi2).

`find_go_main_packages()` skips directories with their own `go.mod` (except
the project root).

### `go install` vs `go build`

Some projects (notably Kubernetes) use `go install` instead of `go build`.
Both the FIPS wrapper (`golang_builder_FIPS_wrapper.sh`) and the RPM wrapper
(`rpm_builder_go_wrapper.sh`) must intercept both `build` and `install`
subcommands to inject `-cover -covermode=atomic`.

## coverage_producer.go — Design Decisions

### No runtime executable name check

The producer's `init()` starts unconditionally.  Injection is controlled
entirely at build time by `inject_coverage_producer()` in `util.py`, which
only copies the file into `cmd/kubelet/` (or whatever target is configured).
This allows future reuse in other binaries without code changes.

### Node name discovery via SelfSubjectReview

The kubelet's API credentials are scoped to a specific node — it can only
list/watch pods with `fieldSelector=spec.nodeName=<name>`.  A cluster-wide
pod list fails with a 403 error.

The producer discovers the node name by calling `SelfSubjectReview` (the
Kubernetes "whoami" API), which returns the authenticated username
(`system:node:<nodename>`), then strips the prefix.  This retries
indefinitely since the API server may not be up during early boot.

### Kubeconfig discovery

The producer reads `/proc/self/cmdline` to find the `--kubeconfig` flag
value.  This works for both the real kubelet and fake-kubelet since they
share the same process.  Falls back to `/var/lib/kubelet/kubeconfig`.

### Network namespace handling

This was the most complex aspect.  Key learnings:

**`/proc/<pid>/net/tcp6` vs `/proc/net/tcp6`**:
- `/proc/<pid>/net/tcp6` shows all sockets in the **network namespace**
  the process belongs to — NOT just sockets owned by that PID.
- For non-hostNetwork containers, this is the pod's netns (correct).
- For hostNetwork containers, this is the host netns (shows ALL host
  sockets, wrong for per-container attribution).

**Solution for hostNetwork containers**: Build a set of socket inodes from
`/proc/<pid>/fd/` (each socket fd is a symlink like `socket:[12345]`),
then filter `/proc/<pid>/net/tcp6` entries by matching inode (field 9).
This gives the exact sockets owned by a specific PID.

**IPv4 vs IPv6**: Go's `net.Listen("tcp", ":port")` binds to `[::]`
(IPv6 any) on modern kernels.  The port appears in `/proc/net/tcp6` but
NOT in `/proc/net/tcp`.  Always scan BOTH files.

**Connecting to coverage servers**:
- Non-hostNetwork: connect to `<podIP>:<port>` — the host can route to
  pod CIDRs.
- hostNetwork: connect to `127.0.0.1:<port>` — the port is on the host.
- Detection: `crictl inspectp` field
  `status.linux.namespaces.options.network` is `"NODE"` for hostNetwork,
  `"POD"` for isolated.

**Empty pod IPs**: `status.network.ip` is `""` for hostNetwork pods.
If the IP is empty and we don't detect hostNetwork correctly, connecting
to `http://:53700/health` resolves to localhost on the host, hitting a
random process — producing wrong binary attributions.  The namespace
options field is the reliable indicator.

### S3 credentials via registry auth

The producer reads `/var/lib/kubelet/config.json` (the kubelet's container
registry auth config).  A fake registry entry for `cluster-code-coverage.io`
holds the S3 credentials.  The auth value is
`base64("coverage:" + base64(json_config))`.

This is a convention — the container runtime will never contact this
"registry".  The MCO distributes the global pull secret to all nodes.

### S3 signing (stdlib only)

AWS Signature V4 is implemented using only `crypto/hmac`, `crypto/sha256`,
and `net/http`.  No AWS SDK.  This is because the producer is compiled into
the kubelet and we cannot add non-vendored dependencies.

### Counter file toggling

Counter files alternate between `.1` and `.2` suffixes.  If an upload is
interrupted, the other file remains intact.  This also prevents storing a
new file every 10 seconds for clusters that may run for weeks.

## fake-kubelet — Testing

The `fake-kubelet/` directory symlinks `coverage_server.go` and
`coverage_producer.go` from the parent directory.  It compiles them into
a single static binary with `-cover -covermode=atomic`.

When run on a node with the kubelet's kubeconfig and registry auth config,
it exercises the full coverage pipeline: server startup, SelfSubjectReview,
pod watching, crictl scanning, /proc parsing, HEAD verification, coverage
collection, and S3 upload.

Build with `make build` in the `fake-kubelet/` directory.

## Common debugging

- **"0 candidate port(s)"**: The scanner was only checking `/proc/net/tcp`
  but Go binds to IPv6.  Must also check `/proc/net/tcp6`.
- **Wrong binary names in S3**: hostNetwork containers' monitors were seeing
  all host ports and connecting to the wrong processes.  Fixed by inode
  filtering and proper hostNetwork detection.
- **Empty pod IP causing misattribution**: `http://:53700` resolves to
  localhost, hitting random host processes.  Must detect hostNetwork via
  `status.linux.namespaces.options.network`, not just empty IP.
- **SelfSubjectReview "user does not start with system:node:"**: The
  kubeconfig user field name may not match the authenticated identity.
  Use the API-based SelfSubjectReview instead of parsing kubeconfig YAML.
- **"pods is forbidden: can only list/watch pods with spec.nodeName"**:
  The kubelet's credentials are node-scoped.  All pod list/watch calls
  must include `fieldSelector=spec.nodeName=<name>`.
