# Security Model

## Overview

art-tools are internal CLI tools that interact with Red Hat build and release infrastructure. They handle embargoed security content and authenticate to multiple internal and external systems.

## Authentication Methods

### Kerberos (GSSAPI/SPNEGO)

Used for: Brew/Koji, Errata Tool, distgit (rhpkg).

Requires a valid Kerberos ticket obtained via `kinit`. The `requests-gssapi` or `requests-kerberos` libraries handle SPNEGO negotiation transparently. CI containers install `krb5-devel` as a build dependency.

### GitHub App Tokens

Managed in `artcommon/artcommonlib/github_auth.py`.

- App credentials via environment variables: `GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY` (or `GITHUB_APP_PRIVATE_KEY_PATH`), and optionally `GITHUB_APP_INSTALLATION_ID`.
- Per-org installation overrides: `GITHUB_APP_INSTALLATION_ID_OPENSHIFT_ENG`, `GITHUB_APP_INSTALLATION_ID_OPENSHIFT_PRIV`, `GITHUB_APP_INSTALLATION_ID_OPENSHIFT`, `GITHUB_APP_INSTALLATION_ID_OPENSHIFT_BOT`.
- Tokens are short-lived (1 hour), cached per org for 50 minutes (`_GIT_TOKEN_TTL`).
- `get_github_client_for_org(org)` is the primary API -- auto-discovers the installation for the org, caches clients, and falls back to `GITHUB_TOKEN` PAT if App credentials are not set.
- Git CLI auth uses a `GIT_ASKPASS` helper script that injects the token as a password with `x-access-token` as the username.
- `get_github_git_pat_env()` bypasses App auth entirely for operations (like pushing to protected branches on openshift-priv) where the PAT has permissions the App lacks.

### Other Credentials

| System | Environment Variable(s) | Used By |
|--------|------------------------|---------|
| Slack | `SLACK_BOT_TOKEN` | `pyartcd/pyartcd/runtime.py` |
| Jira | `JIRA_TOKEN` | `pyartcd/pyartcd/runtime.py` |
| Jenkins | `JENKINS_SERVICE_ACCOUNT`, `JENKINS_SERVICE_ACCOUNT_TOKEN`, `JENKINS_URL` | `pyartcd/pyartcd/jenkins.py` |
| Quay.io | `QCI_USER`, `QCI_PASSWORD` | `pyartcd/pyartcd/oc.py` |
| OpenShift | `KUBECONFIG` | `pyartcd/pyartcd/oc.py`, various pipelines |
| Konflux | `KONFLUX_SA_KUBECONFIG` | `pyartcd/pyartcd/pipelines/okd.py`, `update_golang.py` |
| Signing | `KMS_CRED_FILE`, `KMS_KEY_ID`, `REKOR_URL` | `pyartcd/pyartcd/pipelines/sign_rhcos_containers.py` |
| Cloudflare S3 | `CLOUDFLARE_ENDPOINT` | `pyartcd/pyartcd/s3.py` |
| GitHub PAT | `GITHUB_TOKEN` | Fallback for GitHub App auth |

## Credential Management

- Credentials are never stored in code or in ocp-build-data.
- All credentials are passed via environment variables or Kerberos tickets.
- In CI/pipeline contexts, credentials are injected by the job runner (Jenkins, Tekton).
- GitHub App private keys are stored securely and accessed at runtime via environment variables.

## Trust Boundaries

**Internal Red Hat systems (trusted network):**
- Brew/Koji (build system)
- Errata Tool (advisory management)
- Bugzilla (bug tracking)
- distgit (source repositories via rhpkg)
- UMB (Universal Message Bus, STOMP protocol)

**External systems (public network):**
- GitHub (source code, via App tokens or PAT)
- Quay.io (container registry)

Authentication is required for all system interactions in both zones.

## Embargo Handling

Source: `artcommon/artcommonlib/build_visibility.py`

This is the most security-critical subsystem. Embargoed builds contain security fixes that must not be disclosed before the embargo lift date.

### BuildVisibility Enum

```python
class BuildVisibility(Enum):
    PUBLIC = 0
    PRIVATE = 1
```

### Visibility Suffixes (p-flags)

The visibility suffix in a build's NVR release string encodes both the build system and embargo status:

| Build System | Public | Private/Embargoed |
|-------------|--------|-------------------|
| Brew | `p0` | `p1` |
| Konflux | `p2` | `p3` |

### Key Functions

- `is_release_embargoed(release, build_system, default=True)` -- Checks the p-flag in a release string. Returns `True` for embargoed, `False` for public. **Defaults to `True` (embargoed) when the p-flag is missing** -- this is a deliberate fail-safe.
- `isolate_pflag_in_release(release)` -- Extracts the p-flag (e.g., `p0`, `p1`, `p2`, `p3`) from an NVR release string using regex. Returns `None` if no flag is found.
- `get_build_system(visibility_suffix)` -- Maps a p-flag back to its build system (`brew` or `konflux`).
- `get_visibility_suffix(build_system, visibility)` -- Returns the appropriate p-flag for a given build system and visibility.

### Assembly Integration

- `AssemblyIssueCode.EMBARGOED_CONTENT` (value 10, defined in `artcommon/artcommonlib/assembly.py`) is raised when build sync detects embargoed builds in an assembly.
- This issue code prevents shipping embargoed content in release payloads.
- In `doozer/doozerlib/cli/release_gen_payload.py`, embargoed builds are detected and flagged during payload generation. After an embargo lifts, these can be permitted by explicitly allowing the `EMBARGOED_CONTENT` issue code.

## Input Validation

- `ocp-build-data-validator` validates all YAML configuration before tools consume it.
- JSON schemas in `ocp-build-data-validator/validator/json_schemas/` define valid configuration structure.
- Validation covers format checks, Git source verification, and release configuration correctness.
- This prevents malformed config from causing incorrect builds or advisories.

## Transient Secrets (Konflux)

Managed in `doozer/doozerlib/backend/konflux_client.py`.

- Konflux pipeline runs need git-auth credentials for cloning source repositories.
- A uniquely-named Kubernetes `Secret` is created per `KonfluxClient` instance (labeled with `art.openshift.io/git-auth`).
- The secret is reused for all PipelineRuns within that invocation, then deleted on cleanup via `cleanup_transient_git_auth_secret()`.
- `cleanup_stale_git_auth_secrets()` garbage-collects old secrets (by `max_age_hours`) left behind by crashed processes.
- In dry-run mode, secret creation and deletion are logged but not executed.
