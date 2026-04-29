# Architecture Overview

art-tools is a Python 3.11 monorepo of CLI tools for managing OpenShift Container Platform (OCP) releases. It automates building container images and RPMs, managing errata advisories, and orchestrating multi-step release pipelines.

## System Context

| External System   | Direction | Interface          | Key Files                                                         |
|-------------------|-----------|--------------------|-------------------------------------------------------------------|
| ocp-build-data    | Inbound   | Git clone          | `artcommon/artcommonlib/gitdata.py`                               |
| Brew/Koji         | In/Out    | Koji API           | `doozer/doozerlib/brew.py`, `elliott/elliottlib/brew.py`          |
| Errata Tool       | In/Out    | REST API           | `elliott/elliottlib/errata.py`, `elliott/elliottlib/errata_async.py` |
| Bugzilla          | In/Out    | REST API           | `elliott/elliottlib/bzutil.py`                                    |
| Jira              | In/Out    | REST API           | `pyartcd/pyartcd/jira_client.py`                                  |
| Konflux           | In/Out    | Kubernetes API     | `doozer/doozerlib/backend/konflux_client.py`, `artcommon/artcommonlib/konflux/` |
| Jenkins           | Trigger   | REST API           | `pyartcd/pyartcd/jenkins.py`                                      |
| Slack             | Out       | Bot API            | `pyartcd/pyartcd/slack.py`                                        |
| UMB               | In/Out    | STOMP              | `pyartcd/pyartcd/umb_client.py`                                   |
| GitHub            | In/Out    | REST + Git         | `artcommon/artcommonlib/github_auth.py`                           |
| Quay.io           | Out       | Registry API       | `pyartcd/pyartcd/pipelines/promote.py`                            |
| BigQuery          | Out       | Client API         | `artcommon/artcommonlib/bigquery.py`                               |
| Redis             | In/Out    | Client API         | `artcommon/artcommonlib/redis.py`                                  |

## Package Layering

```
                     +----------+
                     | pyartcd  |  (orchestrates pipelines, may import doozerlib + elliottlib)
                     +----+-----+
                          |
              +-----------+-----------+
              |                       |
         +----v----+           +------v---+
         | doozer  |           | elliott  |   (peer CLI tools, MUST NOT import each other)
         +----+----+           +-----+----+
              |                      |
              +----------+-----------+
                         |
                   +-----v------+
                   | artcommon  |   (shared library)
                   +------------+

  +----------------------------+
  | ocp-build-data-validator   |   (standalone, no cross-imports)
  +----------------------------+
```

## Dependency Rules

1. **doozerlib and elliottlib are peers.** The intended design is no cross-imports, but in practice some CLI modules import from each other for Konflux, shipment, and plashet functionality. These cross-imports exist in ~12 files and represent tech debt.
2. **Both doozerlib and elliottlib import shared code from artcommonlib.**
3. **pyartcd MAY import from doozerlib, elliottlib, and artcommonlib.** It is the orchestration layer.
4. **ocp-build-data-validator is standalone.** It does not import from any other package in this repo.
5. **artcommonlib is the base layer.** It has a small number of imports from doozerlib (2 files) which are tech debt to be resolved.

## Components

| Component    | Entry Point                                   | Critical Code                                          | Purpose                                     | Details                          |
|-------------|-----------------------------------------------|-------------------------------------------------------|---------------------------------------------|----------------------------------|
| doozer      | `doozer/doozerlib/cli/__main__.py`            | `runtime.py`, `distgit.py`, `image.py`, `backend/`   | Build/rebase images and RPMs via Brew/Konflux | [doozer.md](./agentic/design-docs/components/doozer.md) |
| elliott     | `elliott/elliottlib/cli/__main__.py`          | `runtime.py`, `errata.py`, `bzutil.py`, `brew.py`    | Manage errata advisories and bugs            | [elliott.md](./agentic/design-docs/components/elliott.md) |
| pyartcd     | `pyartcd/pyartcd/__main__.py`                 | `pipelines/ocp.py`, `pipelines/promote.py`            | Orchestrate release pipelines                | [pyartcd.md](./agentic/design-docs/components/pyartcd.md) |
| artcommon   | (library, no CLI)                             | `assembly.py`, `model.py`, `runtime.py`, `gitdata.py` | Shared utilities, config, abstractions       | [artcommon.md](./agentic/design-docs/components/artcommon.md) |
| validator   | `ocp-build-data-validator/validator/__main__.py` | `schema/`, `releases.py`                             | Validate ocp-build-data YAML schemas         | `ocp-build-data-validator/`     |

## Data Flow

```
ocp-build-data (YAML configs: group.yml, images/*.yml, rpms/*.yml, releases.yml)
       |
       v
Runtime.initialize(group="openshift-4.17", assembly="stream")
       |
       +-- Clones ocp-build-data repo
       +-- Loads group.yml configuration
       +-- Resolves assembly definition from releases.yml
       |
       v
Metadata objects (ImageMetadata, RPMMetadata)
       |
       +-- Each metadata object wraps one component's YAML config
       +-- Source resolver locates upstream source repos
       |
       v
Build commands (images:build, images:rebase, rpms:build)
       |
       +-- Doozer: distgit operations, Dockerfile generation, build submission
       +-- Backend: Brew/OSBS or Konflux pipeline runs
       |
       v
Build results (Brew NVRs, Konflux build records)
       |
       v
Advisory/Payload (Elliott attaches builds to advisories, pyartcd promotes to payload)
```

## Runtime Initialization Flow

When `doozer --group openshift-4.17 images:build` is executed:

1. **CLI parsing**: Click parses `--group openshift-4.17` and other global options. The `Runtime` object is instantiated with these parameters.

2. **`Runtime.initialize()` called**: This is the core setup method in `doozer/doozerlib/runtime.py` (inherits from `GroupRuntime` in `artcommon/artcommonlib/runtime.py`).

3. **Working directory setup**: Creates or uses `--working-dir` for temporary files, logs, and cloned repos.

4. **ocp-build-data clone**: Clones the ocp-build-data Git repository (default branch derived from group name, e.g. `openshift-4.17`). The data path can be overridden with `--data-path`.

5. **Group config loading**: Reads `group.yml` from cloned ocp-build-data. This defines default settings: Brew tags, repos, arches, freeze state, etc. Loaded as a `Model` object.

6. **Assembly resolution**: If `--assembly` is provided (defaults to `stream`), loads the assembly definition from `releases.yml`. The assembly type (STREAM, STANDARD, CANDIDATE, CUSTOM, PREVIEW) determines constraint enforcement. `assembly_basis_event()` determines the Brew event that pins build state.

7. **Metadata creation**: For each YAML file in `images/` and `rpms/` directories of ocp-build-data (filtered by `--images`/`--rpms` CLI options), creates `ImageMetadata` or `RPMMetadata` objects. Each wraps the component's config, provides distgit operations, and tracks build state.

8. **Brew session**: A shared Koji client session is established (`Runtime.shared_koji_client_session()`), optionally constrained to a point-in-time via `brew_event`.

9. **Command dispatch**: The specific subcommand (e.g. `images:build`) receives the fully initialized Runtime and operates on the loaded metadata objects.

## Critical Code Locations

| Function / Concern                  | File                                                  | Why Critical                                                    |
|-------------------------------------|-------------------------------------------------------|-----------------------------------------------------------------|
| Doozer Runtime initialization       | `doozer/doozerlib/runtime.py`                         | Bootstraps all doozer operations; clones data, loads metadata   |
| Elliott Runtime initialization      | `elliott/elliottlib/runtime.py`                       | Bootstraps all elliott operations                               |
| GroupRuntime base class             | `artcommon/artcommonlib/runtime.py`                   | Shared initialization logic (logging, Konflux DB)               |
| Assembly type resolution            | `artcommon/artcommonlib/assembly.py`                  | Determines constraint enforcement for releases                  |
| Model / Missing config traversal    | `artcommon/artcommonlib/model.py`                     | Every YAML config access goes through Model                     |
| Distgit management                  | `doozer/doozerlib/distgit.py`                         | 3000+ lines; core of image/RPM source management                |
| Image build logic                   | `doozer/doozerlib/image.py`                           | ImageMetadata class; build, rebase, config resolution           |
| Konflux build client                | `doozer/doozerlib/backend/konflux_client.py`          | Interface to new Konflux build system                           |
| Errata Tool API                     | `elliott/elliottlib/errata.py`                        | Creates/modifies advisories; core elliott functionality          |
| Errata async operations             | `elliott/elliottlib/errata_async.py`                  | Async advisory operations for performance                       |
| Bug tracking integration            | `elliott/elliottlib/bzutil.py`                        | Bugzilla and Jira bug queries and attachment                    |
| OCP build pipeline                  | `pyartcd/pyartcd/pipelines/ocp.py`                    | Main build pipeline; orchestrates doozer + elliott              |
| OCP4 Konflux pipeline               | `pyartcd/pyartcd/pipelines/ocp4_konflux.py`           | Konflux variant of main build pipeline                          |
| Prepare release pipeline            | `pyartcd/pyartcd/pipelines/prepare_release_konflux.py`| Sets up advisory, bugs, and payload for a release               |
| Promote pipeline                    | `pyartcd/pyartcd/pipelines/promote.py`                | Promotes builds to release payload and mirrors                  |
| Plashet repo composition            | `doozer/doozerlib/plashet.py`                         | Composes RPM repositories from Brew tags                        |
| Source resolver                     | `doozer/doozerlib/source_resolver.py`                 | Locates and clones upstream source repos for builds             |
| Git data loading                    | `artcommon/artcommonlib/gitdata.py`                   | Loads YAML metadata files from ocp-build-data                   |
| Exec tools                          | `artcommon/artcommonlib/exectools.py`                 | Subprocess execution utilities used everywhere                  |
| Konflux DB integration              | `artcommon/artcommonlib/konflux/konflux_db.py`        | Build record storage for Konflux builds                         |

## Related Documentation

- [AGENTS.md](./AGENTS.md) -- Navigation entry point for AI agents
- [CLAUDE.md](./CLAUDE.md) -- Development setup, commands, and working conventions
- [agentic/design-docs/](./agentic/design-docs/) -- Per-component design documentation
- [agentic/domain/](./agentic/domain/) -- Domain glossary and concepts
- [agentic/decisions/](./agentic/decisions/) -- Architecture decision records
