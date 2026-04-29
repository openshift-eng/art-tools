# art-tools - Agent Navigation

CLI tools for managing OpenShift Container Platform (OCP) releases: builds, advisories, and pipelines.

## What This Repository Does

A Python 3.11 monorepo of five packages that automate OCP release engineering -- building images/RPMs, managing errata advisories, and orchestrating release pipelines.

## Quick Navigation by Intent

| I want to...                     | Go to                                              |
|----------------------------------|----------------------------------------------------|
| Understand the system            | [ARCHITECTURE.md](./ARCHITECTURE.md)               |
| Build/rebase images or RPMs      | [agentic/design-docs/components/doozer.md](./agentic/design-docs/components/doozer.md) |
| Manage advisories/errata         | [agentic/design-docs/components/elliott.md](./agentic/design-docs/components/elliott.md) |
| Understand a pipeline            | [agentic/design-docs/components/pyartcd.md](./agentic/design-docs/components/pyartcd.md) |
| Understand shared utilities      | [agentic/design-docs/components/artcommon.md](./agentic/design-docs/components/artcommon.md) |
| Understand a domain concept      | [agentic/domain/glossary.md](./agentic/domain/glossary.md) |
| Set up dev environment           | [CLAUDE.md](./CLAUDE.md)                           |
| Implement a feature              | Create exec-plan in `agentic/exec-plans/active/` first, read TESTING.md |
| Fix a bug                        | [ARCHITECTURE.md#components](./ARCHITECTURE.md#components), [agentic/DEVELOPMENT.md](./agentic/DEVELOPMENT.md) |
| Find a past decision             | [agentic/decisions/index.md](./agentic/decisions/index.md) |

## Repository Structure

```
art-tools/
  artcommon/artcommonlib/     # Shared library (assembly, model, exectools, konflux)
  doozer/doozerlib/           # Build management CLI (images, RPMs, distgit)
  elliott/elliottlib/         # Advisory/errata management CLI
  pyartcd/pyartcd/            # Release pipeline orchestration
  ocp-build-data-validator/   # Schema validator for ocp-build-data
  agentic/                    # Agent documentation and exec-plans
  hack/                       # Developer utility scripts
  Makefile                    # Build/test/lint targets
  pyproject.toml              # Root project configuration
```

## Component Boundaries

```
pyartcd (orchestrates)
  |          |
  v          v
doozer    elliott
  |          |
  v          v
artcommon (shared)

External: ocp-build-data, Brew/Koji, Errata Tool, Konflux, Jira, Slack
```

## Core Concepts

| Concept       | Definition                                                       | Docs                              |
|---------------|------------------------------------------------------------------|-----------------------------------|
| Group         | An OCP version target (e.g. `openshift-4.17`)                   | [ARCHITECTURE.md](./ARCHITECTURE.md#runtime-initialization-flow) |
| Assembly      | A named release or checkpoint (STREAM, STANDARD, CANDIDATE, CUSTOM, PREVIEW) | `artcommonlib/assembly.py` |
| Runtime       | Central orchestration object initialized per group               | `doozerlib/runtime.py`, `elliottlib/runtime.py` |
| Metadata      | ImageMetadata or RPMMetadata wrapping a build component          | `doozerlib/metadata.py`           |
| Model/Missing | Safe YAML config traversal; Missing singleton prevents KeyError  | `artcommonlib/model.py`           |
| Distgit       | Internal source repositories managed by doozer                   | `doozerlib/distgit.py`            |
| Plashet       | RPM repo composition from Brew tags                              | `doozerlib/plashet.py`            |
| Advisory      | Errata advisory for shipping fixes                               | `elliottlib/errata.py`            |
| Konflux       | New build system replacing OSBS/Brew                             | `doozerlib/backend/`              |
| ocp-build-data| External YAML config repo defining groups, images, RPMs          | `artcommonlib/gitdata.py`         |

## Components

| Component    | Entry Point                          | Purpose                              | Docs                              |
|-------------|--------------------------------------|--------------------------------------|-----------------------------------|
| doozer      | `doozerlib/cli/__main__.py`          | Build management (images, RPMs)      | [doozer.md](./agentic/design-docs/components/doozer.md) |
| elliott     | `elliottlib/cli/__main__.py`         | Advisory and errata management       | [elliott.md](./agentic/design-docs/components/elliott.md) |
| pyartcd     | `pyartcd/__main__.py`                | Pipeline orchestration               | [pyartcd.md](./agentic/design-docs/components/pyartcd.md) |
| artcommon   | (library)                            | Shared utilities and abstractions    | [artcommon.md](./agentic/design-docs/components/artcommon.md) |
| validator   | `validator/__main__.py`              | ocp-build-data schema validation     | `ocp-build-data-validator/`       |

## Key Invariants

1. **doozerlib and elliottlib are peers.** Cross-imports exist (~12 files) but are tech debt. Both import from artcommonlib.
2. **pyartcd MAY import from doozerlib and elliottlib.** It orchestrates both.
3. **Runtime must be initialized before use** -- `Runtime.initialize()` clones ocp-build-data, loads group config, creates Metadata objects.
4. **ocp-build-data-validator is standalone** -- no imports from other packages.
5. **Never push to `origin` remote** -- always push to `dev` remote. PRs against `main`.

## Critical Code Locations

| What                        | File                                          |
|-----------------------------|-----------------------------------------------|
| Doozer Runtime              | `doozer/doozerlib/runtime.py`                 |
| Elliott Runtime             | `elliott/elliottlib/runtime.py`               |
| GroupRuntime base class     | `artcommon/artcommonlib/runtime.py`            |
| Assembly system             | `artcommon/artcommonlib/assembly.py`           |
| Model/Missing               | `artcommon/artcommonlib/model.py`              |
| Distgit management          | `doozer/doozerlib/distgit.py`                 |
| Errata API                  | `elliott/elliottlib/errata.py`                |
| Konflux build client        | `doozer/doozerlib/backend/konflux_client.py`  |

## External Dependencies

| System       | Purpose                    | Auth       |
|-------------|----------------------------|------------|
| Brew/Koji    | Build system               | Kerberos   |
| Errata Tool  | Advisory management        | Kerberos   |
| Bugzilla     | Bug tracking               | API key    |
| Jira         | Issue tracking             | API token  |
| Konflux      | New build system           | Kubernetes |
| Jenkins      | Pipeline triggers          | API token  |
| Slack        | Notifications              | Bot token  |
| GitHub       | Source repos               | OAuth      |
| Quay.io      | Container registry         | Registry   |
| BigQuery     | Analytics                  | Service account |
| Redis        | Caching/locking            | Connection |

## Build and Test

```bash
make venv             # Create venv and install all packages
make format           # Auto-format with ruff
make lint             # Lint checks
make unit             # Run all unit tests
make test             # lint + unit tests
uv run pytest --verbose --color=yes doozer/tests/  # Package-specific tests
```

## Documentation Structure

```
agentic/
  design-docs/components/   # Per-component deep dives
  domain/                   # Domain glossary and concepts
  exec-plans/               # Feature execution plans
  decisions/                # Architecture decision records
  references/               # Reference material
```

## When You're Stuck

- Start with [ARCHITECTURE.md](./ARCHITECTURE.md) for system-wide understanding.
- Check [CLAUDE.md](./CLAUDE.md) for dev setup, commands, and working conventions.
- Search `agentic/decisions/` for prior architectural decisions.
