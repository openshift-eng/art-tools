# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

If the file `.claude/CLAUDE.md` exists, read it. If instructions contradict (for example for what to run with test or run commands), `.claude/CLAUDE.md` takes precedence over this file.

## Overview

This is **art-tools**, a collection of Release tools for managing OpenShift Container Platform (OCP) releases. The repository contains multiple Python packages that work together to automate the OCP release process.

### Core Components

- **doozer** (`./doozer/`) - CLI tool for managing builds via OSBS/Brew (RPMs and container images)
- **elliott** (`./elliott/`) - CLI tool for managing release advisories, errata, and bugs
- **pyartcd** (`./pyartcd/`) - Pipeline code for automated release pipelines (entry point: `artcd`)
- **artcommon** (`./artcommon/`) - Common package used by doozer, elliott, and pyartcd
- **ocp-build-data-validator** (`./ocp-build-data-validator/`) - Schema validator for ocp-build-data

## Development Setup

### Environment Setup

Python 3.11 is the target version (minimum Python 3.11 required). Use `uv` for package management:

```bash
# Create virtual environment and install all dependencies
make venv

# For quick reinstall of editable packages after structure changes
# (uses uv sync --reinstall to force reinstall all editable packages)
make reinstall

# Complete clean reinstall (removes .venv)
make clean-reinstall
```

The `make venv` command:

1. Creates a Python 3.11 virtual environment
2. Installs runtime dependencies from all packages
3. Installs all packages in editable mode via `./install.sh`
4. Sets up development and test dependencies

### Key Commands

**Linting and Formatting:**

```bash
make format          # Auto-format code with ruff
make format-check    # Check formatting without changes
make lint            # Run linting checks (includes format-check)
make pylint          # Run pylint for errors only
```

**Testing:**

```bash
make unit            # Run all unit tests across all packages
make test            # Run lint + unit tests

# Run specific package tests
uv run pytest --verbose --color=yes doozer/tests/
uv run pytest --verbose --color=yes elliott/tests/
uv run pytest --verbose --color=yes pyartcd/tests/

# Functional tests
make functional-elliott
make functional-doozer
```

**Running Single Tests:**

```bash
# Run a specific test file
uv run pytest --verbose --color=yes doozer/tests/test_<module>.py

# Run a specific test function
uv run pytest --verbose --color=yes doozer/tests/test_<module>.py::test_function_name
```

**Other Useful Commands:**

```bash
# Generate shipment schema from elliottlib model
make gen-shipment-schema
```

## Git Conventions

### Branch Workflow

**IMPORTANT: Always work in topic branches. Never commit directly to `main`.**

```bash
# Create a feature branch for your work
git checkout -b your-feature-or-bug-name

# Make your changes, commit, and disover rebase errors before pushing:
git add .
git commit -m "Your commit message"
git fetch origin
git rebase origin/main
```

### Remote Configuration

**CRITICAL: Never push to the `origin` remote. Always use the `dev` remote.**

```bash
# Check your current remotes
git remote -v

# Push to dev remote (NOT origin)
git push dev your-feature-or-bug-name

# Create a pull request from your feature branch
gh pr create --base main
```

### Workflow Summary

1. **Branch Creation**: Always create a topical branch from `main`
   ```bash
   git fetch origin
   git switch -C descriptive-name origin/main
   ```

2. **Development**: Make changes, commit regularly to your feature branch

3. **Push Changes**: Push to `dev` remote only. Pushing with `--force` is allowed.
   ```bash
   git push dev descriptive-name
   ```

4. **Pull Request**: Create PR against `main` branch

5. **Never**:
   - Commit directly to `main`
   - Push to `origin` remote
   - Force push to shared branches without coordination

## Architecture

### Build Data and Metadata System

All tools depend on **ocp-build-data** (<https://github.com/openshift-eng/ocp-build-data>), a separate Git repository that contains:

- Group configurations (e.g., `openshift-4.17`, `openshift-4.18`)
- Image and RPM metadata YAML files
- Errata tool configuration
- Jira query configuration
- Streams and assembly definitions

The tools clone and read from this repository at runtime. Set the data path via:

- `--data-path` CLI flag
- Environment variables: `DOOZER_DATA_PATH`, `ELLIOTT_DATA_PATH`
- Default: <https://github.com/openshift-eng/ocp-build-data>

### Runtime and Initialization Pattern

Both doozer and elliott follow a similar architecture:

- **Runtime class** (`doozerlib/runtime.py`, `elliottlib/runtime.py`) - Central orchestration object
- The Runtime is initialized with a `--group` parameter (e.g., `openshift-4.17`)
- Runtime clones ocp-build-data and loads group configuration
- Metadata objects (`ImageMetadata`, `RPMMetadata`) are created for each component
- CLI commands in `doozerlib/cli/` and `elliottlib/cli/` receive the initialized Runtime

### Assembly System

Assemblies are named releases or checkpoints. Key concepts:

- **stream** assembly: Latest ongoing development (default: `stream`)
- **standard** assembly: Named releases (e.g., `4.17.1`, `4.18.0-ec.3`)
- Assembly basis events: Brew/Konflux events that pin the build state
- Assembly configuration in `releases.yml` in ocp-build-data

### Brew and Koji Integration

Doozer and Elliott interact with Red Hat's Brew (Koji) build system:

- `doozerlib/brew.py`, `elliottlib/brew.py` - Brew/Koji API wrappers
- `Runtime.shared_koji_client_session()` - Shared, authenticated Koji client
- `Runtime.brew_event` - Optional event ID to constrain queries to a point-in-time
- Builds are tagged with candidate tags (e.g., `rhaos-4.17-rhel-9-candidate`)

### Distgit Management

Doozer manages distgit repositories (RPM/container source repos):

- `doozerlib/distgit.py` - Core distgit cloning and management
- `Runtime.distgits_dir` - Working directory for cloned distgits
- Metadata classes (`ImageMetadata`, `RPMMetadata`) encapsulate distgit operations
- Use `rhpkg` commands for distgit interaction (build, push, etc.)

### Pipeline Architecture (pyartcd)

pyartcd contains Tekton/Jenkins pipeline implementations:

- Entry point: `artcd` command (`pyartcd/__main__.py`)
- Pipeline modules in `pyartcd/pipelines/` (e.g., `ocp4.py`, `prepare_release.py`)
- Pipelines orchestrate doozer and elliott commands
- Integration with Jenkins, Jira, Slack, UMB messaging

## Working with This Codebase

### When modifying doozer

- CLI commands are in `doozerlib/cli/` - each command is a separate module
- Core functionality is in `doozerlib/` (e.g., `image.py`, `distgit.py`, `brew.py`)
- Runtime initialization happens in `runtime.py` via `Runtime.initialize()`
- Test structure mirrors source: `doozer/tests/` for unit tests

### When modifying elliott

- CLI commands are in `elliottlib/cli/`
- Errata Tool API interactions are in `elliottlib/errata.py` and `errata_async.py`
- Bugzilla queries are in `elliottlib/bzutil.py`
- Elliott uses `pyproject.toml` (not `setup.py`) for dependency management

### When modifying pyartcd

- Pipelines are in `pyartcd/pipelines/` - one file per pipeline type
- Common utilities: `jenkins.py`, `jira_client.py`, `slack.py`, `locks.py`
- Pipelines typically shell out to doozer/elliott commands

### When modifying artcommon

- Shared utilities used across all tools
- Key modules: `artcommonlib/assembly.py`, `artcommonlib/exectools.py`
- Changes here affect doozer, elliott, and pyartcd
- Konflux integration code is in `artcommonlib/konflux/`

## Configuration Files

- `pyproject.toml` - Main project configuration including dependencies, build settings, and ruff configuration (line length: 120)
- `.pylintrc` - Pylint configuration
- `Makefile` - Primary development commands
- `.devcontainer/Containerfile` - Development environment setup (system dependencies)

## Important Notes

- The codebase requires Red Hat internal systems access (Brew, Errata Tool, Bugzilla)
- Kerberos authentication is required for most operations
- Many operations require `--group` parameter to specify the OCP version
- The `--working-dir` parameter controls where temporary files are stored
- Use `--assembly` to work with specific releases (defaults to `stream`)
