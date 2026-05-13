# Development Guide

## Prerequisites

- **Python 3.11+** (3.11 is the target; 3.12-3.14 are also supported)
- **uv** -- Python package manager ([install instructions](https://docs.astral.sh/uv/getting-started/installation/))
- **Kerberos development libraries** -- `krb5-devel` on Fedora/RHEL, `libkrb5-dev` on Debian/Ubuntu
- **Red Hat internal network access** -- required for Brew, Errata Tool, Bugzilla (VPN or on-site)
- **Git**

## Initial Setup

```bash
git clone https://github.com/openshift-eng/art-tools.git
cd art-tools
make venv
```

`make venv` creates a Python 3.11 virtual environment via `uv venv --python 3.11`, installs all runtime dependencies from `pyproject.toml`, and installs all five packages in editable mode via `./install.sh`.

## Development Workflow

1. Create a topic branch (never commit directly to `main`):
   ```bash
   git fetch origin
   git switch -C descriptive-name origin/main
   ```

2. Make changes.

3. Format code:
   ```bash
   make format
   ```
   This runs `ruff check --fix` and `ruff format` (line length 120, Python 3.11 target).

4. Lint:
   ```bash
   make lint
   ```
   Runs `ruff check` and `ruff format --check`.

5. Run tests:
   ```bash
   make test
   ```
   This runs `make lint` followed by `make unit`.

6. Commit your changes.

7. Push to the `dev` remote (never push to `origin`):
   ```bash
   git push dev descriptive-name
   ```

8. Create a pull request:
   ```bash
   gh pr create --base main
   ```

## Running Tests

### Full test suite

```bash
make test          # lint + all unit tests
make unit          # all unit tests (runs via ./run-tests-parallel.sh)
```

### Per-component unit tests

```bash
make unit-artcommon                # artcommon/tests/
make unit-doozer                   # doozer/tests/
make unit-elliott                  # elliott/tests/
make unit-pyartcd                  # pyartcd/tests/
make unit-ocp-build-data-validator # ocp-build-data-validator/tests/
```

### Specific tests

```bash
# Run a single test file
uv run pytest --verbose --color=yes doozer/tests/test_distgit.py

# Run a specific test function
uv run pytest --verbose --color=yes doozer/tests/test_distgit.py::test_function_name
```

### Functional tests

These require Red Hat internal network access (Brew, Errata Tool, Kerberos):

```bash
make functional-doozer    # doozer/tests_functional/
make functional-elliott   # elliott/functional_tests/
```

### Linting only

```bash
make format-check  # Check formatting without changes (ruff check + ruff format --check)
make lint          # Same as format-check (includes ruff check)
make pylint        # Pylint errors-only pass
```

## Debugging

### Kerberos authentication expired

```bash
kinit <username>@REDHAT.COM
```

### Brew/Koji unreachable

Check VPN connection. Verify connectivity:
```bash
brew hello
```

### ocp-build-data clone failures

- Verify the `--data-path` flag points to a valid repo or URL.
- Confirm the target branch exists (e.g., `openshift-4.17`).
- Check network access to `https://github.com/openshift-eng/ocp-build-data`.

### Debug logging

Use `--debug` on doozer or elliott commands to set log level to DEBUG. Debug output goes to `debug.log` in the working directory.

```bash
doozer --group openshift-4.17 --debug images:list
elliott --group openshift-4.17 --debug find-builds
```

### Inspecting intermediate files

Use `--working-dir` to control where temporary files (distgit clones, brew logs, flags) are written:

```bash
doozer --group openshift-4.17 --working-dir /tmp/doozer-debug images:build
```

## Reinstalling After Changes

```bash
make reinstall       # uv sync --reinstall (quick reinstall of editable packages)
make clean-reinstall # rm -rf .venv && make venv (full clean reinstall)
```

Use `make reinstall` when source code structure changes (new modules, moved files). Use `make clean-reinstall` when dependencies change or the environment is broken.

## Code Organization

The repository contains five packages:

| Package | Directory | CLI Entry Point | Purpose |
|---|---|---|---|
| artcommon | `artcommon/` | (library only) | Shared utilities: Runtime ABC, Model, assembly logic, Konflux integration |
| doozer | `doozer/` | `doozer` | Build management (images, RPMs) via Brew/OSBS and Konflux |
| elliott | `elliott/` | `elliott` | Advisory and errata management |
| pyartcd | `pyartcd/` | `artcd` | Pipeline orchestration (Tekton/Jenkins) |
| validator | `ocp-build-data-validator/` | `validate-ocp-build-data` | Schema validation for ocp-build-data YAML |

Dependency rule: `artcommon` is imported by all other packages. doozer and elliott must not import each other. pyartcd calls doozer and elliott via subprocess, not library import.

See [design-docs/](./design-docs/) for detailed component documentation and [design-docs/components/](./design-docs/components/) for per-package architecture.

## Updating Documentation

When source code changes, the corresponding agentic documentation may need updating. CI checks detect when docs become stale relative to their tracked source files.

### Checking doc quality locally

```bash
make check-docs        # Run all documentation quality metrics
make docs-dashboard    # Generate HTML dashboard at agentic/metrics-dashboard.html
```

### When CI flags stale docs

1. Run `make check-docs` to see which docs are flagged
2. Read the changed source code to understand what changed
3. Update the corresponding concept or component doc
4. Run `make check-docs` again to verify

### Doc-to-code mapping

The CI freshness check (`.github/workflows/validate-agentic-docs.yml`) maps docs to source files. When a source file changes recently but its doc hasn't been updated, CI warns. Key mappings:

| Doc | Tracked Source Files |
|-----|---------------------|
| `agentic/domain/concepts/runtime.md` | `artcommon/artcommonlib/runtime.py`, `doozer/doozerlib/runtime.py`, `elliott/elliottlib/runtime.py` |
| `agentic/domain/concepts/assembly.md` | `artcommon/artcommonlib/assembly.py` |
| `agentic/domain/concepts/metadata.md` | `artcommon/artcommonlib/metadata.py`, `doozer/doozerlib/image.py`, `doozer/doozerlib/rpmcfg.py` |
| `agentic/domain/concepts/brew-koji.md` | `doozer/doozerlib/brew.py`, `elliott/elliottlib/brew.py` |
| `agentic/domain/concepts/distgit.md` | `doozer/doozerlib/distgit.py` |
| `agentic/domain/concepts/errata-advisories.md` | `elliott/elliottlib/errata.py`, `elliott/elliottlib/errata_async.py` |
| `agentic/domain/concepts/konflux.md` | `doozer/doozerlib/backend/konflux_client.py`, `artcommon/artcommonlib/konflux/konflux_db.py` |
| `agentic/domain/concepts/plashet.md` | `doozer/doozerlib/plashet.py` |
| `agentic/domain/concepts/model-missing.md` | `artcommon/artcommonlib/model.py` |
| `agentic/domain/concepts/ocp-build-data.md` | `artcommon/artcommonlib/gitdata.py` |
| `agentic/design-docs/components/doozer.md` | `doozer/doozerlib/cli/__main__.py`, `doozer/doozerlib/runtime.py` |
| `agentic/design-docs/components/elliott.md` | `elliott/elliottlib/cli/__main__.py`, `elliott/elliottlib/runtime.py` |
| `agentic/design-docs/components/pyartcd.md` | `pyartcd/pyartcd/__main__.py` |
| `agentic/design-docs/components/artcommon.md` | `artcommon/artcommonlib/runtime.py`, `artcommon/artcommonlib/assembly.py` |
| `agentic/design-docs/components/validator.md` | `ocp-build-data-validator/validator/__main__.py` |

CI thresholds: warns if source changed in last 30 days but doc is 60+ days stale. Fails if more than 5 warnings.

## Making a Pull Request

See the [Git Conventions section in CLAUDE.md](../CLAUDE.md) for the full workflow. Key rules:

- Always work in topic branches, never commit to `main`.
- Push to the `dev` remote, never to `origin`.
- Force push to your own branch is acceptable.
- Create PRs against `main` via `gh pr create --base main`.
