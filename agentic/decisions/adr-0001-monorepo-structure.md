---
id: ADR-0001
title: Monorepo Structure for Release Tools
date: 2026-04-16
status: accepted
deciders: [art-team]
supersedes: null
superseded-by: null
---

# ADR-0001: Monorepo Structure for Release Tools

## Context

doozer and elliott were originally separate repositories. As the tooling grew, artcommon was extracted to hold shared code (assembly logic, exectools, model utilities, Konflux integration), and pyartcd was added for pipeline automation. Managing dependencies and coordinated releases across 4+ repos became painful: a single artcommon change required updating pinned versions in doozer, elliott, and pyartcd, then testing each independently.

## Decision

Consolidate all tools into a single monorepo with a shared `pyproject.toml`, a single virtual environment managed by `uv`, and unified CI.

The repository layout:

```
art-tools/
  artcommon/artcommonlib/     -- shared library
  doozer/doozerlib/           -- build management CLI
  elliott/elliottlib/         -- advisory management CLI
  pyartcd/pyartcd/            -- pipeline automation CLI
  ocp-build-data-validator/   -- config schema validator
  pyproject.toml              -- single project config, all deps
  install.sh                  -- editable install via uv sync
  Makefile                    -- dev commands
```

## Rationale

- **Single venv** avoids dependency version conflicts between tools. All packages share the same resolved dependency tree via `uv.lock`.
- **Atomic commits** enable cross-tool changes. A refactor in artcommon and its consumers can land in one PR.
- **Shared CI** catches integration issues early. `.github/workflows/unit-tests.yaml` runs `make test` (lint + all unit tests) on every PR.
- **artcommon changes** are immediately testable against all consumers without publishing intermediate versions.

## Consequences

### Positive

- Shared dependencies: one `pyproject.toml` with a single resolved lock file.
- Single CI pipeline: `make test` runs lint and unit tests for all packages.
- Atomic cross-tool changes: artcommon API changes + consumer updates in one commit.
- Easier onboarding: `make venv` sets up everything.

### Negative

- Larger repo: all tools share one git history.
- All tools share Python version constraints (>=3.11).
- CI runs all tests even for single-tool changes (mitigated by parallel test runner `run-tests-parallel.sh`).

### Neutral

- Each tool retains its own directory, entry point, and test suite.
- Per-component test targets exist: `make unit-doozer`, `make unit-elliott`, `make unit-pyartcd`, `make unit-artcommon`, `make unit-ocp-build-data-validator`.

## Implementation

All packages are installed as editable via `install.sh`, which runs `uv sync --active`. `pyproject.toml` defines all entry points:

- `doozer` -> `doozerlib.cli:cli`
- `elliott` -> `elliottlib.cli:cli`
- `artcd` -> `pyartcd.__main__:main`
- `validate-ocp-build-data` -> validator entry point

The `Makefile` provides per-component test targets and a unified `make test` target. `make venv` creates a Python 3.11 venv and installs all packages.

## Alternatives Considered

- **Separate repos** (rejected): Dependency coordination overhead made cross-tool changes require 3-4 PRs with version bumps.
- **Git submodules** (rejected): Added complexity (submodule update discipline) without the benefits of a true monorepo (shared lockfile, single CI).
