---
id: ADR-0002
title: Runtime Pattern for CLI Initialization
date: 2026-04-16
status: accepted
deciders: [art-team]
supersedes: null
superseded-by: null
---

# ADR-0002: Runtime Pattern for CLI Initialization

## Context

CLI tools need consistent initialization: clone ocp-build-data, load group config, set up Koji sessions, configure working directories, resolve assemblies. Each command needs access to this shared state. Without a central orchestration object, initialization logic would be duplicated across dozens of CLI commands.

## Decision

Use a `Runtime` class (inheriting from the `GroupRuntime` ABC in artcommon) that encapsulates all session state. Every CLI command receives an initialized Runtime via Click's `pass_runtime` / `pass_obj` pattern. Runtime is initialized with `--group` (e.g. `openshift-4.17`) which determines which ocp-build-data branch to clone.

### Class Hierarchy

```
artcommonlib.runtime.GroupRuntime (ABC)
    |-- doozerlib.runtime.Runtime
    |-- elliottlib.runtime.Runtime
```

`GroupRuntime.__init__` (`artcommon/artcommonlib/runtime.py`) sets up:
- Logging configuration (`debug`, `quiet` flags)
- Konflux DB connection (`KonfluxDb`)
- `build_system` parameter for dual build system support

`GroupRuntime.initialize()` calls `initialize_logging()` and `initialize_konflux_db()`, then optionally sets `self.build_system`.

### Doozer Runtime

`doozer/doozerlib/runtime.py` -- `Runtime(GroupRuntime)`:
- `initialize()` accepts `mode` (images/rpms/both), `clone_distgits`, `build_system`, `config_only`, `group_only`, etc.
- Clones ocp-build-data via `resolve_metadata()` using `gitdata.GitData` and `BuildDataLoader`.
- Loads group config, releases config, streams config.
- Resolves assembly type and basis event.
- Populates `image_map` (Dict[str, ImageMetadata]) and `rpm_map` (Dict[str, RPMMetadata]).
- Manages Koji client sessions via `shared_koji_client_session()` (shared, locked) and `pooled_koji_client_session()` (pooled, up to 30 concurrent).
- Manages distgit cloning, source resolution, freeze automation checks.

### Elliott Runtime

`elliott/elliottlib/runtime.py` -- `Runtime(GroupRuntime)`:
- Simpler than Doozer's: no distgit cloning, no source resolution.
- `initialize()` accepts `mode`, `no_group`, `build_system`, `with_shipment`.
- Loads group config, resolves assembly type/basis event.
- Populates `image_map` and `rpm_map` for metadata queries.
- Provides `get_bug_tracker()` for Bugzilla/JIRA integration.
- Manages Koji sessions identically to Doozer (shared + pooled pattern).

## Rationale

- **Centralizes config loading**: All ocp-build-data parsing, group config merging, and assembly resolution happen in one place.
- **Avoids global state**: Runtime is an explicit object passed to commands, not a module-level singleton.
- **Makes commands testable**: Mock the Runtime to test CLI commands without cloning repos or connecting to Brew.
- **`--group` as universal entry point**: Maps naturally to the ocp-build-data branch structure (e.g. `openshift-4.17` branch).

## Consequences

### Positive

- Single initialization path for all commands.
- Testable: mock Runtime and inject test data.
- Consistent behavior across all commands in a tool.

### Negative

- Runtime becomes very large: doozer's `Runtime` class is ~1400 lines with many responsibilities (metadata loading, Koji sessions, distgit management, source resolution).
- Initialization is expensive: clones a git repo, connects to Konflux DB, optionally authenticates to Brew.

### Neutral

- pyartcd has its own simpler `Runtime` (`pyartcd/pyartcd/runtime.py`) that orchestrates doozer/elliott via subprocess rather than importing their Runtimes directly.

## Implementation

- `GroupRuntime` ABC: `artcommon/artcommonlib/runtime.py` -- defines `group_config` abstract property, `initialize()`, logging, Konflux DB.
- Doozer Runtime: `doozer/doozerlib/runtime.py` -- inherits GroupRuntime, adds Koji sessions, metadata loading, distgit management.
- Elliott Runtime: `elliott/elliottlib/runtime.py` -- inherits GroupRuntime, adds errata/bugzilla config, shipment metadata.
- CLI commands use Click decorators (`@click.pass_obj` or custom `pass_runtime`) to receive Runtime.
