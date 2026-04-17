---
component: artcommon
type: Library
related: [doozer, elliott, pyartcd]
---

# artcommon (artcommonlib)

## Purpose

artcommonlib is the shared library used by all art-tools components. It provides common abstractions for runtime management, YAML model traversal, assembly logic, subprocess execution, ocp-build-data loading, embargo handling, and Konflux integration.

## Location

| What | Path |
|------|------|
| Package root | `artcommon/artcommonlib/` |
| Tests | `artcommon/tests/` |
| Package config | `artcommon/pyproject.toml` |

## Key Modules

### Runtime and Metadata

- `runtime.py` (~79 lines) -- `GroupRuntime` abstract base class. Provides initialization of logging and Konflux DB. Declares the abstract `group_config` property. Extended by doozer's and elliott's `Runtime` classes.

- `metadata.py` -- `MetadataBase` class, base for `ImageMetadata` and `RPMMetadata`. Loads raw config from ocp-build-data, applies assembly overrides via `assembly_metadata_config()`, and extracts component namespace and name. Defines `CONFIG_MODES`: `enabled`, `disabled`, `wip`.

- `model.py` -- `Model`, `ListModel`, `MissingModel`, and the `Missing` singleton. Provides safe YAML/dict traversal where accessing a non-existent key returns `Missing` (falsy) instead of raising `KeyError`. This is used pervasively throughout the codebase for config access.

### Assembly System

- `assembly.py` -- Core assembly logic. Defines `AssemblyTypes` enum (`STREAM`, `STANDARD`, `CANDIDATE`, `CUSTOM`, `PREVIEW`), `AssemblyIssueCode` enum for constraint violations, and functions for:
  - `assembly_type()` -- Determine assembly type from releases config
  - `assembly_basis_event()` -- Get the basis event for an assembly
  - `assembly_metadata_config()` -- Apply assembly-specific overrides to metadata
  - `assembly_streams_config()` -- Apply assembly-specific stream overrides

### Build Data Loading

- `gitdata.py` (~13KB) -- `GitData` class for cloning and reading ocp-build-data. Handles git clone, checkout of specific commitish, and loading YAML data files from paths (images/, rpms/).

- `config/` -- Configuration loading subsystem:
  - `config/__init__.py` -- `BuildDataLoader` class for loading group config, releases config, and per-component configs with variable substitution and assembly overrides
  - `config/plashet.py` -- Pydantic models for plashet (RPM repository) configuration
  - `config/repo.py` -- Pydantic models for repository configuration (`Repo`, `RepoList`, `ContentSet`, `RepoSync`)

### Execution Utilities

- `exectools.py` (~31KB) -- Subprocess execution utilities. Provides:
  - `cmd_assert()` -- Run command, raise on non-zero exit
  - `cmd_gather()` -- Run command, capture stdout/stderr/rc
  - `parallel_exec()` -- Parallel execution with thread pool
  - `limit_concurrency()` -- Semaphore-based concurrency limiting
  - Timer context managers for performance tracking

### General Utilities

- `util.py` (~49KB, largest module) -- General-purpose utilities including:
  - `isolate_el_version_in_brew_tag()` -- Extract RHEL version from Brew tag
  - `deep_merge()` -- Deep dictionary merging
  - `convert_remote_git_to_https()` -- Git URL normalization
  - Various string, version, and data manipulation helpers

### Embargo and Build Visibility

- `build_visibility.py` -- `BuildVisibility` enum (`PUBLIC`, `PRIVATE`) and visibility suffix system. Maps build system (brew/konflux) and visibility to p-flags:
  - Brew: `p0` (public), `p1` (private)
  - Konflux: `p2` (public), `p3` (private)
  - `is_release_embargoed()` -- Check if a release string indicates embargo (defaults to embargoed if unknown)
  - `isolate_pflag_in_release()` -- Extract p-flag from release string

### Git and Source Management

- `git_helper.py` -- Git operation helpers
- `github_auth.py` (~16KB) -- GitHub App authentication for API access
- `gitlab.py` -- GitLab integration

### Konflux Integration

Located in `artcommon/artcommonlib/konflux/`:

- `konflux_db.py` (~61KB) -- `KonfluxDb` class for querying the Konflux build database (BigQuery-backed). Handles build record queries, caching, and batch operations.
- `konflux_build_record.py` (~17KB) -- `KonfluxBuildRecord` dataclass and `KonfluxBuildOutcome` enum. Represents individual Konflux build records with their metadata.
- `package_rpm_finder.py` -- Utilities for finding RPMs in Konflux builds.

### Brew Integration

- `brew.py` -- Shared Brew/Koji utilities including `BuildStates` enum.

### Other Modules

- `arch_util.py` -- Architecture name mapping and utilities
- `bigquery.py` -- BigQuery client wrapper (used by Konflux DB)
- `build_util.py` -- Build-related utility functions
- `constants.py` -- Shared constants (URLs, tags, advisory types)
- `dotconfig.py` -- User configuration file management (~/.config/)
- `exceptions.py` -- Common exception classes
- `format_util.py` -- Terminal output formatting (colored printing)
- `jira_config.py` -- Jira server configuration constants
- `lock.py` -- Local locking utilities
- `logutil.py` -- Logging setup and configuration
- `oc_image_info.py` -- Parse `oc image info` output
- `pushd.py` -- `Dir` context manager for directory changes
- `redis.py` -- Redis client wrapper
- `release_util.py` -- Release name and version utilities
- `rhcos.py` -- RHCOS-related utilities
- `rpm_utils.py` -- RPM version comparison and manipulation
- `telemetry.py` -- OpenTelemetry integration
- `variants.py` -- `BuildVariant` enum (`OCP`, `OKD`)

## Architecture

artcommonlib follows a layered design:

1. **Data layer** -- `model.py` (safe dict/list traversal), `gitdata.py` (ocp-build-data loading), `config/` (structured config loading)
2. **Domain layer** -- `assembly.py` (release semantics), `build_visibility.py` (embargo logic), `metadata.py` (component metadata)
3. **Runtime layer** -- `runtime.py` (session management ABC)
4. **Integration layer** -- `konflux/` (Konflux DB), `brew.py` (Koji), `github_auth.py` (GitHub)
5. **Utility layer** -- `exectools.py`, `util.py`, `format_util.py`, `rpm_utils.py`

## Dependencies

artcommonlib has minimal external dependencies by design. It is imported by doozer, elliott, and pyartcd. Key external dependencies include:
- `pyyaml` -- YAML parsing
- `pydantic` -- Config model validation (in config/ submodule)
- `google-cloud-bigquery` -- Konflux DB queries
- `tenacity` -- Retry logic (used in consuming tools)

## Related Components

- **doozer** -- Extends GroupRuntime, uses Model/Missing, assembly, metadata, exectools
- **elliott** -- Extends GroupRuntime, uses Model/Missing, assembly, build_visibility
- **pyartcd** -- Uses exectools, assembly, logging utilities
