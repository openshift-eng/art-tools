---
concept: Runtime
type: Class
related:
  - assembly
  - ocp-build-data
  - metadata
  - brew-koji
  - distgit
  - model-missing
---

## Definition

Runtime is the central orchestration object in art-tools that initializes and manages the entire context for a tool invocation. It is responsible for cloning ocp-build-data, loading group configuration, creating Metadata objects, and providing shared resources such as Koji client sessions, working directories, and assembly configuration. Three distinct Runtime implementations exist across the codebase: doozer's full-featured Runtime, elliott's advisory-focused Runtime, and pyartcd's lightweight pipeline Runtime.

## Purpose

Runtime exists to serve as the single entry point for all configuration, state, and resource management during a tool run. Every CLI command in doozer and elliott receives an initialized Runtime, which guarantees that ocp-build-data has been cloned, group config has been loaded, metadata has been parsed, and build system connections are available. Without Runtime, each command would need to independently handle data loading, authentication, and state management.

## Location in Code

- **GroupRuntime (ABC):** `artcommon/artcommonlib/runtime.py` -- Abstract base class defining the interface. Initializes logging and Konflux DB connection. Declares the abstract `group_config` property.
- **Doozer Runtime:** `doozer/doozerlib/runtime.py` -- Full implementation (~1400 lines). Inherits from GroupRuntime. Manages image_map, rpm_map, component_map, source_resolver, distgits_dir, repos, streams, build ordering, and freeze automation state.
- **Elliott Runtime:** `elliott/elliottlib/runtime.py` -- Advisory-focused implementation (~575 lines). Inherits from GroupRuntime. Manages image_map, rpm_map, bug trackers, shipment metadata, and assembly basis events.
- **pyartcd Runtime:** `pyartcd/pyartcd/runtime.py` -- Lightweight pipeline runtime (~60 lines). Does NOT inherit from GroupRuntime. Holds config dict, working_dir, dry_run flag, and factory methods for Jira/Slack/Mail clients.
- **pyartcd GroupRuntime:** `pyartcd/pyartcd/runtime.py` -- A separate class in the same file that DOES inherit from `artcommonlib.runtime.GroupRuntime`. Used when pipeline code needs group configuration.

## Lifecycle

1. **Construction:** Created by Click CLI decorators (`@click.pass_context`) with keyword arguments from CLI options (`--group`, `--working-dir`, `--assembly`, `--data-path`, etc.).
2. **Initialization:** `runtime.initialize()` is called at the start of each command. This:
   - Creates/validates the working directory (or a temp directory).
   - Calls `super().initialize()` to set up logging and Konflux DB.
   - Clones ocp-build-data via `resolve_metadata()` which uses `BuildDataLoader` and `GitData`.
   - Loads `group.yml` into `self.group_config` (a `Model` object).
   - Loads `releases.yml` into `self.releases_config`.
   - Determines `assembly_type` and `assembly_basis_event`.
   - Loads image and RPM metadata YAML files, creating `ImageMetadata` and `RPMMetadata` objects populating `self.image_map` and `self.rpm_map`.
   - (Doozer only) Sets up source_resolver, repos, streams, distgits_dir, record_logger, arches, and freeze_automation.
3. **Usage:** Commands access runtime attributes throughout their execution -- `runtime.image_map`, `runtime.shared_koji_client_session()`, `runtime.group_config`, `runtime.assembly`, `runtime.brew_event`, etc.
4. **Destruction:** On exit, if a temp working directory was created, it is cleaned up via `atexit.register(remove_tmp_working_dir, self)`.

### Key Attributes (Doozer Runtime)

- `group` -- Group name string (e.g., `"openshift-4.17"`)
- `assembly` -- Assembly name string (e.g., `"stream"`, `"4.17.3"`)
- `group_config` -- `Model` wrapping the loaded `group.yml`
- `releases_config` -- `Model` wrapping the loaded `releases.yml`
- `image_map` -- `Dict[str, ImageMetadata]` keyed by distgit_key
- `rpm_map` -- `Dict[str, RPMMetadata]` keyed by distgit_key
- `brew_event` -- Optional Koji event ID constraining all Brew queries
- `assembly_basis_event` -- Event derived from assembly's basis config
- `distgits_dir` -- Path to the directory where distgit repos are cloned
- `working_dir` -- Root working directory for the invocation
- `build_system` -- `"brew"` or `"konflux"`
- `konflux_db` -- `KonfluxDb` instance (from GroupRuntime)

## Related Concepts

- [assembly](assembly.md) -- Runtime determines and stores the assembly type and basis event during initialization.
- [ocp-build-data](ocp-build-data.md) -- Runtime clones this repository and loads all configuration from it.
- [metadata](metadata.md) -- Runtime creates and holds all ImageMetadata and RPMMetadata objects.
- [brew-koji](brew-koji.md) -- Runtime provides shared Koji client sessions via `shared_koji_client_session()`.
- [distgit](distgit.md) -- Runtime manages `distgits_dir` where distgit repositories are cloned.
- [model-missing](model-missing.md) -- Runtime's `group_config` and `releases_config` are Model objects enabling safe traversal.
