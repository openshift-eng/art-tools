---
concept: ocp-build-data
type: ExternalSystem
related:
  - runtime
  - assembly
  - metadata
  - model-missing
  - plashet
---

## Definition

ocp-build-data is an external Git repository (`github.com/openshift-eng/ocp-build-data`) that serves as the single source of truth for all build and release configuration in the OpenShift Container Platform release process. It contains YAML configuration files organized by version groups (e.g., `openshift-4.17`, `openshift-4.18`), including per-component image and RPM metadata, group-level configuration, streams definitions, assembly/release definitions, and errata tool configuration. All art-tools (doozer, elliott, pyartcd) clone and read from this repository at runtime.

## Purpose

ocp-build-data exists to decouple build/release configuration from tool logic. By storing all component definitions, version constraints, assembly configurations, and release parameters in a separate data repository, the ART team can modify build behavior without changing tool code. This also enables per-version configuration (different groups for different OCP versions), reproducible builds (assembly basis events pin configuration to a point in time), and collaborative editing of release parameters through standard Git workflows.

## Location in Code

- **GitData class:** `artcommon/artcommonlib/gitdata.py` -- Handles cloning the data repo, checking out the correct branch/commitish, and loading YAML files. The `GitData.clone_data()` method handles both remote Git URLs and local directory paths. `GitData.load_data()` reads YAML files from a subdirectory, supports filtering, exclusion, key-based selection, and variable substitution.
- **DataObj class:** `artcommon/artcommonlib/gitdata.py` -- Wraps a single loaded YAML file with its key, path, and parsed data. Supports reload and save operations using `ruamel.yaml` to preserve formatting.
- **BuildDataLoader:** `artcommon/artcommonlib/config/` -- Higher-level loader that uses GitData to load group config, releases config, and other configuration files with assembly merging support.
- **Runtime integration:** Both `doozer/doozerlib/runtime.py` and `elliott/elliottlib/runtime.py` call `resolve_metadata()` during initialization, which clones ocp-build-data and populates metadata maps.
- **CLI flags:** `--data-path` overrides the default repo URL. Environment variables `DOOZER_DATA_PATH` and `ELLIOTT_DATA_PATH` also work. `--group` specifies the branch to check out (e.g., `openshift-4.17`). Append `@commitish` to pin a specific commit.

## Lifecycle

1. **Clone:** When a Runtime initializes, it clones ocp-build-data into the working directory using `GitData.clone_data()`. The branch is determined by the `--group` parameter. If a local clone already exists and is up to date, cloning is skipped.
2. **Load Group Config:** `group.yml` is loaded from the group's root directory, parsed into a `Model` object, and optionally merged with assembly-level group overrides via `assembly_group_config()`.
3. **Load Releases Config:** `releases.yml` is loaded to resolve assembly definitions, basis events, and member overrides.
4. **Load Metadata:** YAML files from the `images/` and `rpms/` subdirectories are loaded via `GitData.load_data()`. Each file becomes a `DataObj` whose data is wrapped in a `Model` and passed to `ImageMetadata` or `RPMMetadata` constructors. Files can be filtered by mode (`enabled`, `disabled`, `wip`) and by explicit include/exclude lists.
5. **Load Additional Config:** Streams config (`streams.yml`), errata tool config (`erratatool.yml`), and other files are loaded as needed, with variable substitution (e.g., `{MAJOR}`, `{MINOR}`, `{runtime_assembly}`).
6. **Read-only Usage:** Throughout a tool run, the cloned data is read but never modified by the tools (except rare admin operations). Changes to ocp-build-data are made through separate PRs to the repository.

### Repository Structure

```
openshift-4.17/           # Group branch
  group.yml               # Group-level configuration (arches, branch, vars, repos, etc.)
  streams.yml             # Stream image definitions (builders, base images)
  releases.yml            # Assembly/release definitions
  erratatool.yml          # Errata tool configuration
  images/                 # One YAML file per container image component
    openshift-enterprise-cli.yml
    ose-node.yml
    ...
  rpms/                   # One YAML file per RPM component
    openshift-clients.yml
    cri-o.yml
    ...
```

### Key Configuration Fields (group.yml)

- `name` -- Group name (must match branch name)
- `vars` -- Template variables (MAJOR, MINOR, etc.)
- `arches` -- Supported architectures
- `branch` -- Default distgit branch
- `advisories` -- Default advisory IDs
- `freeze_automation` -- Build freeze state
- `software_lifecycle.phase` -- Release phase (e.g., `pre-release`)
- `repos` / `all_repos` -- RPM repository definitions
- `plashet` -- Plashet RPM composition configuration

## Related Concepts

- [runtime](runtime.md) -- Runtime clones ocp-build-data during initialization and loads all configuration from it.
- [assembly](assembly.md) -- Assembly definitions live in `releases.yml` within ocp-build-data.
- [metadata](metadata.md) -- Each image/RPM YAML file in ocp-build-data becomes a Metadata object.
- [model-missing](model-missing.md) -- All loaded YAML data is wrapped in Model objects for safe traversal.
- [plashet](plashet.md) -- Plashet configuration references Brew tags and RPM repos defined in ocp-build-data.
