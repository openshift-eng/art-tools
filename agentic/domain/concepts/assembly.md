---
concept: Assembly
type: Pattern
related:
  - runtime
  - ocp-build-data
  - metadata
  - brew-koji
  - model-missing
---

## Definition

An Assembly is a named configuration that defines how a specific OCP release is composed, what builds are included, and what validation constraints are enforced. Assemblies are defined in the `releases.yml` file within ocp-build-data and support inheritance, allowing child assemblies to layer configuration on top of parent assemblies. The assembly system controls build pinning, RPM consistency checks, and release validation through a combination of types, basis events, and permits.

## Purpose

Assemblies exist to manage the complexity of releasing multiple versions of OpenShift simultaneously. The default `stream` assembly represents continuous development with no constraints, while `standard` assemblies (e.g., `4.17.3`) enforce strict consistency checks to ensure release quality. This system allows ART to pin specific builds, override metadata per-release, track validation issues, and support different release types (GA, candidate, preview, custom) all through declarative YAML configuration rather than code changes.

## Location in Code

- **Core module:** `artcommon/artcommonlib/assembly.py` -- Contains all assembly logic: type resolution, config merging, inheritance traversal, basis event calculation, metadata overrides, and permit checking.
- **AssemblyTypes enum:** Defined in `artcommon/artcommonlib/assembly.py` with values:
  - `STREAM` -- Default. Continuous build, no basis event, minimal constraints.
  - `STANDARD` -- All constraints enforced. Used for GA releases (e.g., `4.17.3`).
  - `CANDIDATE` -- Release candidate or feature candidate.
  - `CUSTOM` -- No constraints enforced. Used for ad-hoc builds.
  - `PREVIEW` -- Preview/next releases (internal name: `.next`).
- **AssemblyIssueCode enum:** Classifies validation problems:
  - `IMPERMISSIBLE` -- Cannot be permitted under any circumstances.
  - `CONFLICTING_INHERITED_DEPENDENCY` -- Override dependency installed at wrong version.
  - `CONFLICTING_GROUP_RPM_INSTALLED` -- Different members installed different versions of the same group RPM.
  - `MISMATCHED_SIBLINGS` -- Containers from same source built from different commits.
  - `OUTDATED_RPMS_IN_STREAM_BUILD` -- Container has different RPM version than assembly specifies.
  - `INCONSISTENT_RHCOS_RPMS` -- Arch-specific RHCOS builds installed different RPM versions.
  - `MISSING_INHERITED_DEPENDENCY` -- Expected dependency not installed.
  - `EMBARGOED_CONTENT` -- Build sync contains embargoed builds.
  - `UNSHIPPABLE_KERNEL` -- Kernel has early-kernel-stop-ship tag.
  - And others (`MISSING_RHCOS_CONTAINER`, `FAILED_CONSISTENCY_REQUIREMENT`, `FAILED_CROSS_RPM_VERSIONS_REQUIREMENT`, `MISMATCHED_NETWORK_MODE`).
- **AssemblyIssue class:** Encapsulates a validation issue with a message, component name, and code.

## Lifecycle

1. **Definition:** Assemblies are defined in `releases.yml` in ocp-build-data under the `releases` key. Each assembly entry has an `assembly` block containing optional fields: `type`, `basis` (with `brew_event`, `time`, or parent `assembly`), `group` overrides, `members` overrides, `streams` overrides, `rhcos` config, `permits`, and `issues`.
2. **Resolution at Runtime:** During `Runtime.initialize()`, the assembly name (from `--assembly` CLI flag, defaulting to `"stream"` for elliott or `"test"` for doozer) is resolved:
   - `assembly_type()` determines the `AssemblyTypes` enum value.
   - `assembly_basis_event()` computes the basis event (Brew event ID or Konflux timestamp), recursing through inherited assemblies.
3. **Config Merging:** Assembly configuration is merged using `_merger()`, which supports special key suffixes:
   - `key!` -- Force-set value (dominant).
   - `key?` -- Set only if not already present (default value).
   - `key-` -- Remove the key entirely.
   - Lists are appended and deduplicated.
4. **Metadata Overrides:** `assembly_metadata_config()` merges per-component overrides from the assembly's `members.images` or `members.rpms` lists onto the base metadata config, respecting inheritance.
5. **Validation:** During release operations, `AssemblyIssue` objects are created for detected problems. `assembly_permits()` checks whether the assembly (or group lifecycle phase) has permits defined that allow specific issue codes to pass.
6. **Component Exclusion:** `assembly_excluded_components()` returns distgit keys marked with `exclude: true` in the assembly definition, respecting inheritance.

### Key Functions

- `assembly_type(releases_config, assembly)` -- Returns AssemblyTypes for the named assembly.
- `assembly_basis_event(releases_config, assembly, strict, build_system)` -- Returns the basis event (int for Brew, datetime for Konflux), recursing through inheritance.
- `assembly_group_config(releases_config, assembly, group_config)` -- Merges assembly group overrides onto the base group config.
- `assembly_metadata_config(releases_config, assembly, meta_type, distgit_key, meta_config)` -- Merges per-component assembly overrides onto metadata config.
- `assembly_permits(releases_config, group_config, assembly)` -- Returns permits list, respecting lifecycle phase (`pre-release` uses `prerelease_permits`).
- `assembly_streams_config(releases_config, assembly, streams_config)` -- Merges assembly stream overrides.
- `assembly_excluded_components(releases_config, assembly, meta_type)` -- Returns set of excluded distgit keys.

## Related Concepts

- [runtime](runtime.md) -- Runtime resolves the assembly type and basis event during initialization and stores them for use by all commands.
- [ocp-build-data](ocp-build-data.md) -- Assembly definitions live in `releases.yml` within ocp-build-data.
- [metadata](metadata.md) -- Per-component metadata config is overridden by assembly member definitions via `assembly_metadata_config()`.
- [brew-koji](brew-koji.md) -- Assembly basis events constrain Brew queries to a point-in-time snapshot.
- [model-missing](model-missing.md) -- Assembly configs are accessed as Model objects, relying on Missing for safe traversal of undefined keys.
