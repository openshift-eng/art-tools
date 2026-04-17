---
concept: Metadata
type: Class
related:
  - runtime
  - ocp-build-data
  - assembly
  - distgit
  - brew-koji
  - konflux
  - model-missing
---

## Definition

Metadata is the class hierarchy that represents a single buildable component (container image or RPM) in the OCP release process. Each Metadata instance is loaded from a YAML file in ocp-build-data and encapsulates the component's configuration, distgit information, build targets, and build history. The hierarchy spans artcommon, doozer, and elliott with specialized subclasses for images and RPMs in each tool.

## Purpose

Metadata exists to provide a unified interface for querying and manipulating individual build components. It abstracts the details of how component configuration is loaded (from YAML files with assembly overrides), how builds are discovered (from Brew or Konflux), and how distgit repositories are managed. Every operation that touches a specific component -- rebasing, building, finding latest builds, checking scan results -- goes through its Metadata object.

## Location in Code

### Class Hierarchy

```
MetadataBase (artcommon/artcommonlib/metadata.py)
|
+-- Metadata (doozer/doozerlib/metadata.py)
|   |-- ImageMetadata (doozer/doozerlib/image.py)
|   |-- RPMMetadata (doozer/doozerlib/rpmcfg.py)
|
+-- Metadata (elliott/elliottlib/metadata.py)
    |-- ImageMetadata (elliott/elliottlib/imagecfg.py)
    |-- RPMMetadata (elliott/elliottlib/rpmcfg.py)
```

- **MetadataBase:** `artcommon/artcommonlib/metadata.py` -- The shared base class. Handles config loading from `DataObj`, assembly metadata config merging via `assembly_metadata_config()`, component name extraction, branch determination, build target resolution, and latest build queries for both Brew and Konflux.
- **Doozer Metadata:** `doozer/doozerlib/metadata.py` -- Extends MetadataBase with distgit management (`DistGitRepo`), source resolution, rebuild hint detection, scan-sources logic, Cgit feed parsing, and upstream commit tracking.
- **Doozer ImageMetadata:** `doozer/doozerlib/image.py` -- Image-specific operations: Dockerfile management, parent image resolution, golang builder version extraction, build ordering dependencies, and payload/release classification.
- **Doozer RPMMetadata:** `doozer/doozerlib/rpmcfg.py` -- RPM-specific operations: spec file management, source cloning, version/release tracking, and kube environment variable extraction.
- **Elliott Metadata:** `elliott/elliottlib/metadata.py` -- Thin wrapper that simply inherits from MetadataBase (12 lines).
- **Elliott ImageMetadata:** `elliott/elliottlib/imagecfg.py` -- Provides image properties: `image_name`, `is_release`, `is_payload`, `is_olm_operator`, `base_only`.
- **Elliott RPMMetadata:** `elliott/elliottlib/rpmcfg.py` -- Provides `rpm_name` from config (7 lines).

## Lifecycle

1. **Loading:** During `Runtime.initialize()`, YAML files from `ocp-build-data/images/` and `ocp-build-data/rpms/` are loaded via `GitData.load_data()` into `DataObj` instances. Each `DataObj` is passed to the appropriate Metadata constructor.
2. **Config Assembly:** In `MetadataBase.__init__()`, the raw YAML config is wrapped in a `Model` (`self.raw_config`), then merged with assembly overrides via `assembly_metadata_config()` to produce `self.config`. This means `self.config` reflects both the base YAML and any assembly-specific overrides.
3. **Registration:** The constructed Metadata is added to `runtime.image_map` or `runtime.rpm_map`, keyed by `distgit_key`. The component name is also registered in `runtime.component_map`.
4. **Usage:** Commands access metadata through the runtime maps. Common operations:
   - `meta.get_latest_build()` -- Find latest build in Brew or Konflux (delegates based on `runtime.build_system`).
   - `meta.get_component_name()` -- Get the Brew/Konflux component name (e.g., `"openshift-enterprise-cli-container"`).
   - `meta.branch()` -- Get the distgit branch for the component.
   - `meta.determine_targets()` -- Get Brew build targets.
   - `meta.config.some.nested.key` -- Access any config value safely via Model.
5. **Distgit Operations (Doozer):** Doozer's Metadata creates a `DistGitRepo` for cloning and managing the downstream distribution git repository.
6. **Destruction:** Metadata objects are not explicitly destroyed; they live as long as the Runtime.

### Key Attributes

- `distgit_key` -- Unique identifier derived from the YAML filename (e.g., `"openshift-enterprise-cli"`).
- `name` -- Base name without differentiator suffixes (split on `.`).
- `config` -- `Model` object with assembly-merged configuration.
- `raw_config` -- `Model` object with unmerged base configuration.
- `meta_type` -- `"image"` or `"rpm"`.
- `namespace` -- `"containers"`, `"rpms"`, or `"apbs"`.
- `mode` -- `"enabled"`, `"disabled"`, or `"wip"`.
- `enabled` -- Boolean, True when mode is `"enabled"`.
- `qualified_key` -- `"{namespace}/{distgit_key}"` (e.g., `"containers/openshift-enterprise-cli"`).

## Related Concepts

- [runtime](runtime.md) -- Runtime creates Metadata objects during initialization and stores them in image_map/rpm_map.
- [ocp-build-data](ocp-build-data.md) -- Each Metadata instance is loaded from a YAML file in ocp-build-data.
- [assembly](assembly.md) -- Assembly member overrides are merged into metadata config via `assembly_metadata_config()`.
- [distgit](distgit.md) -- Doozer's Metadata creates DistGitRepo instances for downstream repository management.
- [brew-koji](brew-koji.md) -- `get_latest_brew_build()` queries Koji for the component's latest build using the component name and assembly.
- [konflux](konflux.md) -- `get_latest_konflux_build()` queries Konflux DB for the component's latest build.
- [model-missing](model-missing.md) -- `self.config` is a Model object, enabling safe traversal of arbitrarily nested configuration.
