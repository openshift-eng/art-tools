---
concept: Plashet
type: Pattern
related:
  - brew-koji
  - runtime
  - metadata
  - assembly
  - ocp-build-data
---

## Definition

A plashet is a composed RPM repository assembled from builds in specific Brew tags, used to provide a consistent set of RPM dependencies for container image builds. The PlashetBuilder queries Brew for the latest (or assembly-specific) RPM builds from configured tags, downloads the RPMs, and creates a local yum/dnf repository. This ensures that all container images in a release are built against the same RPM versions, preventing dependency skew.

## Purpose

Plashets exist to solve the RPM consistency problem in container image builds. Without plashets, container image builds would pull RPMs from live repositories where package versions change constantly, leading to non-reproducible builds and potential version mismatches between sibling images. By composing a fixed RPM repository from specific Brew-tagged builds, plashets ensure that every image in a release sees the same RPM versions, which is critical for release consistency and assembly validation.

## Location in Code

- **PlashetBuilder:** `doozer/doozerlib/plashet.py` -- Core logic for assembling RPM repositories from Brew tags. Key methods:
  - `from_tag(tag, inherit, assembly, event, only)` -- Returns RPM builds from a specified Brew tag. When assembly is set, uses `find_latest_builds()` to find assembly-appropriate builds rather than just the tag-latest. Caches build lookups internally.
  - `_get_builds(ids_or_nvrs)` -- Batch-fetches build dicts from Brew with internal caching to avoid redundant queries.
  - `_cache_build(build)` -- Saves build dicts indexed by both build_id and NVR.
- **CLI interface:** `doozer/doozerlib/cli/config_plashet.py` (~64KB) -- The `config:plashet` CLI command that orchestrates plashet creation. Handles:
  - Signed/unsigned RPM selection based on signing key configuration.
  - RPM repository layout with `createrepo`.
  - Signing status verification and retry logic.
  - Multi-arch repository composition.
  - Concerns tracking for plashet viability (stored in `plashet.yml`).
  - OpenShift-aware NVR comparison via `compare_nvr_openshift_aware()`.
- **PlashetConfig:** `artcommon/artcommonlib/config/plashet.py` -- Pydantic model for plashet configuration from ocp-build-data's group config.
- **Runtime integration:** `Runtime.get_plashet_config()` in `doozer/doozerlib/runtime.py` loads plashet configuration from `group_config.plashet`.
- **Repos integration:** `doozer/doozerlib/repos.py` uses plashet configuration along with repo definitions to set up the complete RPM repository landscape for builds.

## Lifecycle

1. **Configuration:** Plashet parameters are defined in ocp-build-data's `group.yml` under the `plashet` key and in repo definitions. This includes which Brew tags to pull from, signing key requirements, and RPM inclusion/exclusion rules.
2. **Tag Querying:** `PlashetBuilder.from_tag()` queries Brew for RPM builds in the specified tag:
   - Without assembly: Uses `koji_api.listTagged(tag, latest=True)` to get the tag-latest builds.
   - With assembly: Uses `koji_api.listTagged(tag, latest=False)` to get ALL tagged builds, then `find_latest_builds()` filters for the assembly-appropriate subset.
   - Results are keyed by component name.
3. **Build Selection:** Multiple tags may contribute builds. When the same component appears in multiple tags, the plashet logic resolves conflicts using version comparison, with OpenShift-version-aware sorting when applicable.
4. **Signing Verification:** For production plashets, RPMs must be signed with an approved key (e.g., `fd431d51`). The CLI checks signing status and can wait/retry for unsigned RPMs to become signed.
5. **Repository Creation:** Downloaded RPMs are organized into a directory structure and `createrepo` is run to generate yum/dnf repository metadata.
6. **Consumption:** The resulting plashet repository URL/path is injected into container image builds (via repo files in the distgit) so that `yum/dnf install` commands resolve against the composed repository.

### Key Concepts

- **Brew Tags:** Named labels in Koji that group builds together (e.g., `rhaos-4.17-rhel-9-candidate`). The `inherit` flag controls whether parent tags are also searched.
- **Signing Keys:** RPMs must be signed with Red Hat's GPG key before shipping. Plashets can enforce this requirement.
- **Assembly-aware selection:** When an assembly is specified, plashet uses `find_latest_builds()` which understands assembly naming conventions in the NVR release field.
- **Plashet concerns:** During assembly, potential issues (e.g., unsigned RPMs, version conflicts) are collected in `plashet_concerns` and written to `plashet.yml` for audit.

## Related Concepts

- [brew-koji](brew-koji.md) -- PlashetBuilder queries Brew tags and retrieves builds from Koji to compose repositories.
- [runtime](runtime.md) -- Runtime loads plashet configuration and provides Koji sessions for PlashetBuilder.
- [metadata](metadata.md) -- RPMMetadata objects correspond to the RPM components whose builds appear in plashets.
- [assembly](assembly.md) -- Assembly configuration controls which builds are selected for plashet composition.
- [ocp-build-data](ocp-build-data.md) -- Plashet configuration (tags, signing keys, repo definitions) is defined in ocp-build-data.
