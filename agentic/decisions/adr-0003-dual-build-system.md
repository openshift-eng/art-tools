---
id: ADR-0003
title: Dual Build System Support (Brew and Konflux)
date: 2026-04-16
status: accepted
deciders: [art-team]
supersedes: null
superseded-by: null
---

# ADR-0003: Dual Build System Support (Brew and Konflux)

## Context

Historically, all OCP builds used Brew (Red Hat's Koji instance) via OSBS. The team is migrating to Konflux, a Tekton-based build system running on Kubernetes. Migration cannot be done atomically -- both systems must coexist during the transition period, which spans multiple OCP releases.

## Decision

Support both build systems simultaneously. The `--build-system` flag (passed to doozer, elliott, and pyartcd commands) controls which system is used. Build records from both systems are tracked in a unified database. `GroupRuntime.initialize()` accepts a `build_system` parameter.

### Visibility Suffixes

Build visibility is encoded in the release field suffix, defined in `artcommon/artcommonlib/build_visibility.py`:

| Build System | Public  | Private (Embargoed) |
|-------------|---------|---------------------|
| Brew        | `p0`    | `p1`                |
| Konflux     | `p2`    | `p3`                |

The `BuildVisibility` enum has two values: `PUBLIC` and `PRIVATE`. Functions:
- `get_visibility_suffix(build_system, visibility)` -- returns the p-flag string.
- `is_release_embargoed(release, build_system)` -- checks if a release string indicates an embargoed build.
- `isolate_pflag_in_release(release)` -- extracts the p-flag from a release string.
- `get_build_system(visibility_suffix)` -- determines build system from a p-flag.

### Build Records

`artcommon/artcommonlib/konflux/konflux_build_record.py` defines `KonfluxRecord` and `KonfluxBuildRecord`, which provide a unified interface for tracking builds regardless of build system. Key fields: `name`, `group`, `version`, `release`, `assembly`, `engine` (Engine.KONFLUX or Engine.BREW), `outcome` (KonfluxBuildOutcome), `image_pullspec`.

Records are stored in BigQuery via `KonfluxDb` (`artcommon/artcommonlib/konflux/konflux_db.py`).

## Rationale

- **Gradual migration reduces risk**: Components can be opted into Konflux incrementally without disrupting the entire release pipeline.
- **Teams can migrate independently**: Image owners can move their builds to Konflux when ready.
- **Build records provide unified interface**: `KonfluxBuildRecord` abstracts away the build system, so downstream consumers (elliott, pyartcd) work with both.

## Consequences

### Positive

- Low-risk incremental migration: components move to Konflux one at a time.
- Components can be migrated independently per OCP version.
- Unified build record model for querying builds across both systems.

### Negative

- Code duplication between Brew and Konflux paths (e.g. `ocp4_scan.py` vs `ocp4_scan_konflux.py`, `prepare_release.py` vs `prepare_release_konflux.py`).
- Increased complexity: two auth mechanisms (Kerberos for Brew, kubeconfig for Konflux), two build trigger flows.
- Assembly basis events differ: Brew uses integer event IDs, Konflux uses timestamps. Runtime converts between them.

### Neutral

- Visibility suffixes encode build system (p0/p1 = Brew, p2/p3 = Konflux), making it possible to determine build provenance from the release string alone.

## Implementation

### Doozer Backend

`doozer/doozerlib/backend/` contains Konflux-specific modules:

- `konflux_client.py` -- Kubernetes API client for Konflux. Creates PipelineRuns, manages transient git-auth secrets (prefixed `art-transient-pipeline-auth-`), watches pipeline status.
- `konflux_image_builder.py` -- Builds container images via Konflux PipelineRuns. Manages build record creation, NVR comparison, SLSA attestation verification.
- `konflux_fbc.py` -- File-Based Catalog (FBC) builds via Konflux.
- `konflux_olm_bundler.py` -- OLM bundle builds via Konflux.
- `rebaser.py` -- `KonfluxRebaser` class handles rebasing images for Konflux builds (Dockerfile modifications, lockfile generation, build repo management).
- `build_repo.py` -- Manages the build source repository for Konflux.

### Artcommon Konflux

`artcommon/artcommonlib/konflux/`:

- `konflux_build_record.py` -- Data model for build records (`KonfluxRecord`, `KonfluxBuildRecord`, `KonfluxBundleBuildRecord`). Enums: `KonfluxBuildOutcome` (failure/success/pending/timeout/cancelled), `ArtifactType` (rpm/image), `Engine` (konflux/brew).
- `konflux_db.py` -- BigQuery-backed database for build records. Provides caching, exponential search windows, query helpers.

### Pipeline Variants

`pyartcd/pyartcd/pipelines/`:

- `ocp4_scan.py` (Brew) / `ocp4_scan_konflux.py` (Konflux) -- build scanning pipelines.
- `prepare_release_konflux.py` -- release preparation for Konflux-built assemblies. Uses `--build-system=konflux` with doozer/elliott commands.
- `olm_bundle.py` (Brew) / `olm_bundle_konflux.py` (Konflux) -- OLM bundle pipelines.
