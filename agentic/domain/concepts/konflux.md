---
concept: Konflux
type: ExternalSystem
related:
  - runtime
  - metadata
  - brew-koji
  - distgit
  - assembly
---

## Definition

Konflux is the next-generation build system for OpenShift, based on Tekton pipelines running on Kubernetes. It is actively replacing the legacy OSBS/Brew build pipeline for container image builds. Konflux provides a cloud-native CI/CD approach where builds are defined as PipelineRun resources, build records are stored in BigQuery, and enterprise contract (EC) verification ensures supply chain security. Both Brew and Konflux coexist during the migration period, with the `--build-system` flag controlling which system art-tools use.

## Purpose

Konflux exists to modernize the OCP build infrastructure. The legacy OSBS/Brew pipeline has scaling and maintenance challenges, and Konflux addresses these by providing Tekton-based pipelines, improved supply chain security via enterprise contracts, better build reproducibility, and a more cloud-native architecture. For art-tools, Konflux introduces a parallel build path that can eventually fully replace Brew for container image builds while maintaining compatibility with existing assembly and metadata patterns.

## Location in Code

### Doozer (Build Orchestration)

- **KonfluxClient:** `doozer/doozerlib/backend/konflux_client.py` (~67KB) -- The main client for interacting with the Konflux Kubernetes API. Handles:
  - PipelineRun creation and management via the Kubernetes dynamic client.
  - Application and Component resource management in Konflux namespaces.
  - Git authentication secret management (transient secrets per invocation).
  - Enterprise Contract (EC) verification pipeline triggering.
  - Label-based PipelineRun tracking for the current doozer invocation.
- **KonfluxImageBuilder:** `doozer/doozerlib/backend/konflux_image_builder.py` -- Orchestrates the image build process through Konflux:
  - Creates BuildRepo instances with rebased source.
  - Submits PipelineRun resources to Konflux.
  - Monitors build progress via KonfluxWatcher.
  - Records build results to KonfluxDb (BigQuery).
  - Handles NVR computation, architecture-specific builds, and build record creation.
- **KonfluxFbcBuilder:** `doozer/doozerlib/backend/konflux_fbc.py` -- Handles file-based catalog (FBC) builds in Konflux for OLM operator bundles and catalogs.
- **BuildRepo:** `doozer/doozerlib/backend/build_repo.py` -- Manages the Git repository that Konflux builds from. Unlike traditional distgit, BuildRepo is a transient repo created per build with the rebased source.
- **KonfluxRebaser:** `doozer/doozerlib/backend/rebaser.py` -- Prepares source for Konflux builds by rebasing into the BuildRepo format.
- **KonfluxWatcher:** `doozer/doozerlib/backend/konflux_watcher.py` -- Watches PipelineRun resources via Kubernetes watch API for completion.
- **PipelineRunInfo:** `doozer/doozerlib/backend/pipelinerun_utils.py` -- Utilities for parsing PipelineRun status and results.

### Artcommon (Shared Infrastructure)

- **KonfluxDb:** `artcommon/artcommonlib/konflux/konflux_db.py` (~60KB) -- BigQuery-backed database for build records. Provides:
  - `get_latest_build()` -- Find latest build by name, group, assembly, outcome.
  - `get_build_record_by_nvr()` -- Look up specific build by NVR.
  - `add_build()` -- Insert a new build record.
  - `BuildCache` -- In-memory cache with per-group indexing and exponential search window expansion.
  - Two-tier caching: small columns (no installed_rpms/installed_packages) and all columns.
- **KonfluxBuildRecord:** `artcommon/artcommonlib/konflux/konflux_build_record.py` -- Data model for Konflux build records. Key fields: name, group, version, release, assembly, el_target, arches, art_job_url, pipeline_url, installed_rpms, etc.
- **Enums:**
  - `KonfluxBuildOutcome` -- `SUCCESS`, `FAILURE`, `PENDING`, `TIMEOUT`, `CANCELLED`.
  - `KonfluxECStatus` -- `PASSED`, `FAILED`, `NOT_APPLICABLE`.
  - `ArtifactType` -- `RPM`, `IMAGE`.
  - `Engine` -- `KONFLUX`, `BREW`.
- **KonfluxRecord (base):** Base class for all Konflux records with common fields and build ID generation.

### Runtime Integration

- `GroupRuntime.initialize_konflux_db()` in `artcommon/artcommonlib/runtime.py` initializes the KonfluxDb connection.
- `runtime.build_system` (`"brew"` or `"konflux"`) controls which build system is queried.
- `runtime.konflux_db` provides access to the BigQuery-backed build record database.
- `MetadataBase.get_latest_konflux_build()` in `artcommon/artcommonlib/metadata.py` queries Konflux DB for the latest build matching assembly, outcome, and el_target constraints.

## Lifecycle

1. **Initialization:** `GroupRuntime.initialize()` creates a `KonfluxDb` instance connected to BigQuery. The `--build-system` flag sets `runtime.build_system` to `"konflux"`.
2. **Source Preparation:** `KonfluxRebaser` prepares a `BuildRepo` with the rebased source. Unlike distgit, this is a transient Git repository created per build.
3. **Build Submission:** `KonfluxImageBuilder` creates a PipelineRun resource in the Konflux Kubernetes namespace via `KonfluxClient`. The PipelineRun references the BuildRepo source and build configuration.
4. **Build Monitoring:** `KonfluxWatcher` monitors the PipelineRun for completion using the Kubernetes watch API. Progress is tracked via labels including a unique `doozer-watch-id` per invocation.
5. **EC Verification:** After build completion, `KonfluxClient` can trigger enterprise contract verification to check supply chain compliance.
6. **Record Storage:** Build results are stored as `KonfluxBuildRecord` entries in BigQuery via `KonfluxDb.add_build()`.
7. **Build Queries:** `MetadataBase.get_latest_konflux_build()` queries BigQuery for builds, using assembly-aware logic parallel to Brew queries: checking pinned builds (`is` config), falling back from assembly-specific to stream to true latest.

### Key Differences from Brew

- **Build infrastructure:** Tekton pipelines on Kubernetes vs. OSBS/mock on Brew workers.
- **Build records:** BigQuery vs. Koji database.
- **Authentication:** Kubernetes service accounts and GitHub App tokens vs. Kerberos.
- **Basis events:** Timestamps (`basis.time`) instead of Brew event IDs (`basis.brew_event`).
- **Source management:** Transient BuildRepo vs. persistent distgit repositories.

## Related Concepts

- [runtime](runtime.md) -- Runtime initializes KonfluxDb and stores the `build_system` flag that controls Brew vs. Konflux usage.
- [metadata](metadata.md) -- MetadataBase provides `get_latest_konflux_build()` for querying Konflux build records.
- [brew-koji](brew-koji.md) -- Coexists with Konflux; both build systems are supported simultaneously during migration.
- [distgit](distgit.md) -- Konflux uses BuildRepo instead of traditional distgit, but the rebase concept is similar.
- [assembly](assembly.md) -- Assembly basis events for Konflux use timestamps (`basis.time`) instead of Brew event IDs.
