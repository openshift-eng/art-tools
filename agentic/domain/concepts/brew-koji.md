---
concept: Brew/Koji
type: ExternalSystem
related:
  - runtime
  - metadata
  - assembly
  - distgit
  - plashet
  - konflux
---

## Definition

Brew is Red Hat's internal instance of the Koji build system, used to build and manage RPM and container image artifacts for OpenShift Container Platform releases. Koji is the upstream open-source build system; Brew adds Red Hat-specific extensions and integrations. art-tools interact with Brew through the `koji` Python client library, using Kerberos (GSSAPI) authentication to submit builds, query build history, manage tags, and retrieve build artifacts.

## Purpose

Brew/Koji serves as the authoritative build and artifact management system for OCP releases. It provides the build infrastructure (OSBS for container images, mock for RPMs), the tagging system that controls which builds are candidates for inclusion in a release, and the historical record of all builds. art-tools rely on Brew for triggering builds, finding latest builds for assemblies, verifying build provenance via tags, and assembling RPM repositories (plashets). As Konflux is introduced as a replacement, both systems coexist, with the `--build-system` flag controlling which is used.

## Location in Code

- **Doozer Brew utilities:** `doozer/doozerlib/brew.py` -- Contains `watch_task()`, `watch_tasks()`, `get_build_objects()`, `list_archives_by_builds()`, `get_builds_tags()`, and build-related utility functions. Manages the `watch_task_info` dict for tracking active build tasks.
- **Elliott Brew utilities:** `elliott/elliottlib/brew.py` -- Contains `get_tagged_builds()`, `get_latest_builds()`, and brew query utilities focused on advisory operations.
- **Shared Brew types:** `artcommon/artcommonlib/brew.py` -- Contains `BuildStates` enum (BUILDING=0, COMPLETE=1, DELETED=2, FAILED=3, CANCELED=4).
- **Runtime Koji sessions:** Both `doozer/doozerlib/runtime.py` and `elliott/elliottlib/runtime.py` provide:
  - `shared_koji_client_session()` -- Returns a context-managed, authenticated `koji.ClientSession`. Uses GSSAPI login.
  - `pooled_koji_client_session()` -- Provides session pooling for concurrent access with thread safety via `koji_lock`.
  - `session_pool` / `session_pool_available` -- Session pool management dicts.
- **Build status detection:** `doozer/doozerlib/build_status_detector.py` -- Uses Koji sessions to detect build freshness and determine if rebuilds are needed.

## Lifecycle

1. **Authentication:** When `shared_koji_client_session()` is first called, it creates a `koji.ClientSession` pointing to Brew's hub URL and authenticates via `session.gssapi_login()`. This requires valid Kerberos credentials. The `--disable-gssapi` flag skips authentication for read-only operations.
2. **Event Constraining:** If an assembly has a basis event, `runtime.brew_event` is set to that event ID. All subsequent Koji queries (e.g., `listTagged`, `listBuilds`) include this event to get a consistent point-in-time view.
3. **Build Queries:** `MetadataBase.get_latest_brew_build()` uses the Koji API to find the latest build:
   - Constructs NVR patterns based on component name, version prefix, and assembly suffix.
   - Queries `koji_api.listBuilds()` with pattern matching, package ID, and optional `completeBefore` timestamp.
   - Respects assembly pinning via `config['is']` which specifies an exact NVR.
   - Verifies builds are tagged with expected candidate tags (e.g., `rhaos-4.17-rhel-9-candidate`).
4. **Build Triggering (Doozer):** `DistGitRepo` uses `rhpkg container-build` (for images via OSBS) or `rhpkg build` (for RPMs) to trigger Brew builds. `watch_task()` monitors the Koji task until completion, cancellation, or timeout (controlled by `BREW_BUILD_TIMEOUT`).
5. **Tagging:** Builds are tagged into candidate tags automatically after successful builds. Build tags are verified during latest-build lookups to ensure consistency.

### Key Concepts

- **NVR (Name-Version-Release):** The unique identifier for a build, e.g., `openshift-clients-4.17.0-202312151200.p0.g1234567.assembly.stream.el9`.
- **Candidate Tags:** Brew tags like `rhaos-4.17-rhel-9-candidate` that mark builds as potential release candidates.
- **Brew Events:** Monotonically increasing integer IDs representing points in time. Used to query "what was the latest build as of event X?" for reproducible assembly composition.
- **Package:** A Koji package corresponds to a component name. Each package has a history of builds.
- **Task:** A Koji task represents an in-progress build operation. Tasks have states (OPEN, CLOSED, CANCELED, FAILED).

## Related Concepts

- [runtime](runtime.md) -- Runtime provides shared Koji client sessions and stores the brew_event constraint.
- [metadata](metadata.md) -- Each Metadata object queries Brew for its latest build via `get_latest_brew_build()`.
- [assembly](assembly.md) -- Assembly basis events constrain Brew queries; assembly `is` config pins specific NVRs.
- [distgit](distgit.md) -- Builds are triggered from distgit repositories via rhpkg, which submits Koji tasks.
- [plashet](plashet.md) -- PlashetBuilder queries Brew tags to assemble RPM repositories.
- [konflux](konflux.md) -- Coexists with Brew as a build system; `--build-system` flag controls which is used.
