---
component: pyartcd
type: PipelineOrchestrator
related: [doozer, elliott, artcommon]
---

# pyartcd

## Purpose

pyartcd (`artcd` command) is the automated release pipeline orchestrator. It sequences doozer and elliott commands into end-to-end pipelines for building, scanning, and releasing OCP. It integrates with Jenkins, Jira, Slack, and UMB messaging to drive the release process.

## Location

| What | Path |
|------|------|
| Entry point | `pyartcd/pyartcd/__main__.py` -- `main()` function, calls `cli()` |
| CLI group definition | `pyartcd/pyartcd/cli.py` -- Click group with global options, creates `Runtime` |
| Runtime | `pyartcd/pyartcd/runtime.py` -- `Runtime.from_config_file()` |
| Pipeline modules | `pyartcd/pyartcd/pipelines/` -- one module per pipeline |
| Scheduled pipelines | `pyartcd/pyartcd/pipelines/scheduled/` -- recurring scan/sync jobs |
| Support modules | `pyartcd/pyartcd/` -- jenkins.py, jira_client.py, slack.py, locks.py, etc. |
| Tests | `pyartcd/tests/` |
| Package config | `pyartcd/pyproject.toml` |

## Responsibilities

- Orchestrate doozer and elliott via subprocess calls
- Manage release pipelines (build, scan, prepare, promote)
- Integrate with Jenkins for CI/CD triggers
- Create and manage Jira tickets for release tracking
- Send Slack notifications for pipeline status
- Acquire distributed locks (Redis-based) to prevent concurrent conflicting operations
- Schedule recurring scan and sync jobs

## Key Pipeline Categories

### Build Pipelines

- `ocp.py` -- Main OCP image/RPM build pipeline (Brew-based)
- `ocp4_konflux.py` -- OCP build pipeline for Konflux
- `build_sync.py` -- Synchronize built images to mirrors
- `build_sync_multi.py` -- Multi-version build sync
- `build_microshift.py` -- MicroShift build pipeline
- `build_microshift_bootc.py` -- MicroShift bootc build
- `build_rhcos.py` -- RHCOS build pipeline
- `build_plashets.py` -- Generate RPM plashets (repos)
- `build_fbc.py` -- Build File-Based Catalog images
- `build_merged_fbc.py` -- Build merged FBC images
- `build_layered_products.py` -- Build layered product images

### Scan Pipelines

- `ocp4_scan.py` -- Scan OCP images for source changes (Brew)
- `ocp4_scan_konflux.py` -- Scan OCP images for source changes (Konflux)
- `scan_fips.py` -- FIPS compliance scanning
- `scan_for_kernel_bugs.py` -- Kernel bug scanning
- `scan_operator.py` -- Operator scanning
- `scan_plashet_rpms.py` -- Scan plashet RPMs
- `brew_scan_osh.py` -- OpenSCAP scanning via Brew
- `layered_products_scan_konflux.py` -- Scan layered products (Konflux)

### Release Pipelines

- `prepare_release_konflux.py` -- Prepare a release (create advisories, sweep bugs, find builds)
- `promote.py` -- Promote a release (largest pipeline at ~128KB; handles signing, mirroring, Cincinnati graph updates)
- `release_from_fbc.py` -- Release from File-Based Catalog
- `gen_assembly.py` -- Generate assembly definitions from nightlies

### OKD Pipelines

- `okd.py` -- OKD build pipeline
- `okd_images_health.py` -- OKD images health check

### Utility Pipelines

- `update_golang.py` -- Update Golang versions across components
- `rebuild.py` -- Rebuild specific images
- `rebuild_golang_rpms.py` -- Rebuild Golang RPMs
- `cleanup_locks.py` -- Clean up stale distributed locks
- `art_notify.py` -- Send ART notifications
- `advisory_drop.py` -- Drop advisories
- `check_bugs.py` -- Check bug status
- `images_health.py` -- Check image build health
- `olm_bundle.py` -- OLM bundle operations (Brew)
- `olm_bundle_konflux.py` -- OLM bundle operations (Konflux)
- `operator_sdk_sync.py` -- Sync Operator SDK
- `review_cvp.py` -- Review CVP test results
- `tarball_sources.py` -- Generate source tarballs
- `tag_rpms.py` -- Tag RPMs in Brew
- `seed_lockfile.py` -- Seed Konflux lockfiles
- `sigstore_sign.py` -- Sigstore signing
- `quay_doomsday_backup.py` -- Backup Quay repositories
- `sync_rhcos_specialized.py` -- Sync RHCOS specialized images
- `fbc_import_from_index.py` -- Import FBC from index

### Scheduled Pipelines

Located in `pyartcd/pyartcd/pipelines/scheduled/`:

- `schedule_ocp4_scan.py` -- Scheduled OCP source change scan
- `schedule_ocp4_scan_konflux.py` -- Scheduled Konflux source change scan
- `schedule_build_sync_multi.py` -- Scheduled multi-version build sync
- `schedule_layered_products_scan.py` -- Scheduled layered products scan
- `schedule_okd_scan.py` -- Scheduled OKD scan
- `schedule_scan_operator.py` -- Scheduled operator scan
- `schedule_scan_plashet_rpms.py` -- Scheduled plashet RPM scan

## Key Support Modules

- `jenkins.py` -- Jenkins API client (uses tenacity retry)
- `jira_client.py` -- Jira API client for release tracking tickets (uses tenacity retry)
- `slack.py` -- Slack messaging integration
- `locks.py` -- Redis-based distributed locking (uses tenacity retry)
- `git.py` -- Git operations helper (uses tenacity retry)
- `oc.py` -- OpenShift CLI wrapper (uses tenacity retry)
- `signatory.py` -- Build signing operations (uses tenacity retry)
- `constants.py` -- Pipeline constants and configuration

## Architecture

### Pipeline Execution Model

Each pipeline module typically:

1. Defines a Click command registered to the `artcd` CLI group
2. Accepts pipeline-specific parameters (group, assembly, version, etc.)
3. Creates pipeline-specific context (Jira tickets, Slack channels)
4. Shells out to doozer and/or elliott commands via subprocess
5. Handles results, sends notifications, updates tracking tickets

Pipelines use `click_coroutine` (from `pyartcd/pyartcd/cli.py`) to run async pipeline code within Click's synchronous dispatch.

### Runtime

pyartcd's `Runtime` (in `pyartcd/pyartcd/runtime.py`) is distinct from doozer/elliott's GroupRuntime subclasses. It loads configuration from `~/.config/artcd.toml` and provides:
- Working directory management
- Dry-run mode
- Configuration for external service credentials (Jenkins, Jira, Slack tokens)

### Distributed Locking

`pyartcd/pyartcd/locks.py` implements Redis-based distributed locks to prevent concurrent pipeline runs from conflicting (e.g., two promote pipelines running simultaneously for the same version).

## Dependencies

- **doozer** -- Invoked as subprocess for build operations
- **elliott** -- Invoked as subprocess for advisory/bug operations
- **artcommonlib** -- Shared utilities (exectools, assembly, logging)
- **Jenkins** -- CI/CD trigger and status (via jenkins.py)
- **Jira** -- Release tracking tickets (via jira_client.py)
- **Slack** -- Notifications (via slack.py)
- **Redis** -- Distributed locking (via locks.py)
- **Click** -- CLI framework

## Related Components

- **doozer** -- Build tool orchestrated by pyartcd
- **elliott** -- Advisory tool orchestrated by pyartcd
- **artcommon** -- Shared library used by pyartcd directly
