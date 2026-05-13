---
workflow: ReleasePreparation
components: [pyartcd, doozer, elliott]
related_concepts: [Assembly, Runtime, Brew/Koji, Errata/Advisories, Konflux]
---

# Release Preparation Workflow

## Overview

The end-to-end flow for preparing an OCP release, from assembly creation to promotion. The primary implementation is `PrepareReleaseKonfluxPipeline` in `pyartcd/pyartcd/pipelines/prepare_release_konflux.py`, which orchestrates doozer and elliott commands to create advisories, attach builds and bugs, generate payloads, and promote to release channels.

## High-Level Steps

### 1. Assembly Definition

An assembly is defined in `releases.yml` within ocp-build-data. Each assembly specifies:
- Basis event (Brew event ID or Konflux timestamp) that pins the build state
- Assembly type: `standard` (named release like 4.17.1), `candidate`, `preview`, or `custom`
- Release date
- Component-specific overrides (pinned NVRs, excluded components)

### 2. Pipeline Initialization

`PrepareReleaseKonfluxPipeline.run()` performs:
1. `initialize()` -- sets up working directories, clones ocp-build-data and shipment-data repos, validates assembly config, reads `releases.yml` and `group.yml`.
2. `check_advisory_stage_policy()` -- validates advisory stage constraints for the assembly type.
3. `check_blockers()` -- runs `elliott find-bugs:blocker` to check for unresolved blocker bugs.

### 3. Advisory Creation

`prepare_et_advisories()` orchestrates:
- Creates RHBA/RHEA advisories via `elliott create` with appropriate boilerplate from `erratatool.yml`.
- For security releases (RHSA), creates security advisories with CVE metadata.
- Runs `elliott find-bugs sweep` to find VERIFIED bugs and attach them to advisories.
- Runs `elliott find-builds` to find and attach builds (images, RPMs) to advisories.
- Runs `elliott attach-cve-flaws` for security advisories.
- Runs verification commands: `elliott verify-attached-bugs`, `elliott verify-attached-operators`.

### 4. Shipment Preparation

`prepare_shipment()`:
- Creates or updates a shipment configuration in the shipment-data repo.
- Generates Konflux Snapshot and Release resources.
- Creates a merge request (MR) in GitLab for the shipment config.

### 5. Jira Ticket Management

`handle_jira_ticket()`:
- Creates or updates the release Jira ticket.
- Attaches advisory links, shipment MR URL, and build information.

### 6. Build Data PR

`create_update_build_data_pr()`:
- Updates the assembly definition in `releases.yml` with advisory numbers.
- Creates a pull request in ocp-build-data via GitHub.

### 7. Payload Verification

`verify_payload()`:
- Runs `doozer release:gen-payload` to generate release imagestream.
- Verifies payload content matches expected components.

### 8. Promotion (separate pipeline)

`PromotePipeline` in `pyartcd/pyartcd/pipelines/promote.py`:
- Verifies all advisories are in the correct state.
- Signs container images.
- Promotes the payload to release channels (candidate, fast, stable).
- Updates Cincinnati graph data.
- Sends notifications via Slack.

## Sequence Diagram

```
    Assembly Author       pyartcd              elliott              doozer            Brew/Konflux
         |                   |                    |                   |                    |
         |  Define assembly  |                    |                   |                    |
         |  in releases.yml  |                    |                   |                    |
         |------------------>|                    |                   |                    |
         |                   |                    |                   |                    |
         |           initialize()                 |                   |                    |
         |                   |--- clone ocp-build-data, shipment-data                     |
         |                   |                    |                   |                    |
         |           check_blockers()             |                   |                    |
         |                   |--- find-bugs:blocker ----------------->|                    |
         |                   |<-- blocker count --|                   |                    |
         |                   |                    |                   |                    |
         |         prepare_et_advisories()        |                   |                    |
         |                   |--- create -------->|                   |                    |
         |                   |<-- advisory IDs ---|                   |                    |
         |                   |--- find-bugs sweep>|                   |                    |
         |                   |<-- attached bugs --|                   |                    |
         |                   |--- find-builds --->|                   |                    |
         |                   |<-- attached builds-|                   |                    |
         |                   |--- attach-cve-flaws>                  |                    |
         |                   |--- verify-* ------>|                   |                    |
         |                   |                    |                   |                    |
         |         prepare_shipment()             |                   |                    |
         |                   |--- create shipment MR (GitLab) ------->                    |
         |                   |                    |                   |                    |
         |         verify_payload()               |                   |                    |
         |                   |--- release:gen-payload --------------->|                    |
         |                   |<-- imagestream ----|-------------------|                    |
         |                   |                    |                   |                    |
         |   [Promotion - separate pipeline]      |                   |                    |
         |                   |--- sign images --->|-------------------|---> registries     |
         |                   |--- promote ------->|-------------------|---> channels       |
         |                   |                    |                   |                    |
```

## Error Handling

| Step | Failure Mode | Behavior |
|------|-------------|----------|
| Assembly validation | Assembly not found in releases.yml | Pipeline exits with ValueError |
| Blocker check | Blocker bugs found | Warning logged; pipeline continues but warns |
| Advisory creation | Errata Tool API failure | Exception raised; pipeline attempts to save partial progress via `create_update_build_data_pr()` in `finally` block |
| Bug sweep | No bugs found | Warning; advisory may be empty |
| Build attachment | Missing builds | ElliottFatalError; logged and re-raised |
| Payload verification | Payload mismatch | Verification failure raised |
| Bundle/FBC builds | Build errors during prep | Deferred: errors collected, pipeline exits with code 2 (UNSTABLE) after completing other steps |
| Promotion | Signing failure | Retried with tenacity; eventually raises |

## Key Source Files

- `pyartcd/pyartcd/pipelines/prepare_release_konflux.py` -- main pipeline
- `pyartcd/pyartcd/pipelines/promote.py` -- promotion pipeline
- `pyartcd/pyartcd/pipelines/build_sync.py` -- build sync pipeline
- `elliott/elliottlib/cli/create_cli.py` -- advisory creation
- `elliott/elliottlib/cli/find_builds_cli.py` -- build attachment
- `elliott/elliottlib/cli/find_bugs_sweep_cli.py` -- bug sweep
- `doozer/doozerlib/cli/release_gen_payload.py` -- payload generation
