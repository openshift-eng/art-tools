---
workflow: AdvisoryManagement
components: [elliott]
related_concepts: [Errata/Advisories, Brew/Koji, Assembly]
---

# Advisory Management Workflow

## Overview

How release advisories are created, populated with bugs and builds, verified, and shipped using elliott. Advisories are managed through the Red Hat Errata Tool and follow a defined state machine from creation to CDN push.

## Steps

### 1. Create Advisory

Elliott provides three creation commands:

#### `elliott create` (`elliott/elliottlib/cli/create_cli.py`)

Creates a new advisory with full boilerplate. Options:
- `--type` (required): `RHBA` (bug fix) or `RHEA` (enhancement). RHSA (security) advisories are handled separately.
- `--art-advisory-key` (required): Key into `erratatool.yml` in ocp-build-data for boilerplate text.
- `--date`: Release date (format: YYYY-Mon-DD).
- `--assigned-to`, `--manager`, `--package-owner`: Email addresses for advisory ownership.
- `--batch-id`: Batch ID for grouping advisories.
- `--with-placeholder`: Creates a placeholder bug and attaches it.
- `--with-liveid / --no-liveid`: Whether to request a Live ID.
- `--yes`: Non-interactive mode.

#### `elliott create-placeholder` (`elliott/elliottlib/cli/create_placeholder_cli.py`)

Creates a placeholder bug for an advisory when no real bugs are ready to attach.

#### `elliott create-textonly` (`elliott/elliottlib/cli/create_textonly_cli.py`)

Creates a text-only advisory (no builds, just text content).

### 2. Find and Attach Bugs

#### `elliott find-bugs sweep` (`elliott/elliottlib/cli/find_bugs_sweep_cli.py`)

Sweeps qualified bugs into advisories:
- Default policy: sweeps only `VERIFIED` status bugs (per ART policy document).
- `--include-status`: Add additional statuses to sweep.
- `--exclude-status`: Remove statuses from sweep.
- `--add` / `-a`: Attach found bugs to a specific advisory ID.
- `--cve-only`: Only sweep CVE tracker bugs.
- Uses `FindBugsSweep(FindBugsMode)` class which delegates to `BugTracker.search()`.

#### `elliott find-bugs blocker`

Finds blocker bugs that could block a release. Used in pre-release checks.

#### `elliott attach-bugs` (`elliott/elliottlib/cli/attach_bugs_cli.py`)

Manually attach specific bugs to an advisory.

### 3. Find and Attach Builds

#### `elliott find-builds` (`elliott/elliottlib/cli/find_builds_cli.py`)

Finds builds and optionally attaches them to an advisory:
- `--attach` / `-a`: Attach builds to ADVISORY.
- `--build` / `-b`: Add specific NVR or build ID.
- `--builds-file` / `-f`: Read builds from file or STDIN.
- `--use-default-advisory`: Use the default advisory configured in group.yml.
- Supports both Brew builds and Konflux builds (via `KonfluxBuildRecord`).
- Uses `BuildFinder` (`elliott/elliottlib/build_finder.py`) for build discovery.
- Validates builds against assembly constraints (pinned NVRs, excluded components).

### 4. Attach CVE Flaws

#### `elliott attach-cve-flaws` (`elliott/elliottlib/cli/attach_cve_flaws_cli.py`)

For security advisories (RHSA):
- Finds CVE flaw bugs associated with the release.
- Attaches them to the advisory.
- Required for security advisories to pass errata validation.

### 5. Verify

Multiple verification commands ensure advisory correctness before shipping:

#### `elliott verify-attached-bugs` (`elliott/elliottlib/cli/verify_attached_bugs_cli.py`)

Validates that all attached bugs meet release criteria (correct status, target release, etc.).

#### `elliott verify-attached-operators` (`elliott/elliottlib/cli/verify_attached_operators_cli.py`)

Validates operator bundle builds attached to the advisory.

#### `elliott verify-cvp` (`elliott/elliottlib/cli/verify_cvp_cli.py`)

Checks Container Verification Pipeline results for attached builds.

#### `elliott verify-payload` (`elliott/elliottlib/cli/verify_payload.py`)

Validates that the release payload matches expected content.

#### `elliott validate-rhsa` (`elliott/elliottlib/cli/validate_rhsa.py`)

Validates RHSA advisory content (CVE descriptions, CVSS scores, etc.).

### 6. Change State

#### `elliott change-state` (`elliott/elliottlib/cli/change_state_cli.py`)

Moves an advisory through the state machine:
- `--state`: Target state (`NEW_FILES`, `QE`, `REL_PREP`).
- `--from`: Only change state if currently in this state (guard).
- `--advisory` / `-a`: Specific advisory ID.
- `--default-advisories`: Change state of all group default advisories.
- `--noop` / `--dry-run`: Check without changing.

Advisory state machine:
```
NEW_FILES --> QE --> REL_PREP --> PUSH_READY --> IN_PUSH --> SHIPPED_LIVE
```

State transition requirements:
- `NEW_FILES -> QE`: Bugs or JIRA issues must be attached.
- `QE -> REL_PREP`: QE testing must be complete.
- `REL_PREP -> PUSH_READY`: All verification checks pass.

### 7. Ship

Advisory is pushed to CDN by the Errata Tool. This is triggered externally (not by elliott directly). The `push_cdn_stage` function in `elliott/elliottlib/errata.py` can push to CDN staging for testing.

## Advisory Types

| Type | Description | Use Case |
|------|-------------|----------|
| RHBA | Bug Fix Advisory | Standard OCP releases with bug fixes |
| RHEA | Enhancement Advisory | OCP releases with new features |
| RHSA | Security Advisory | Releases containing CVE fixes |

## State Diagram

```
    +------------+     +---------+     +----------+     +------------+     +---------+     +-------------+
    | NEW_FILES  |---->|   QE    |---->| REL_PREP |---->| PUSH_READY |---->| IN_PUSH |---->| SHIPPED_LIVE|
    +------------+     +---------+     +----------+     +------------+     +---------+     +-------------+
         |                  |               |
         v                  v               v
      (attach           (QE tests      (final
       bugs &            complete)      verification)
       builds)
```

## Integration with Assemblies

When using `--assembly`, elliott reads the assembly definition from `releases.yml`:
- Default advisories are stored in the assembly's group config under `advisories`.
- `--use-default-advisory` resolves to the advisory ID from group config.
- Assembly basis events constrain Brew queries for build discovery.
- Assembly-specific bug inclusion/exclusion rules apply during sweep.

## Key Source Files

- `elliott/elliottlib/cli/create_cli.py` -- advisory creation
- `elliott/elliottlib/cli/find_builds_cli.py` -- build discovery and attachment
- `elliott/elliottlib/cli/find_bugs_sweep_cli.py` -- bug sweep
- `elliott/elliottlib/cli/attach_bugs_cli.py` -- manual bug attachment
- `elliott/elliottlib/cli/attach_cve_flaws_cli.py` -- CVE flaw attachment
- `elliott/elliottlib/cli/change_state_cli.py` -- state transitions
- `elliott/elliottlib/cli/verify_attached_bugs_cli.py` -- bug verification
- `elliott/elliottlib/cli/verify_attached_operators_cli.py` -- operator verification
- `elliott/elliottlib/cli/verify_cvp_cli.py` -- CVP verification
- `elliott/elliottlib/cli/verify_payload.py` -- payload verification
- `elliott/elliottlib/errata.py` -- Errata Tool API (sync)
- `elliott/elliottlib/errata_async.py` -- Errata Tool API (async)
- `elliott/elliottlib/build_finder.py` -- build discovery logic
- `elliott/elliottlib/bzutil.py` -- Bugzilla/JIRA bug tracking
- `elliott/elliottlib/runtime.py` -- Elliott Runtime
