---
component: elliott
type: CLI
related: [doozer, pyartcd, artcommon]
---

# Elliott

## Purpose

Elliott is a CLI tool for managing release advisories, errata, and bugs for OCP releases. It interfaces with Red Hat's Errata Tool, Jira, Bugzilla, and Brew to automate the advisory lifecycle -- from creation through bug/build attachment to release verification.

## Location

| What | Path |
|------|------|
| Entry point | `elliott/elliottlib/cli/__main__.py` -- `main()` function |
| CLI group definition | `elliott/elliottlib/cli/common.py` -- Click group, creates `Runtime` |
| Core runtime | `elliott/elliottlib/runtime.py` -- `Runtime(GroupRuntime)` |
| Core logic | `elliott/elliottlib/` -- errata.py, errata_async.py, bzutil.py, brew.py |
| CLI commands | `elliott/elliottlib/cli/` -- one module per command |
| Tests | `elliott/tests/` |
| Package config | `elliott/pyproject.toml` |

## Responsibilities

- **Advisory lifecycle management** -- Create, modify, change state, and drop advisories
- **Bug finding and sweeping** -- Find bugs matching various criteria and attach them to advisories
- **Build finding and attaching** -- Find Brew/Konflux builds and attach them to advisories
- **CVE flaw management** -- Attach CVE flaws to RHSA advisories
- **Advisory verification** -- Verify attached bugs, operators, CVP tests, payload consistency
- **Konflux release management** -- Create and watch Konflux releases
- **Shipment management** -- Manage release shipments

## CLI Command Groups

Commands are registered in `elliott/elliottlib/cli/__main__.py` via `cli.add_command()`.

### Advisory Operations

- `create` (`elliott/elliottlib/cli/create_cli.py`) -- Create a new advisory
- `create-placeholder` (`elliott/elliottlib/cli/create_placeholder_cli.py`) -- Create placeholder advisory
- `create-textonly` (`elliott/elliottlib/cli/create_textonly_cli.py`) -- Create text-only advisory
- `advisory-date` (`elliott/elliottlib/cli/advisory_date_cli.py`) -- Set advisory release date
- `advisory-drop` (`elliott/elliottlib/cli/advisory_drop_cli.py`) -- Drop an advisory
- `advisory-images` (`elliott/elliottlib/cli/advisory_images_cli.py`) -- List images on an advisory
- `get` -- Get advisory details (defined inline in `__main__.py`)
- `change-state` (`elliott/elliottlib/cli/change_state_cli.py`) -- Change advisory state

### Bug Operations

- `find-bugs` (`elliott/elliottlib/cli/find_bugs_cli.py`) -- General bug finder
- `find-bugs:sweep` (`elliott/elliottlib/cli/find_bugs_sweep_cli.py`) -- Sweep bugs into advisories
- `find-bugs:blocker` (`elliott/elliottlib/cli/find_bugs_blocker_cli.py`) -- Find blocker bugs
- `find-bugs:golang` (`elliott/elliottlib/cli/find_bugs_golang_cli.py`) -- Find Golang-related bugs
- `find-bugs:kernel` (`elliott/elliottlib/cli/find_bugs_kernel_cli.py`) -- Find kernel bugs
- `find-bugs:kernel-clones` (`elliott/elliottlib/cli/find_bugs_kernel_clones_cli.py`) -- Find kernel clone bugs
- `find-bugs:qe` (`elliott/elliottlib/cli/find_bugs_qe_cli.py`) -- Find QE bugs
- `find-bugs:second-fix` (`elliott/elliottlib/cli/find_bugs_second_fix_cli.py`) -- Find second-fix bugs
- `attach-bugs` (`elliott/elliottlib/cli/attach_bugs_cli.py`) -- Attach bugs to an advisory
- `remove-bugs` (`elliott/elliottlib/cli/remove_bugs_cli.py`) -- Remove bugs from an advisory
- `repair-bugs` (`elliott/elliottlib/cli/repair_bugs_cli.py`) -- Repair bug states

### Build Operations

- `find-builds` (`elliott/elliottlib/cli/find_builds_cli.py`) -- Find Brew builds for an advisory
- `move-builds` (`elliott/elliottlib/cli/move_builds_cli.py`) -- Move builds between advisories
- `pin-builds` (`elliott/elliottlib/cli/pin_builds_cli.py`) -- Pin builds in assembly definitions
- `remove-builds` (`elliott/elliottlib/cli/remove_builds_cli.py`) -- Remove builds from an advisory
- `tag-builds` (`elliott/elliottlib/cli/tag_builds_cli.py`) -- Tag builds in Brew
- `attach-cve-flaws` (`elliott/elliottlib/cli/attach_cve_flaws_cli.py`) -- Attach CVE flaws to advisories
- `poll-signed` -- Poll for RPM signing status (defined inline in `__main__.py`)

### Verification

- `verify-attached-bugs` (`elliott/elliottlib/cli/verify_attached_bugs_cli.py`) -- Verify bugs attached to advisories
- `verify-attached-operators` (`elliott/elliottlib/cli/verify_attached_operators_cli.py`) -- Verify operator builds
- `verify-cvp` (`elliott/elliottlib/cli/verify_cvp_cli.py`) -- Verify CVP test results
- `verify-payload` (`elliott/elliottlib/cli/verify_payload.py`) -- Verify release payload consistency
- `verify-conforma` (`elliott/elliottlib/cli/conforma_cli.py`) -- Verify Conforma test results
- `validate-rhsa` (`elliott/elliottlib/cli/validate_rhsa.py`) -- Validate RHSA advisory completeness

### Konflux

- `konflux-release` (`elliott/elliottlib/cli/konflux_release_cli.py`) -- Create Konflux releases
- `konflux-release-watch` (`elliott/elliottlib/cli/konflux_release_watch_cli.py`) -- Watch Konflux release progress

### Utilities

- `get-golang-report` (`elliott/elliottlib/cli/get_golang_report_cli.py`) -- Report on Golang versions
- `get-golang-versions` (`elliott/elliottlib/cli/get_golang_versions_cli.py`) -- Get Golang version info
- `get-network-mode` (`elliott/elliottlib/cli/get_network_mode_cli.py`) -- Get network mode for images
- `shipment` (`elliott/elliottlib/cli/shipment_cli.py`) -- Manage release shipments
- `snapshot` (`elliott/elliottlib/cli/snapshot_cli.py`) -- Take advisory/build snapshots
- `tarball-sources` (`elliott/elliottlib/cli/tarball_sources_cli.py`) -- Generate source tarballs
- `rhcos` (`elliott/elliottlib/cli/rhcos_cli.py`) -- RHCOS-related operations
- `find-unconsumed-rpms` (`elliott/elliottlib/cli/find_unconsumed_rpms.py`) -- Find RPMs not consumed by images
- `process-release-from-fbc-bugs` (`elliott/elliottlib/cli/process_release_from_fbc_bugs_cli.py`) -- Process FBC release bugs

## Architecture

### Runtime Initialization

Elliott's `Runtime` extends `GroupRuntime` similarly to doozer. Key differences:
- Default assembly is `stream` (doozer defaults to `test`)
- Supports `--shipment-path` for Konflux release data
- Does not clone distgit repos by default

### Errata Tool Integration

Two interfaces exist:
- `elliott/elliottlib/errata.py` -- Synchronous Errata Tool API wrapper (uses `errata_tool` library)
- `elliott/elliottlib/errata_async.py` -- Async replacement (preferred for new code)

The `Advisory` class in `errata.py` wraps an Errata Tool advisory, providing methods for build attachment, state changes, and queries.

### Bug Tracking

- `elliott/elliottlib/bzutil.py` -- Bugzilla and Jira bug query utilities (uses tenacity retry for reliability)
- Bug sweeping queries are configured in ocp-build-data's `bug.yml`

### Default Advisory Lookup

The `find_default_advisory()` function in `elliott/elliottlib/cli/common.py` reads default advisory IDs from `group_config.advisories` (set in ocp-build-data's group.yml or releases.yml). The `--use-default-advisory` option allows commands to use these defaults instead of explicit advisory IDs.

## Dependencies

- **artcommonlib** -- GroupRuntime, Model, assembly, exectools, build_visibility
- **errata_tool** -- Python library for Errata Tool API
- **Brew/Koji** -- Build system queries (via `elliottlib/brew.py`)
- **Jira/Bugzilla** -- Bug tracking (via `elliottlib/bzutil.py`)
- **Click** -- CLI framework

## Related Components

- **doozer** -- Produces the builds that elliott attaches to advisories
- **pyartcd** -- Orchestrates elliott commands in release pipelines
- **artcommon** -- Provides shared runtime, model, assembly, and utility code
