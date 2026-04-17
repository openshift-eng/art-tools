---
concept: Errata/Advisories
type: ExternalSystem
related:
  - runtime
  - metadata
  - brew-koji
  - assembly
  - ocp-build-data
---

## Definition

The Errata Tool is Red Hat's internal system for managing release advisories (errata). An advisory groups one or more erratum together with associated metadata to track the lifecycle of a software update from creation through QE verification to publication on the CDN. Elliott is the art-tools CLI that automates advisory management, providing commands to create advisories, attach Brew builds, attach bugs, verify advisory contents, and manage advisory state transitions.

## Purpose

Errata/advisories exist to formalize the release process for OCP updates. Every shipped OCP release must have properly configured advisories that specify exactly which builds are included, which bugs are fixed, and which security vulnerabilities are addressed. Elliott automates what would otherwise be tedious manual work in the Errata Tool web UI, enabling the ART team to manage hundreds of advisories across multiple OCP versions efficiently. The system ensures that all shipped content goes through proper QE verification and approval workflows.

## Location in Code

- **Synchronous API wrapper:** `elliott/elliottlib/errata.py` -- Contains the `Advisory` class (wrapping `errata_tool.Erratum`) and utility functions for interacting with the Errata Tool REST API. Key components:
  - `Advisory` class -- Provides `ensure_state()`, `attach_builds()`, `set_cdn_repos()`, `remove_builds()`.
  - `get_raw_erratum()` -- Fetches raw erratum data by advisory ID.
  - Various query functions for finding advisories, checking states, and manipulating builds.
- **Async API wrapper:** `elliott/elliottlib/errata_async.py` -- `AsyncErrataAPI` class providing async HTTP operations using `aiohttp` with GSSAPI (Kerberos) authentication. Used for high-concurrency advisory operations.
- **Bug tracking:** `elliott/elliottlib/bzutil.py` -- `BugTracker`, `BugzillaBugTracker`, `JIRABugTracker` classes for finding and attaching bugs to advisories.
- **Elliott CLI commands:** `elliott/elliottlib/cli/` -- Each advisory operation has its own CLI module:
  - `create_cli.py` -- Create new advisories.
  - `find_builds_cli.py` -- Find Brew builds for attachment.
  - `attach_bugs_cli.py` -- Attach bugs to advisories.
  - `find_bugs_cli.py` -- Find bugs for attachment.
  - `attach_cve_flaws_cli.py` -- Attach CVE flaws to security advisories.
  - `change_state_cli.py` -- Transition advisory state.
  - `verify_cvp_cli.py` -- Verify Container Verification Pipeline results.
- **Constants:** `elliott/elliottlib/constants.py` -- Defines `errata_url`, `errata_states`, advisory type constants, and product version mappings.
- **Errata tool config:** Loaded from `erratatool.yml` in ocp-build-data via `Runtime.get_errata_config()`.

## Lifecycle

### Advisory Lifecycle

1. **Creation:** `elliott create` creates a new advisory in the Errata Tool. Advisory types:
   - **RHBA** (Red Hat Bug Advisory) -- Bug fix updates.
   - **RHSA** (Red Hat Security Advisory) -- Security updates. Requires CVE information.
   - **RHEA** (Red Hat Enhancement Advisory) -- New features/enhancements.
2. **Build Attachment:** `elliott find-builds` and `elliott attach-builds` find qualifying Brew builds and attach them to the advisory. Builds must be from the correct candidate tags and match the assembly's constraints.
3. **Bug Attachment:** `elliott find-bugs` discovers JIRA/Bugzilla bugs that should be associated with the advisory. Bugs are linked to the advisory to document what fixes are included.
4. **CDN Configuration:** CDN repositories are configured to specify where the advisory's content will be published.
5. **QE Verification:** The advisory moves to QE state for testing. Automated checks (CVP - Container Verification Pipeline) verify container images.
6. **State Transitions:** Advisories progress through states (NEW_FILES -> QE -> REL_PREP -> PUSH_READY -> IN_PUSH -> SHIPPED_LIVE). `elliott change-state` automates these transitions.
7. **Publication:** Final state transition pushes content to the CDN, making it available to customers.

### Authentication

All Errata Tool API interactions require Kerberos (GSSAPI) authentication:
- Synchronous: Uses `requests_gssapi.HTTPSPNEGOAuth` or `requests_kerberos.HTTPKerberosAuth`.
- Async: Uses `gssapi.SecurityContext` to generate Negotiate tokens manually.

### Advisory Configuration in ocp-build-data

The `erratatool.yml` file in ocp-build-data and the `advisories` section of `group.yml` define:
- Product and product version mappings.
- Default advisory IDs for the group.
- Errata tool URL and API endpoints.
- Per-advisory-type configuration.

## Related Concepts

- [runtime](runtime.md) -- Elliott's Runtime provides `get_errata_config()` and `get_default_advisories()` from ocp-build-data configuration.
- [metadata](metadata.md) -- Metadata objects are used to find builds for advisory attachment by resolving component names and querying Brew.
- [brew-koji](brew-koji.md) -- Builds attached to advisories come from Brew. Advisory operations query Brew to find qualifying builds by tag and NVR pattern.
- [assembly](assembly.md) -- Assembly configuration determines which builds are eligible for a specific release advisory.
- [ocp-build-data](ocp-build-data.md) -- Advisory configuration, product versions, and default advisory IDs are defined in ocp-build-data.
