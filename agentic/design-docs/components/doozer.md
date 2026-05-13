---
component: doozer
type: CLI
related: [elliott, pyartcd, artcommon]
---

# Doozer

## Purpose

Doozer is a CLI tool for managing OCP builds -- both RPMs and container images -- via Brew/OSBS and Konflux. It handles the full lifecycle: rebasing source into distgit, building, scanning, and generating release payloads.

## Location

| What | Path |
|------|------|
| Entry point | `doozer/doozerlib/cli/__main__.py` -- `main()` function, calls `cli(obj={})` |
| CLI group definition | `doozer/doozerlib/cli/__init__.py` -- Click group with global options, creates `Runtime` |
| Core runtime | `doozer/doozerlib/runtime.py` -- `Runtime(GroupRuntime)`, the central session object |
| Core logic | `doozer/doozerlib/` -- image.py, distgit.py, brew.py, rpmcfg.py, repos.py |
| CLI commands | `doozer/doozerlib/cli/` -- one module per command group |
| Tests | `doozer/tests/` |
| Package config | `doozer/pyproject.toml` |

## Responsibilities

- **Build management** -- Build RPMs and container images via Brew/Koji (`brew.py`) and Konflux (`backend/konflux_image_builder.py`)
- **Distgit management** -- Clone, rebase, and push changes to distgit repositories (`distgit.py`)
- **Source rebasing** -- Pull upstream source, apply downstream patches, update Dockerfiles with correct base images and RPM versions
- **Image scanning** -- FIPS compliance scanning, OSH (OpenSCAP) scanning, source change detection
- **Release payload generation** -- Generate release payloads and assemblies from built images
- **OLM bundle management** -- Rebase and build Operator Lifecycle Manager bundles
- **FBC (File-Based Catalog)** -- Import and manage file-based operator catalogs

## Architecture

### CLI Command Groups

Commands are registered in `doozer/doozerlib/cli/__main__.py`. The main groups:

**Image operations** (`doozer/doozerlib/cli/images.py`):
- `images:build` -- Build container images via Brew
- `images:rebase` -- Rebase upstream source into distgit
- `images:push` -- Push distgit changes
- `images:clone` -- Clone distgit repos
- `images:list`, `images:print` -- Query image metadata
- `images:foreach` -- Run arbitrary commands on each image
- `images:show-tree`, `images:show-ancestors` -- Visualize image dependency tree

**Konflux image operations** (`doozer/doozerlib/cli/images_konflux.py`):
- `images:konflux:rebase` -- Rebase for Konflux builds
- `images:konflux:build` -- Build via Konflux

**OKD image operations** (`doozer/doozerlib/cli/images_okd.py`):
- `images:okd` -- OKD-specific image operations
- `images:okd:prs` -- Manage OKD pull requests

**Image streams** (`doozer/doozerlib/cli/images_streams.py`):
- `images:streams` -- Manage image streams
- `images:streams:gen-buildconfigs` -- Generate BuildConfig YAML
- `images:streams:mirror` -- Mirror stream images

**RPM operations** (`doozer/doozerlib/cli/rpms.py`):
- `rpms:build` -- Build RPMs
- `rpms:rebase` -- Rebase RPM sources
- `rpms:rebase-and-build` -- Combined rebase + build
- `rpms:clone`, `rpms:clone-sources` -- Clone RPM distgits/sources
- `rpms:print` -- Query RPM metadata

**Config operations**:
- `config:*` (`doozer/doozerlib/cli/config.py`) -- Read/print/commit group config, read assemblies/releases
- `config:plashet` (`doozer/doozerlib/cli/config_plashet.py`) -- Generate plashet (RPM repo) from Brew builds
- `config:tag-rpms` (`doozer/doozerlib/cli/config_tag_rpms.py`) -- Tag RPMs in Brew

**Release operations**:
- `release:gen-payload` (`doozer/doozerlib/cli/release_gen_payload.py`) -- Generate release payload
- `release:gen-assembly` (`doozer/doozerlib/cli/release_gen_assembly.py`) -- Generate assembly definition from nightlies
- `release:calc-upgrade-tests` (`doozer/doozerlib/cli/release_calc_upgrade_tests.py`) -- Calculate upgrade test matrix

**Scan operations**:
- `scan:sources` (`doozer/doozerlib/cli/scan_sources.py`) -- Detect source changes since last build
- `scan:sources:konflux` (`doozer/doozerlib/cli/scan_sources_konflux.py`) -- Konflux variant
- `scan:fips` (`doozer/doozerlib/cli/scan_fips.py`) -- Scan images for FIPS compliance
- `scan:osh` (`doozer/doozerlib/cli/scan_osh.py`) -- OpenSCAP scanning

**Other commands**:
- `detect-embargo` (`doozer/doozerlib/cli/detect_embargo.py`) -- Detect embargoed builds
- `olm-bundle` (`doozer/doozerlib/cli/olm_bundle.py`) -- Manage OLM operator bundles
- `fbc` (`doozer/doozerlib/cli/fbc.py`) -- File-Based Catalog operations (`fbc:rebase-and-build`, `fbc:import`)
- `images:health` (`doozer/doozerlib/cli/images_health.py`) -- Check image build health
- `get-nightlies` (`doozer/doozerlib/cli/get_nightlies.py`) -- Fetch nightly build info

### Runtime Initialization

The `Runtime.initialize()` method in `doozer/doozerlib/runtime.py` performs:

1. Creates working directory structure (distgits, sources, brew-logs, flags)
2. Calls `super().initialize()` (GroupRuntime: logging, Konflux DB)
3. Resolves metadata from ocp-build-data via `resolve_metadata()` which creates `GitData` and `BuildDataLoader`
4. Determines assembly type from `releases.yml`
5. Loads group config (with assembly and variant overrides)
6. Sets assembly basis event (locks Brew queries to a point-in-time)
7. Creates `SourceResolver` for upstream source management
8. Loads image and RPM metadata into `image_map` and `rpm_map`
9. Resolves parent/child image relationships and generates build ordering (`generate_image_tree()`)
10. Optionally clones distgit repos

### Build Ordering

Images are built in dependency order. `Runtime.generate_image_tree()` builds a tree of parent-child relationships and produces `image_order` -- a flat list where parents always precede children. Cyclic dependencies are detected and rejected during initialization.

## Interfaces

**Input:**
- ocp-build-data YAML configs (via `--data-path` or `--group`)
- `--group` (e.g., `openshift-4.17`) -- required for most commands
- `--assembly` -- selects release assembly (default: `test`)
- `--images` / `--rpms` -- filter to specific components
- `--arches` -- target architectures

**Output:**
- Brew/Konflux builds (submitted via koji/Konflux APIs)
- Distgit commits and pushes
- Release payload definitions
- `record.log` -- structured log of operations performed
- Assembly definitions (YAML)

## Dependencies

- **artcommonlib** -- GroupRuntime, Model, assembly, exectools, gitdata, metadata, build_visibility
- **Brew/Koji** -- Build system API (`doozerlib/brew.py`, `KojiWrapper`)
- **Konflux** -- Cloud-native build system (`doozerlib/backend/konflux_image_builder.py`)
- **distgit/rhpkg** -- Red Hat package git repos for RPMs and container builds
- **Click** -- CLI framework

## Related Components

- **elliott** -- Consumes doozer build outputs for advisory management
- **pyartcd** -- Orchestrates doozer commands in automated pipelines
- **artcommon** -- Provides shared runtime, model, assembly, and utility code
