---
concept: Distgit
type: Pattern
related:
  - runtime
  - metadata
  - brew-koji
  - ocp-build-data
  - konflux
---

## Definition

Distgit (distribution git) is the pattern of using internal Git repositories to store the buildable source for RPM and container image components. Each component has a downstream distgit repository that contains the Dockerfile (for images) or spec file (for RPMs) plus any patches, sources, or build configuration needed to produce a build in Brew. Doozer manages the lifecycle of these repositories: rebasing upstream source into the distgit format, pushing changes, and triggering builds.

## Purpose

Distgit exists as the bridge between upstream open-source projects and Red Hat's internal build system (Brew/OSBS). Upstream source code cannot be built directly in Brew; it must be transformed into a specific format in a distgit repository that Brew understands. Doozer automates this transformation through the rebase-push-build workflow, ensuring that downstream builds stay synchronized with upstream changes while applying Red Hat-specific patches, configurations, and dependency pinning.

## Location in Code

- **Core module:** `doozer/doozerlib/distgit.py` -- One of the largest modules (~3062 lines). Contains:
  - `DistGitRepo` -- Base class for distgit repository management. Handles cloning, pushing, branching, and common operations.
  - `ImageDistGitRepo` -- Image-specific distgit operations. Handles Dockerfile manipulation, source injection, `content_sets.yml` generation, `container.yaml` updates, and OSBS build triggering.
  - `RPMDistGitRepo` -- RPM-specific distgit operations. Handles spec file updates, tarball generation, and RPM build triggering.
- **Helper functions:** `recursive_overwrite()` (rsync-based file copy), `pull_image()` (podman pull with retries), `map_image_name()` (image name remapping).
- **Distgit type registry:** `doozer/doozerlib/metadata.py` defines `DISTGIT_TYPES = {'image': ImageDistGitRepo, 'rpm': RPMDistGitRepo}` mapping meta_type to the correct distgit class.
- **Runtime integration:** `runtime.distgits_dir` is set during `Runtime.initialize()` to `{working_dir}/distgits/`. This is where all distgit repos are cloned.

## Lifecycle

### Clone Phase

1. `DistGitRepo.__init__()` is called with a `Metadata` object and `autoclone=True` (default).
2. `clone()` creates a namespace subdirectory (e.g., `distgits/containers/`) and clones the distgit repo using `rhpkg clone` or direct git commands.
3. The distgit branch is determined from `metadata.branch()` (which may come from config, group config, or the runtime).
4. If the directory already exists and `--upcycle` is set, it does a `git fetch --all && git reset --hard @{upstream}` instead of recloning.

### Rebase Phase (images:rebase command)

1. Source is resolved from the upstream repository via `SourceResolver`.
2. `ImageDistGitRepo._run_modifications()` applies source modifications defined in the metadata config.
3. The Dockerfile is updated with correct FROM lines (parent image references), labels, and environment variables.
4. `content_sets.yml` and `container.yaml` are generated/updated for OSBS.
5. Dependencies (RPMs, other images) are pinned to specific versions.
6. Changes are committed to the local distgit clone.

### Push Phase (images:push command)

1. `DistGitRepo.push()` pushes the rebased changes to the remote distgit repository.
2. Uses `rhpkg` for authenticated push operations.

### Build Phase (images:build command)

1. `ImageDistGitRepo._trigger_build()` uses `rhpkg container-build` to submit an OSBS build task to Brew.
2. `DistGitRepo._watch_tasks()` monitors the Koji task via `brew.watch_task()`.
3. Build logs are captured in `runtime.brew_logs_dir`.
4. Results are recorded in `runtime.record_logger`.

### Key Attributes (DistGitRepo)

- `metadata` -- Reference to the parent Metadata object.
- `config` -- Model wrapping the component's config (same as `metadata.config`).
- `distgit_dir` -- Absolute path to the cloned distgit directory.
- `dg_path` -- `pathlib.Path` version of `distgit_dir`.
- `branch` -- The distgit branch (e.g., `rhaos-4.17-rhel-9`).
- `sha` -- Current commit SHA of the distgit repo.
- `source_sha` -- Short SHA of the upstream source commit used in rebase.
- `source_full_sha` -- Full SHA of the upstream source commit.
- `build_status` -- Boolean indicating if the build succeeded.
- `push_status` -- Boolean indicating if the push succeeded.

### Constants

- `BASE_IGNORE` -- Files always excluded from rebase: `[".git", ".oit"]`.
- Distgit interaction uses `rhpkg` (Red Hat's enhanced version of `fedpkg`).

## Related Concepts

- [runtime](runtime.md) -- Runtime manages `distgits_dir` and provides the working directory structure for distgit clones.
- [metadata](metadata.md) -- Each Metadata object creates a DistGitRepo. The metadata's config drives distgit branch selection, source resolution, and build configuration.
- [brew-koji](brew-koji.md) -- Builds are triggered from distgit via rhpkg, which submits Koji/Brew tasks. Build results are tracked as Koji builds.
- [ocp-build-data](ocp-build-data.md) -- Component YAML files in ocp-build-data define the distgit configuration: branch, namespace, component name, content sets, and build targets.
- [konflux](konflux.md) -- Konflux builds use a different mechanism (build repos and Tekton pipelines) but still reference distgit-like source management through BuildRepo.
