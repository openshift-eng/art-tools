---
workflow: ImageBuildLifecycle
components: [doozer, artcommon]
related_concepts: [Distgit, Brew/Koji, Konflux, Metadata, Runtime]
---

# Image Build Lifecycle Workflow

## Overview

How an OCP container image goes from source change to tagged build. The lifecycle differs depending on the build system (Brew/OSBS or Konflux), but both follow the same logical steps: source resolution, rebase, push, build, tag, inclusion.

## Steps

### 1. Source Resolution

Identify the upstream source repository and commit for each image.

- `ImageMetadata` (defined in `doozer/doozerlib/image.py`) reads its config from ocp-build-data (e.g. `images/openshift-apiserver.yml`).
- The config specifies the upstream source repo alias and branch.
- `SourceResolver` (`doozer/doozerlib/source_resolver.py`) clones or updates the upstream source to the correct commit.
- For assemblies with a basis event, source commits are pinned to the state at that event.

### 2. Rebase

Update the downstream build repository with new source and Dockerfile modifications.

#### Brew/OSBS Path (distgit)

`DistGitRepo` (`doozer/doozerlib/distgit.py`):
- Clones the distgit repo for the image (e.g. `containers/openshift-apiserver`).
- Copies upstream source into distgit, applying ignore rules (`BASE_IGNORE = [".git", ".oit"]`).
- Modifies the Dockerfile: updates FROM lines, injects labels, applies source modifications from `SourceModifierFactory`.
- Sets the release field with visibility suffix (p0/p1) and assembly info.

#### Konflux Path (build repo)

`KonfluxRebaser` (`doozer/doozerlib/backend/rebaser.py`):
- Manages a build source repository (`BuildRepo` in `doozer/doozerlib/backend/build_repo.py`).
- Performs similar Dockerfile modifications as distgit: updates FROM lines, injects labels, applies modifications.
- Generates lockfiles for RPM dependencies (`ArtifactLockfileGenerator`, `RPMLockfileGenerator`).
- Detects package managers (cachito-enabled builds).
- Sets visibility suffix (p2/p3) for Konflux builds.
- Handles CPE labels for product identification.

### 3. Push

Commit and push the rebased content to the build repository.

#### Brew/OSBS Path

- `DistGitRepo.push()` commits and pushes to the distgit branch (e.g. `rhaos-4.17-rhel-9`).
- Uses `rhpkg` commands for distgit interaction.

#### Konflux Path

- `BuildRepo` pushes to a branch in the Konflux build source repository on GitHub.
- Uses GitHub App token authentication for push access.

### 4. Build

Trigger the actual container build.

#### Brew/OSBS Path

- `OSBS2Builder` (`doozer/doozerlib/osbs2_builder.py`) triggers a build via `rhpkg container-build`.
- Build runs in Brew (Koji) using OSBS.
- Build is tracked by Brew task ID.

#### Konflux Path

- `KonfluxImageBuilder` (`doozer/doozerlib/backend/konflux_image_builder.py`) creates a Tekton PipelineRun via the Kubernetes API.
- `KonfluxClient` (`doozer/doozerlib/backend/konflux_client.py`) manages the PipelineRun lifecycle:
  - Creates transient git-auth secrets (prefixed `art-transient-pipeline-auth-`) for source access.
  - Labels PipelineRuns with `doozer-watch-id` for tracking.
  - `KonfluxWatcher` (`doozer/doozerlib/backend/konflux_watcher.py`) monitors pipeline completion.
- Build records are written to BigQuery via `KonfluxDb`.
- NVR comparison against existing builds determines whether a new build is needed.

### 5. Tag

Built artifacts are tagged for release candidacy.

#### Brew/OSBS Path

- Successful builds are automatically tagged with the candidate tag (e.g. `rhaos-4.17-rhel-9-candidate`).
- For hotfix assemblies (non-stream), builds are also tagged with a hotfix tag (e.g. `rhaos-4.17-rhel-9-hotfix`).

#### Konflux Path

- Build records in BigQuery serve as the equivalent of Brew tags.
- Konflux Snapshots capture the set of built images for a release.

### 6. Inclusion

Built images are included in the release payload.

- `doozer release:gen-payload` generates the release imagestream from candidate builds.
- Payload images must follow naming conventions (prefix `ose-`).
- For Konflux, Snapshots and Release resources are created to track the payload.
- Elliott attaches builds to advisories for errata-based delivery.

## Role of ImageMetadata

`ImageMetadata` (`doozer/doozerlib/image.py`, inherits from `Metadata`) is the central object for each image:

- Loaded from ocp-build-data YAML config during `Runtime.initialize()`.
- Stores: image name, distgit key, component name, parent/child relationships, architecture config.
- `config` attribute: a `Model` object wrapping the raw YAML config.
- `is_payload`: whether this image is destined for the OCP release payload.
- `for_release`: whether this image is released via errata.
- `distgit_repo()`: returns the `DistGitRepo` instance for Brew builds.
- `resolve_parent()`: establishes the parent image dependency chain.
- `get_component_name()`: returns the Brew component name.

Images are stored in `Runtime.image_map` (Dict[str, ImageMetadata]) keyed by distgit key, and ordered by dependency in `Runtime.image_order`.

## Build System Comparison

| Aspect | Brew/OSBS | Konflux |
|--------|-----------|---------|
| Source repo | distgit (pkgs.devel.redhat.com) | GitHub build repo |
| Build trigger | `rhpkg container-build` | Tekton PipelineRun |
| Auth | Kerberos (GSSAPI) | GitHub App + kubeconfig |
| Build tracking | Brew task ID, koji session | BigQuery build records |
| Tagging | Brew candidate tag | BigQuery + Snapshot |
| Visibility | p0 (public), p1 (private) | p2 (public), p3 (private) |
| Rebaser | `DistGitRepo` | `KonfluxRebaser` |

## Key Source Files

- `doozer/doozerlib/image.py` -- `ImageMetadata` class
- `doozer/doozerlib/distgit.py` -- `DistGitRepo` for Brew distgit operations
- `doozer/doozerlib/backend/rebaser.py` -- `KonfluxRebaser` for Konflux rebase
- `doozer/doozerlib/backend/konflux_image_builder.py` -- Konflux build orchestration
- `doozer/doozerlib/backend/konflux_client.py` -- Kubernetes API client for Konflux
- `doozer/doozerlib/backend/build_repo.py` -- Build source repo management
- `doozer/doozerlib/source_resolver.py` -- Upstream source resolution
- `doozer/doozerlib/rpmcfg.py` -- `RPMMetadata` class
- `doozer/doozerlib/osbs2_builder.py` -- Brew/OSBS build triggering
