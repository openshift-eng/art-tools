# Glossary - art-tools

> Canonical definitions for domain terminology. Alphabetical order. Link to detailed docs where available.

## A

### Advisory
**Definition**: An errata advisory (RHSA, RHBA, or RHEA) that ships bug fixes, security patches, or enhancements to customers via the Red Hat CDN.
**Type**: ExternalSystem concept
**Related**: [Errata/Advisories](./concepts/errata-advisories.md), [Elliott](../design-docs/components/elliott.md)

### Assembly
**Definition**: A named release or checkpoint that controls how builds are pinned and validated. Five types: STREAM, STANDARD, CANDIDATE, CUSTOM, PREVIEW.
**Type**: Pattern
**Related**: [Assembly concept](./concepts/assembly.md), [Runtime](./concepts/runtime.md)
**Details**: [./concepts/assembly.md](./concepts/assembly.md)

## B

### Brew
**Definition**: Red Hat's instance of the Koji build system. Used to build RPMs and container images for OCP.
**Type**: ExternalSystem
**Related**: [Brew/Koji](./concepts/brew-koji.md), [Konflux](./concepts/konflux.md)
**Details**: [./concepts/brew-koji.md](./concepts/brew-koji.md)

### Build Sync
**Definition**: The process of synchronizing builds for an assembly, ensuring all required images and RPMs are built and tagged. Implemented in `pyartcd/pipelines/build_sync.py`.
**Type**: Workflow step
**Related**: [Release Preparation](./workflows/release-preparation.md)

## C

### Candidate Tag
**Definition**: A Brew tag (e.g. `rhaos-4.17-rhel-9-candidate`) used to collect builds that are candidates for inclusion in a release.
**Type**: Brew concept
**Related**: [Brew/Koji](./concepts/brew-koji.md)

## D

### Distgit
**Definition**: Distribution git repositories -- internal source repos where doozer pushes rebased sources before triggering builds in Brew.
**Type**: Pattern
**Related**: [Distgit concept](./concepts/distgit.md), [Doozer](../design-docs/components/doozer.md)
**Details**: [./concepts/distgit.md](./concepts/distgit.md)

## E

### Embargo
**Definition**: A restriction on disclosing security fix details before a coordinated release date. Embargoed builds are marked with PRIVATE visibility.
**Type**: Security concept
**Related**: [SECURITY.md](../SECURITY.md), `artcommonlib/build_visibility.py`

### Errata Tool
**Definition**: Red Hat's internal system for managing release advisories. Elliott interacts with it to create, populate, and ship advisories.
**Type**: ExternalSystem
**Related**: [Errata/Advisories](./concepts/errata-advisories.md)

## F

### FBC (File-Based Catalog)
**Definition**: A file-based operator catalog format used for OLM (Operator Lifecycle Manager) operators in OCP. Replaces the deprecated SQLite-based catalog.
**Type**: DataFormat
**Related**: `doozerlib/backend/konflux_fbc.py`, `doozerlib/cli/fbc.py`

## G

### Group
**Definition**: An OCP version target (e.g. `openshift-4.17`) that maps to a branch in ocp-build-data. Specified via `--group` on all CLI commands.
**Type**: Configuration concept
**Related**: [Runtime](./concepts/runtime.md), [ocp-build-data](./concepts/ocp-build-data.md)

## I

### ImageMetadata
**Definition**: A metadata object representing a single container image component, loaded from ocp-build-data YAML by Runtime.
**Type**: Class
**Related**: [Metadata](./concepts/metadata.md), `doozerlib/image.py`

## K

### Konflux
**Definition**: A Tekton-based build system replacing OSBS/Brew for OCP builds. Active migration in progress.
**Type**: ExternalSystem
**Related**: [Konflux concept](./concepts/konflux.md), [ADR-0003](../decisions/adr-0003-dual-build-system.md)
**Details**: [./concepts/konflux.md](./concepts/konflux.md)

## M

### Metadata
**Definition**: The base abstraction for a buildable component (image or RPM). Wraps ocp-build-data YAML config and provides build lifecycle operations.
**Type**: Class
**Related**: [Metadata concept](./concepts/metadata.md), ImageMetadata, RPMMetadata

### Model/Missing
**Definition**: Safe YAML config traversal pattern. `Model` wraps dicts; accessing undefined keys returns the `Missing` singleton (falsy) instead of raising KeyError.
**Type**: Pattern
**Related**: [Model/Missing concept](./concepts/model-missing.md), `artcommonlib/model.py`
**Details**: [./concepts/model-missing.md](./concepts/model-missing.md)

## N

### NVR
**Definition**: Name-Version-Release -- the standard identifier for a Brew/Koji build (e.g. `openshift-clients-4.17.0-202401151205.p0.g1234567.assembly.stream.el9`).
**Type**: Identifier format
**Related**: [Brew/Koji](./concepts/brew-koji.md)

## O

### ocp-build-data
**Definition**: External git repository containing YAML configuration for all OCP build components -- group configs, image metadata, RPM metadata, assembly definitions, and streams.
**Type**: ExternalSystem
**Related**: [ocp-build-data concept](./concepts/ocp-build-data.md), [Runtime](./concepts/runtime.md)
**Details**: [./concepts/ocp-build-data.md](./concepts/ocp-build-data.md)

### OLM Bundle
**Definition**: An operator bundle for the Operator Lifecycle Manager, packaging operator metadata and manifests for distribution.
**Type**: DataFormat
**Related**: `doozerlib/cli/olm_bundle.py`

## P

### Payload
**Definition**: A release payload containing all container images for an OCP release, used for installation and upgrades.
**Type**: Release artifact
**Related**: `doozerlib/cli/release_gen_payload.py`, [Release Preparation](./workflows/release-preparation.md)

### Pipeline
**Definition**: An automated workflow in pyartcd that orchestrates doozer and elliott commands for release operations (builds, scans, promotions).
**Type**: Module
**Related**: [pyartcd](../design-docs/components/pyartcd.md), `pyartcd/pipelines/`

### Plashet
**Definition**: RPM repository composition tool. Assembles RPM repos from Brew tags to provide consistent dependency sets for container image builds.
**Type**: Pattern
**Related**: [Plashet concept](./concepts/plashet.md), `doozerlib/plashet.py`
**Details**: [./concepts/plashet.md](./concepts/plashet.md)

## R

### Rebase
**Definition**: The process of updating a distgit repository with new upstream source, modifying Dockerfiles, and updating dependencies before triggering a build.
**Type**: Workflow step
**Related**: [Distgit](./concepts/distgit.md), [Image Build Lifecycle](./workflows/image-build-lifecycle.md)

### RHCOS
**Definition**: Red Hat CoreOS -- the immutable operating system used by OCP nodes. Managed separately but tracked by art-tools for release coordination.
**Type**: ExternalSystem concept
**Related**: `doozerlib/rhcos.py`, `elliottlib/rhcos.py`

### RPMMetadata
**Definition**: A metadata object representing a single RPM component, loaded from ocp-build-data YAML by Runtime.
**Type**: Class
**Related**: [Metadata](./concepts/metadata.md), `doozerlib/rpmcfg.py`

### Runtime
**Definition**: Central orchestration object for CLI sessions. Initialized with `--group`, clones ocp-build-data, loads config, creates Metadata objects. Holds Koji sessions and working directory.
**Type**: Class
**Related**: [Runtime concept](./concepts/runtime.md), [ADR-0002](../decisions/adr-0002-runtime-pattern.md)
**Details**: [./concepts/runtime.md](./concepts/runtime.md)

## S

### Shipment
**Definition**: A structured representation of a release delivery, modeled in `elliottlib/shipment_model.py` using Pydantic.
**Type**: DataFormat
**Related**: `elliottlib/shipment_model.py`, `elliottlib/shipment_utils.py`

### Source Resolution
**Definition**: The process of determining the upstream source repository and commit for a component, performed by doozer during rebase.
**Type**: Workflow step
**Related**: `doozerlib/source_resolver.py`, [Distgit](./concepts/distgit.md)

### Stream
**Definition**: The default assembly type representing continuous development builds with no pinned constraints.
**Type**: Assembly type
**Related**: [Assembly](./concepts/assembly.md)

---

## See Also

- [Domain concepts](./concepts/) -- Detailed explanations
- [Workflows](./workflows/) -- How concepts interact
- [ARCHITECTURE.md](../../ARCHITECTURE.md) -- System structure
