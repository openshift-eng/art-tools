# Build a unified layered-product FBC

`artcd lp-unified-fbc` combines explicit FBC builds for one or more operators and
one OCP target. Source groups are inferred from the explicit build records; no group
parameter or new ocp-build-data groups are required.

```sh
artcd --working-dir=/tmp/artcd lp-unified-fbc \
  --ocp-target-version=5.0 \
  --fbc-build=quay-operator-fbc-3.18.2-<release>.ocp5.0 \
  --fbc-build=quay-operator-fbc-3.19.1-<release>.ocp5.0 \
  --fbc-build=container-security-operator-fbc-3.18.2-<release>.ocp5.0 \
  --fbc-build=quay-bridge-operator-fbc-3.19.1-<release>.ocp5.0 \
  --output-file=/tmp/artcd/unified-fbc.json
```

Repeat `--fbc-build` for every operator and source version to include. Each value can
be an FBC NVR or image pullspec. Pullspecs must match the successful build record.
All source groups must belong to the same product and support the target in their
existing `OCP_TARGET_VERSIONS`. ARTCD selects the first source group in sorted
order for internal Doozer configuration. Optional `--product` restricts inputs to
a specific product (the Quay task supplies `--product=quay`). Build data is read
from `--data-path`, which defaults to the production repository; every inspected
source revision is saved.

All input FBCs must match the specified OCP target. Run the command separately for
each target with that target's source builds. Explicit inputs define the included
operators and versions; operators are not discovered automatically and do not need
to appear in every source group. Every invocation replaces the combined catalog
for that product and target, so include all operators intended for that output.

Build architectures come from `konflux.arches` on the target's `openshift-MAJOR.MINOR`
branch in the same build-data repository. OpenShift 4.12 and 4.13 build only for
`x86_64`. The list is resolved once during preparation and used for build submission
and all database outcomes. Missing or invalid target configuration fails preparation,
including dry runs.

The job reads bundle metadata to identify each input's package and updated
channels. It preserves the current production history for each selected package,
applies each selected channel, and emits shared objects once. Other channels
carried by the input's historical snapshot are ignored. Two inputs owning the
same package/channel, conflicting published objects, removed production entries, and
ambiguous default channels cause the build to fail. The production index must
exist. This command does not create a complete replacement operator index;
its fragment contains only the selected operator packages. Different operators can
use the same channel name; default channels are resolved independently per package.

The generated source stores each package at `catalog/<package>/catalog.yaml`.
The image preserves this layout at `/configs/<package>/catalog.yaml`, as required
by the release pipeline's `validate-fbc` task.

Each input must contain exactly its source bundle's operator package. The selected CSV name is read
from the actual bundle image, rather than the database's `bundle_csv_name`, which
can come from an outdated package YAML. That CSV must exist as an `olm.bundle`
for that package and appear in every declared channel. Its name is saved in provenance.

The output application is derived from the product: `quay-fbc-unified` for Quay.
All OCP targets share this application, with a separate component per target,
such as `quay-fbc-unified-4-15`. This naming also applies to single-operator inputs.
The OCP 4.22 source branch is `art-quay-fbc-unified-4-22`. Successful builds receive a
unique `quay-fbc-unified-4.22-<timestamp>.ocp4.22` NVR.
Database records and ART name labels use `quay-fbc-unified`;
records retain the existing execution group, `stream` assembly, and all source bundle NVRs.
The output image defaults to `quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc`,
matching existing FBC builds. The Konflux execution namespace remains product-specific.
Existing operator-specific applications, components, and branches are left untouched;
new builds use the product-wide resources. Catalog files are regenerated on each run,
including removal of packages omitted from the new selection.
Before staging or production release, update release configuration that references
an operator-specific application to use the product-wide application instead.

The JSON result includes `nvr`, digest-pinned `pullspec`, `build_url`,
`provenance_url`, `dry_run`, and the complete `provenance`. The source repository
stores `unified-provenance.json`, including a sorted `operators` list of package names
(replacing the singular `operator` field), input digests, source groups and their
build-data revisions, channel ownership, and production baseline digest.

For local inspection, use the global `artcd --dry-run` flag. It renders and
validates the catalog and generates local source files without pushing sources,
creating Konflux resources, or writing build records. Registry, build-data, and
GitHub read access plus `opm` are still required. `pullspec`, `build_url`, and
`provenance_url` are null until a real build succeeds.

The Doozer entry point is `beta:fbc:merge-layered`, with `--major-minor` for the
OCP target and the same repeatable `--fbc-build` inputs. Direct Doozer use still
requires an existing source `--group` for runtime configuration; ARTCD infers it
automatically. Use ARTCD for normal invocations: its product/OCP lock serializes
source pushes and builds across
execution groups. Direct Doozer invocations require equivalent serialization.

Use the existing product kubeconfig and registry credentials (`QUAY_AUTH_FILE`),
GitHub App credentials, and build-database credentials. Overrides include
`--fbc-repo`, `--image-repo`, `--plr-template`, `--skip-checks`, and repeatable
`--skip-task`. ARTCD exposes `--kubeconfig`; Doozer exposes
`--konflux-kubeconfig`, `--konflux-context`, and `--konflux-namespace`.

This command builds the unified fragment. Shipment generation and production
release orchestration are separate work. A fragment reflects its recorded
production baseline; independently releasing older fragments later can still
replace newer package content.

## Prepare a combined shipment from group releases

`artcd prepare-release-lp` can prepare source releases and then use the same
unified builder to create their final shipment:

```sh
artcd --working-dir=/tmp/artcd prepare-release-lp \
  --create-unified-fbcs \
  --unified-fbc-groups=quay-3.18,quay-3.17 \
  --create-mr --force
```

This mode currently supports Quay only. Supply comma-separated groups without
assembly assignments. It selects the highest numeric assembly in each group's
`releases.yml` within that group's major/minor release train, using numeric
ordering (for example, `3.18.10` follows `3.18.9`). The selected assembly must
contain resolvable image pins; the pipeline does not fall back to an older release
when the newest assembly is incomplete. `--group`, `--assembly`, and
`--refresh-fbcs` cannot be used in unified mode.

Each group gets an isolated workspace. The pipeline prepares and validates its
pinned images, operator bundles, and ordinary FBCs. It then builds one unified
FBC for each OCP target supported by any selected group, including every prepared
operator FBC from the selected groups that support that target. A group which
does not support a target contributes no input for that target; production history
is preserved by the unified merger. Include all groups intended for the output.

The final shipment MR contains each selected release's image snapshots, including
its bundles, and only the unified FBC snapshots. Ordinary per-version FBCs are
build inputs and are omitted from the shipment. Unified snapshots use application
`quay-fbc-unified` and ReleasePlans `quay-fbc-stage-unified` and
`quay-fbc-prod-unified`, configured in the product catalog. The existing Konflux
ReleasePlans and admissions must reference that product-wide application before
staging. Older configuration pointing to `quay-operator-fbc-unified` needs updating.

Use `--force` for the first consolidation when the selected assemblies already
point to shipment MRs. Every selected assembly's existing `shipment.mr` is updated
to the same combined MR URL. Source MRs are closed only after all pointers are
successfully updated. A partial pointer update leaves the new MR Draft and reports
the groups needing attention; no source MRs are closed. Resolve the failure and
rerun with `--force`. Merged MRs, production attempts, and MRs containing releases
outside the selected set cannot be replaced automatically.

Subsequent runs without `--force` reuse the shared MR when its complete release
scope matches. Unchanged shipment files retain their stage results; changed files
need staging again. Ordinary mode refuses to modify a unified shipment MR. All
selected group/assembly shipment locks are acquired in sorted order alongside
the existing source and unified FBC build locks.

The separate Jenkins job `build/layered-products-prepare-release-unified` exposes
`CREATE_UNIFIED_FBCS` (default `false`) and `UNIFIED_FBC_GROUPS`, as well as the
existing ordinary-mode parameters. In unified mode, leave `GROUP` and `ASSEMBLY`
blank and disable `REFRESH_FBCS`. The original `build/layered-products-prepare-release`
job remains available. Until this implementation is in the ARTCD image, set
`ART_TOOLS_COMMIT=ashwindasr@codex/lp-prepare-unified-release` on the new job.

A global `--dry-run` reports selected assemblies, source build steps, and target
membership in `unified-release-plan.json`. It does not fabricate source FBC NVRs,
merge unbuilt images, or write shipment MRs, release pointers, or build records.
The standalone `lp-unified-fbc` command can still render and validate a merge using
explicit existing FBC inputs during a dry run.

## Validation

Run the new merge/backend tests and existing FBC tests, then the ARTCD tests
in a separate pytest invocation (the projects share the `tests` package name):

```sh
pytest -q doozer/tests/backend/test_konflux_fbc_merge.py \
  doozer/tests/backend/test_konflux_fbc.py doozer/tests/cli/test_fbc.py
pytest -q pyartcd/tests/pipelines/test_merge_layered_fbc.py \
  pyartcd/tests/pipelines/test_prepare_unified_release_lp.py \
  pyartcd/tests/pipelines/test_prepare_release_lp.py pyartcd/tests/test_lp_shipment.py
pytest -q elliott/tests/test_snapshot_cli.py
```

For group inference, cover NVR and pullspec inputs from multiple groups, input
order independence, mixed products, unsupported targets, and missing build records.
Before production use, run a dry run with real Quay inputs, then a staging build.
Run the release pipeline's `validate-fbc` task against the staging image to check
the package directory layout. Supply three operators across multiple source groups
and inspect the rendered catalogs to confirm that all selected packages, minor-version
updates, and existing production channels survive. No release is
triggered by this job.
