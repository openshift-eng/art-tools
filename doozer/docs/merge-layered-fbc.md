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

### Build an existing source branch

Pass `--skip-rebase` to ARTCD or Doozer to build the current remote unified FBC
branch, for example `art-quay-fbc-unified-4-21` for Quay targeting OCP 4.21.
This supports rebuilding after manual catalog fixes committed to that branch.
Without the flag, sources are regenerated as before.

The flag skips input catalog rendering, production baseline merging, source
generation, commits and pushes. It uses a fresh checkout and builds that exact
commit, preserving the Dockerfile, catalogs, mirror sets and provenance. The NVR,
release and source bundle NVRs come from the committed Dockerfile and provenance;
rebuilding reuses the existing NVR and image tag. The branch must already contain
consistent unified FBC metadata for the requested product, target and execution
group. Missing branches and mismatched metadata fail before a build starts.

Continue supplying `--fbc-build` inputs so ARTCD can infer and validate the source
group and obtain its product configuration. Use inputs from the same execution
group as the committed branch. The committed branch determines catalog content;
the supplied images are not re-rendered in this mode. The existing product/target
lock, architecture lookup, build retries and result recording still apply.
Combining `--skip-rebase` with `--dry-run` checks the branch and metadata without
starting Konflux builds or writing database records.

This command builds the unified fragment. Shipment generation and production
release orchestration are separate work. A fragment reflects its recorded
production baseline; independently releasing older fragments later can still
replace newer package content.

## Validation

Run the new merge/backend tests and existing FBC tests, then the ARTCD tests
in a separate pytest invocation (the projects share the `tests` package name):

```sh
pytest -q doozer/tests/backend/test_konflux_fbc_merge.py \
  doozer/tests/backend/test_konflux_fbc.py doozer/tests/cli/test_fbc.py
pytest -q pyartcd/tests/pipelines/test_merge_layered_fbc.py
```

For group inference, cover NVR and pullspec inputs from multiple groups, input
order independence, mixed products, unsupported targets, and missing build records.
Before production use, run a dry run with real Quay inputs, then a staging build.
Run the release pipeline's `validate-fbc` task against the staging image to check
the package directory layout. Supply three operators across multiple source groups
and inspect the rendered catalogs to confirm that all selected packages, minor-version
updates, and existing production channels survive. No release is
triggered by this job.
