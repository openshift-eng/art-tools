# Build a unified layered-product FBC

`artcd lp-unified-fbc` combines explicit FBC builds for one operator and one
OCP target. Source groups are inferred from the explicit build records; no group
parameter or new ocp-build-data groups are required.

```sh
artcd --working-dir=/tmp/artcd lp-unified-fbc \
  --ocp-target-version=5.0 \
  --fbc-build=quay-operator-fbc-3.18.2-<release>.ocp5.0 \
  --fbc-build=quay-operator-fbc-3.19.1-<release>.ocp5.0 \
  --output-file=/tmp/artcd/unified-fbc.json
```

Repeat `--fbc-build` for every minor version of the operator to include. Each value can
be an FBC NVR or image pullspec. Pullspecs must match the successful build record.
All source groups must belong to the same product and support the target in their
existing `OCP_TARGET_VERSIONS`. ARTCD selects the first source group in sorted
order for internal Doozer configuration. Optional `--product` restricts inputs to
a specific product (the Quay task supplies `--product=quay`). Build data is read
from `--data-path`, which defaults to the production repository; every inspected
source revision is saved.

The job reads bundle metadata to identify each input's package and updated
channels. It preserves the current production history for that package,
applies each selected channel, and emits shared objects once. Other channels
carried by the input's historical snapshot are ignored. Two inputs owning the
same channel, conflicting published objects, removed production entries, and
ambiguous default channels cause the build to fail. The production index must
exist. This command does not create a complete replacement operator index;
its fragment contains only the selected operator package.

All inputs must contain the same operator package. The selected bundle CSV must
exist as an `olm.bundle` for that package and appear in every declared channel.

The output application/component is derived from the operator package and target,
for example `container-security-operator-fbc-unified-4-22`. Its source branch is
`art-container-security-operator-fbc-unified-4-22`. Successful builds receive a
unique `container-security-operator-fbc-unified-4.22-<timestamp>.ocp4.22` NVR.
Database records and ART name labels use `container-security-operator-fbc-unified`;
records retain the existing execution group, `stream` assembly, and all source bundle NVRs.
The output image defaults to `quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc`,
matching existing FBC builds. The Konflux execution namespace remains product-specific.

The JSON result includes `nvr`, digest-pinned `pullspec`, `build_url`,
`provenance_url`, `dry_run`, and the complete `provenance`. The source repository
stores `unified-provenance.json`, including the operator package, input digests, source groups and their
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
Inspect the rendered catalogs to confirm both selected minor-version updates and
all existing production channels survive. No release is triggered by this job.
