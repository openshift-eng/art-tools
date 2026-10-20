# Unified Quay FBC build

The `merge-layered-fbc` Pipeline builds a single fragment from explicitly selected
Quay operator FBC builds for one OCP version. It creates no shipment or release.

Deploy after [the Doozer/ARTCD implementation](https://github.com/openshift-eng/art-tools/pull/3774)
is available in `quay.io/redhat-user-workloads/ocp-art-tenant/art-cd:latest`.
Argo discovers the Task and Pipeline from this application directory. Existing
Quay credentials and service-account permissions are reused. The Quay team role
and admission policy allow starting this pipeline.

Use an existing Quay source group as the execution context. All selected builds
must belong to Quay and support the target in their existing build-data
`OCP_TARGET_VERSIONS`. No new groups or build-data changes are required. Supply
one FBC build per operator/minor version; each entry can be an NVR or pullspec.

Example parameters (replace the sample NVRs with actual successful builds):

```yaml
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: merge-layered-fbc-
  namespace: art-quay-tenant
spec:
  pipelineRef:
    name: merge-layered-fbc
  params:
  - name: group
    value: quay-3.18
  - name: ocp-target-version
    value: "5.0"
  - name: fbc-builds
    value:
    - quay-operator-fbc-3.18.2-<release>.ocp5.0
    - quay-operator-fbc-3.19.1-<release>.ocp5.0
  - name: dry-run
    value: "true"
```

`dry-run` defaults to `true`. It reads source catalogs and production history,
then renders and validates local merged sources without remote writes. Set it to
`false` to push the FBC source and build in Konflux.

Optional parameters are `data-path`, `fbc-repo`, `image-repo`, `plr-template`
(full template URL), `skip-checks`, and `skip-tasks` (comma-separated task names).
Inputs are passed as array arguments or environment variables to the fixed task
script; they are not substituted into Python source.

Pipeline results:

- `merged-fbc-nvr`: built NVR, or planned NVR in dry runs.
- `merged-fbc-pullspec`: immutable output image; empty in dry runs.
- `build-url`: Konflux PipelineRun URL; empty in dry runs.
- `provenance-url`: committed source/input/baseline provenance; empty in dry runs.

The complete JSON build result, including inline provenance, is printed in the
task log. Publishing a URL as the Tekton result keeps result sizes bounded when
many operator builds are merged.

Validate locally with `pytest -q pyartcd/tests/test_merge_layered_fbc_manifests.py`.
Before production use, run with real Quay inputs in dry-run mode, then perform a
staging build and inspect the resulting catalog for both minor-version updates
and retained production channels.
