# Pyartcd

Layered product builds support `version: auto` in `group.yml`. This derives the
image build version from the group's `MAJOR.MINOR` suffix and fixes the patch
component at zero. For example, both `acm-5.1` and `mce-5.1` resolve to `5.1.0`,
including builds for z-stream assemblies. The group name must end in
`MAJOR.MINOR`; otherwise version resolution fails.

An explicit `build-layered-products --version` overrides the group setting;
`--version=auto` also derives `MAJOR.MINOR.0`. Existing explicit versions and
behavior when the group setting is omitted are unchanged. The layered product
scan resolves `auto` before triggering bundle and FBC builds.

CSV timestamping still uses the operator's `art.yaml` substitutions. With
`version: auto`, `{FULL_VER}` expands to `MAJOR.MINOR.0-<build timestamp>`.

`prepare-release-lp` and `release-from-fbc` add `v{{ assembly }}` and
`v{{ assembly }}-{{ incrementer }}` to layered-product image/advisory
shipments regardless of the group version. This also applies to extras shipments
from `release-from-fbc`. Elliott resolves the shipment assembly when constructing
stage and production Releases, and Konflux expands the incrementer. These tags
are combined with RPA tags, so a `3.18.3` assembly can publish `v3.18.3` and
`v3.18.3-<incrementer>` even when the image version is `3.18.0`. Release validation
rejects `{{ labels.version }}` tags in shipment defaults and either environment's
ReleasePlan or RPA when assembly-based tags are used. Non-OpenShift shipments can
carry tags without release notes. FBC and OCP optional shipments do not receive
these tags. FBC shipments retain their catalog release path. See the
[shipment tag configuration](../elliott/README.md#shipment-release-tags).

`release-from-fbc` uses `DOOZER_DATA_PATH` and `DOOZER_DATA_GITREF` consistently
for product resolution and FBC refresh. A local path combined with an explicit ref
is rejected because Doozer reads local working-tree contents without checking
out that ref. For a local checkout, select the revision yourself and unset
`DOOZER_DATA_GITREF`; alternatively, use a remote Git URL with the requested ref.

## Preparing inputs for unified layered-product catalogs

Run `artcd prepare-release-lp -g quay-3.18 --assembly=3.18.5 --create-mr
--unified-fbc` to prepare an image shipment containing operands and bundles while
recording the generated FBCs for a separate unified catalog build. Without the
flag, individual FBC shipments are still generated as before.

After all FBC builds and provenance checks succeed, the command writes each
catalog to `releases.<assembly>.assembly.group.fbc_builds` on the group's
build-data branch:

```yaml
fbc_builds:
  "4.22":
    quay-operator:
      nvr: quay-operator-fbc-3.18.5-<release>.ocp4.22
      pullspec: quay.io/example/art-fbc@sha256:<64-character-digest>
      operand_nvrs:
      - quay-operator-container-3.18.5-<release>
      # All effective assembly operand pins, including inherited pins.
```

FBCs remain separate from `members.images`. Records are replaced as a complete
set when preparing or refreshing this assembly; unrelated assembly fields are
preserved. Concurrent operand or FBC-record changes cause preparation to stop
rather than overwrite another release. Identical records require no new commit.
Dry runs do not write records. Configure
`build_config.ocp_build_data_repo_push_url` for the write destination, including
when testing against a build-data fork. Reusing an existing shipment MR removes
its obsolete individual FBC shipment files through normal reconciliation.
