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
`v{{ labels.version }}-{{ incrementer }}` to layered-product image/advisory
shipments regardless of the group version. This also applies to extras shipments
from `release-from-fbc`. Elliott resolves the shipment assembly when constructing
stage and production Releases, and Konflux expands the image version and
incrementer. These tags are combined with RPA tags, so a `3.18.3` assembly can
publish `v3.18.3` and `v3.18.0-<incrementer>` when the image version is `3.18.0`.
Release validation allows this fixed version/incrementer tag alongside the
assembly alias, while rejecting other `{{ labels.version }}` tags in shipment
defaults and either environment's ReleasePlan or RPA. Non-OpenShift shipments can
carry tags without release notes. FBC and OCP optional shipments do not receive
these tags. FBC shipments retain their catalog release path. See the
[shipment tag configuration](../elliott/README.md#shipment-release-tags).

`release-from-fbc` uses `DOOZER_DATA_PATH` and `DOOZER_DATA_GITREF` consistently
for product resolution and FBC refresh. A local path combined with an explicit ref
is rejected because Doozer reads local working-tree contents without checking
out that ref. For a local checkout, select the revision yourself and unset
`DOOZER_DATA_GITREF`; alternatively, use a remote Git URL with the requested ref.
