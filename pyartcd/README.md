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
`v{{ assembly }}-{{ release_timestamp }}` to generated image/advisory shipments
only when the effective group configuration has `version: auto`. This also
applies to extras shipments from `release-from-fbc`. Elliott resolves
the shipment assembly when constructing stage and production Releases, and
Konflux expands the release timestamp. These tags are combined with RPA tags,
so a `3.18.3` assembly can publish `v3.18.3` even when the image version is
`3.18.0`. RPA validation rejects `{{ labels.version }}` tags in either environment
when the shipment uses assembly-based tags, preventing both versions from being
published. Groups with explicit versions retain their existing behavior.
Non-OpenShift shipments can carry tags without release notes. FBC
shipments retain their catalog release path. See the
[shipment tag configuration](../elliott/README.md#shipment-release-tags).
