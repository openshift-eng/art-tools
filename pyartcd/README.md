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
