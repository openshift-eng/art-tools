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

## FBC failure health reporting

`artcd build-fbc` records failed FBC outcomes in Redis rather than sending an
immediate failure notification. Existing `images-health` and
`layered-products-image-health` commands include these failures in their summary
and detailed reports. Their existing invocation cadence is unchanged; no new
scheduler is required. Counts are reported starting with the first failure.

The build consumer path is `BuildFbcPipeline.run` → `_rebase_and_build` →
`doozer beta:fbc:rebase-and-build`. Doozer writes `rebase_fbc_konflux` and
`build_fbc_konflux` outcomes to `record.log`. The pipeline removes a previous log
before invoking Doozer so stale outcomes cannot establish recovery, and processes
partial outcomes even when Doozer exits unsuccessfully. Rebase record names have
an added `-fbc` suffix; build record names are operator keys.
`skip_fbc_konflux` records distinguish reused builds and missing-bundle skips from
missing outcome records, so a successful no-op is not counted as a failure and
does not clear an invocation fallback.
Terminal `failed_fbc_konflux` records also identify operators whose tasks fail
before rebase/build records are written, including partial multi-operator runs.

Counter branches have the following shape (components are percent-encoded):

```text
count:fbc-failure:konflux:<group>:<assembly>:<build-version>:<ocp-target>:operator:<operator>
count:fbc-failure:konflux:<group>:<assembly>:<build-version>:<ocp-target>:invocation:<scope-sha256>
```

Existing counter helpers store `:failure` and optional execution URL metadata.
An operator failure increments once per invocation, even if multiple failure
records exist. Only successful builds clear operator counters; successful rebase,
skips, and builds of other operators or targets do not. An invocation fallback
captures errors without attributable failed-operator records, including missing
or malformed logs. Its scope hashes canonicalized image/NVR selection and build
configuration; only a successful build in that same scope clears it.

Reports include all stored assemblies and version targets for their configured
group, with explicit assembly, build-version, and OCP-target labels. Redis read
errors are reported as unavailable, not healthy. Counter updates are best effort
and cannot replace a build exception or fail a successful build. Production-index
skips leave counters untouched. Dry runs do not mutate counters or send external
notifications. Recovery clears counters without a new immediate notification.
FBC failures do not create Jira tickets.

### Validation plan

All regression services are mocked. Run the focused suite first:

```bash
uv run pytest --verbose --color=yes pyartcd/tests/test_fbc_health.py \
  pyartcd/tests/pipelines/test_build_fbc.py \
  pyartcd/tests/pipelines/test_images_health.py \
  pyartcd/tests/pipelines/test_layered_products_image_health.py
uv run pytest --verbose --color=yes doozer/tests/cli/test_fbc.py
make unit-pyartcd
make format-check
make lint
```

Repository CI installs dependencies using `make venv` and runs `make test`.
Run the package-specific test commands separately to avoid collisions between
their Python `tests` packages.
