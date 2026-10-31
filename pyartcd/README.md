# Pyartcd

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
