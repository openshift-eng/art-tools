# Pyartcd

## FBC failure health reporting

FBC builds record bounded Redis evidence instead of sending immediate failure
messages. Existing OCP `images-health` and `layered-products-image-health`
reports include counts and scoped details, beginning with the first failure.
Their existing schedules, configured groups, product variants and OCP freeze
eligibility remain unchanged. FBC evidence does not create Jira tickets.

### Responsibilities and evidence

* Doozer preserves its existing output, exit codes and rebase/build log records.
  It additionally emits `fbc_outcome` terminal records with operator `name` and
  `outcome=succeeded|failed|reused|skipped`. Reused/skipped records include a
  reason (`existing_build` or `no_bundle`). Actual successes include the executed
  OCP target; available exact operator, bundle and FBC NVRs are metadata.
  `fbc_invocation|status=complete|operators=N|target=X|` establishes coverage.
* `fbc_health.py` owns parsing, accounting, retention and URL-free collection.
  Valid partial outcomes survive malformed records. Missing/incomplete evidence
  is tracking-unavailable, not an invented failed build. Conflicting records
  cannot establish recovery.
* `BuildFbcPipeline.run` owns accounting for `artcd build-fbc`.
  `fbc_build.build_fbc_catalogs` owns accounting for its direct Doozer subprocesses,
  used by `prepare-release-lp` and FBC refresh in `release-from-fbc`.
  Neither entrypoint calls the other; each accounts once per actual invocation.
  `prepare-release-konflux` owns its additional direct FBC subprocess accounting
  through the same `fbc_health.account_run` contract, retaining its partial results.
  Direct subprocess logs live in `doozer-working-fbc-<target>` (or
  `doozer-working-fbc-default`), separate from caller Doozer/Elliott logs.
  Only the owned FBC log is cleared before execution. If clearing fails, stale
  evidence is not trusted and tracking cannot change the build result.
* The two existing report pipelines retain FBC data when unrelated image,
  BigQuery or configuration collection fails. They publish available evidence
  and an incomplete-report warning before propagating the collection failure.
  Layered-product FBC evidence is first qualified by the resolved group variant.
  If variant resolution fails, only unknown-scope evidence is retained and
  tracking is unavailable, not healthy. Later failures retain the resolved
  variant's findings plus unknown-scope evidence, without other variants.
  Incomplete group/version notifications omit execution metadata/log links and
  automatic Slack job attachments, including optional extra notifications.
  Rendering helpers default to the existing complete-report link policy; their
  `include_execution_links=False` option retains counts and identifiers without
  execution links. Controlled dashboard/history links may remain.
  OCP groups that fail their freeze eligibility check are not FBC-scanned.

### Redis contract

Each percent-encoded scope is a Redis hash:

```text
count:fbc-failure:konflux:<group>:<variant>:<assembly>:<ocp-target>:operator:<operator>
count:fbc-failure:konflux:<group>:<variant>:<assembly>:<ocp-target>:invocation:latest
count:fbc-failure:konflux:<group>:<variant>:<assembly>:<ocp-target>:tracking:latest
```

Hashes contain `kind`, `last_failure_at`, `build_version`, requested
`operator_nvrs` and available individual artifact NVRs. Failure hashes contain
`failure_count`; tracking-unavailable hashes do not claim a failure count.
FBC accounting does not collect or persist execution URLs. General report
collection never reads legacy URL fields or exact artifacts; it shows only scope,
identity, last build version and counts. This does not erase historical fields
already stored; no migration or live deletion is performed.

An FBC-local Redis transaction increments the count, updates metadata and sets a
**30-day inactivity TTL together**. Only another failure of the same entity
refreshes a failure hash's retention; another unavailable-evidence event refreshes
its scoped tracking hash. Reports, unrelated builds and skips do not refresh
failure TTLs. Retired operators, named assemblies and groups therefore age out without
requiring a global cleanup service. Expiration means **evidence aged out**, not
verified recovery; empty reports say **no tracked failures**, not that FBC is
known healthy. This retention policy does not change report cadence.

Version, exact NVRs, selected operator sets and build options are not identity.
A newer successful build deletes only its matching operator state, across build
versions, while retaining isolation across groups, variants, assemblies and OCP
targets. Rebase-only successes, reused builds and missing-bundle skips never
establish operator recovery. Existing production-index probing and reuse lookup
semantics are not reimplemented by the tracker.

`invocation:latest` represents only a truly failed invocation without an
attributable operator failure. A complete attributable invocation supersedes
that uncertainty, including when it identifies operator failures. Skips/reuse
alone do not clear it. `tracking:latest` records unavailable outcome evidence;
complete valid evidence clears it. A product configuration failure before variant
resolution is labeled `unknown`, not attributed to an invented operator.
Resolving that configuration supersedes its unknown-variant invocation/tracking
state for the same group, assembly and target, without clearing operator state.
An implicit layered-product target remains `unknown` until configuration or
executed-target evidence resolves it; product versions are never OCP targets.
Resolution also supersedes only the matching unknown-target invocation/tracking
scope, not operator state, other assemblies, variants or explicit targets.
Repeated or malformed named outcomes permanently prevent recovery for that
operator within the invocation, while unrelated valid outcomes remain usable.
Conflicting executed targets cannot be attributed to the requested operator
scope. A partial Redis hash is unavailable evidence; only a vanished hash may be
ignored as an expiry/deletion race.
All state is best effort: Redis update/read/reset errors never replace the
producer result; read errors appear as unavailable, not healthy. Dry-run builds
do not mutate Redis or post messages. This unshipped draft schema needs no
migration or compatibility reader.

### Validation

All regression services are mocked. Run focused suites separately by package:

```bash
uv run pytest --verbose --color=yes pyartcd/tests/test_fbc_health.py \
  pyartcd/tests/test_fbc_build.py pyartcd/tests/pipelines/test_build_fbc.py \
  pyartcd/tests/pipelines/test_images_health.py \
  pyartcd/tests/pipelines/test_layered_products_image_health.py \
  pyartcd/tests/pipelines/test_prepare_release_lp.py \
  pyartcd/tests/pipelines/test_prepare_release_konflux.py \
  pyartcd/tests/pipelines/test_release_from_fbc.py
uv run pytest --verbose --color=yes doozer/tests/cli/test_fbc.py
make format-check
make lint
```

Repository CI uses `make venv` and `make test`. Required signing, safety hooks
and genuine applicable attestation requirements still apply; test success does
not substitute for attestation.
