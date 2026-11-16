# Pyartcd

## Direct layered-product release assemblies

Layered-product `release-from-fbc` inputs must use
`repository@sha256:<64 lowercase hex characters>`. Tag inputs, including
`repository:tag@sha256:...`, are rejected before environment checks or repository
setup, including in test mode and dry runs. This is a change for callers that
previously supplied tags: use the catalog's immutable digest instead. Refreshed
FBCs must also be digest-pinned. OCP optional input handling is unchanged.

`release-from-fbc` generates `assembly.yml` in the ARTCD working directory from
the validated product operator and operand NVRs selected for shipment. It uses
canonical Doozer image keys, records source FBC pullspecs by OCP target, and
creates a standard assembly. Generated bundles stay in the image shipment;
external dependencies do not become invented assembly members.

With `--create-mr`, normal creation, reuse, and forced replacement persist the
complete assembly and `group.shipment.mr` together before making the shipment
MR ready. Existing production-history checks still block these writes, including
with `--force`. Concurrent assembly or inherited-pin changes fail preparation.
Without `--create-mr`, shipment and assembly files are local outputs only.
`--test --create-mr` still creates an isolated draft shipment MR and never writes
assembly data or a shipment pointer to ocp-build-data. Dry runs do not publish.
Test mode and dry runs also print the generated assembly YAML in the job logs
so it remains inspectable after the task pod is removed.

Existing `basis.assembly` inheritance follows `gen-assembly-lp`: changed builds
receive child overrides and absent parent components remain inherited. Explicit
exclusions are honored, and the parent is unchanged. Effective inherited pins
are included in the image shipment. Standalone reruns replace the generated pin
list so removed inputs do not remain pinned.
Removing a redundant or obsolete pin preserves unrelated member settings, such
as architecture overrides and explicit exclusions. Pin reconciliation handles
merge-operator forms such as `metadata.is!` as well as ordinary `metadata.is`.

Use `--unified-fbc` to record validated immutable FBC builds in
`group.fbc_builds` and omit individual FBC shipment files. Operators, operands,
and bundles remain in the image shipment. The separate `lp-unified-fbc` job
consumes these records; this option does not itself build a unified catalog.
Omitting the option preserves individual FBC shipments and clears outdated
unified records. The option is LP-only and cannot be used with `--ocp-optional`.
The group's `ignore_patch_version` policy is honored when recording FBCs and
when the unified consumer reads them; major and minor versions must still match.
Tekton exposes `unified-fbc`, defaulting to `false`; deploy matching art-tools
support before selecting it. OCP optional shipments retain their existing path.

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
for product resolution, assembly loading, and FBC refresh. Assembly loading accepts
both SSH and HTTPS GitHub repository URLs. A local path combined with an explicit ref
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
`build_config.ocp_build_data_repo_push_url` for the write destination. Reusing an existing shipment MR removes
its obsolete individual FBC shipment files through normal reconciliation.

For fork runs, point all three repository settings at the same fork:

- `build_config.ocp_build_data_repo_push_url` is the preparation write destination.
- `prepare-release-lp --build-data-repo-url` reads the assembly used during preparation.
- `lp-unified-fbc --data-path` reads the recorded FBCs for the unified build.

For example, configure `/tmp/artcd-fork.toml` with the following build-data setting
alongside the other configuration required for release preparation:

```toml
[build_config]
ocp_build_data_repo_push_url = "https://github.com/ashwindasr/ocp-build-data"
```

Then prepare and consume the assembly from that fork:

```sh
artcd --config=/tmp/artcd-fork.toml prepare-release-lp \
  --group=quay-3.18 --assembly=3.18.5 --create-mr --unified-fbc \
  --build-data-repo-url=https://github.com/ashwindasr/ocp-build-data

artcd lp-unified-fbc --product=quay --assembly=3.18.5 \
  --ocp-target-version=4.22 \
  --data-path=https://github.com/ashwindasr/ocp-build-data
```

Setting only the push URL leaves preparation reading upstream, which can cause
its concurrent-change comparison to fail on pre-existing fork differences.
The unified job also cannot see fork-only records when it reads upstream. If the
read and write repositories intentionally differ, make the same assembly pins
and recorded FBCs available to both readers before using them. Production runs
using matching upstream read/write settings keep their existing behavior.

## Freezing build automation

Set `freeze_automation` in `config/automation.yml` on the canonical
`openshift-eng/ocp-build-data` `main` branch to control automation for every
group that uses Pyartcd's freeze checks. It accepts the same values as a group's
own `freeze_automation` setting:

- `true` (or `yes`) — freeze all automation, **including** manually-triggered
  runs.
- `scheduled` — block **all** non-manual (timer-triggered) runs, on every day of
  the week; manual runs are always permitted.
- `weekdays` — block non-manual runs **on weekdays** but permit them **on
  weekends** (Saturday/Sunday). Non-manual `images-health` runs are also
  permitted on Monday when `JOB_BASE_NAME=images-health`. Manual runs are
  always permitted.
- `false` (or `no`) — not frozen; defer to each group's own
  `freeze_automation` policy.

Both `scheduled` and `weekdays` always permit manual runs. `scheduled` blocks
every non-manual run; `weekdays` permits the weekend runs and Monday health
check described above.

Global and group values are case-insensitive, and surrounding whitespace is
ignored. Invalid values stop the freeze check instead of permitting builds.

This master switch takes precedence over group `freeze_automation` values and
custom build-data paths or refs for scheduled jobs and direct non-test Konflux
builds: whenever it is set to anything other than `false`/`no`, that value
overrides the group's own setting. Direct `test` assembly Konflux builds use
the group policy instead. Set the switch back to `false` to restore each
group's existing freeze policy for all jobs.

The switch is read on every freeze check except direct `test` assembly builds.
A missing file leaves group policies in effect so this code can be deployed
before the config is merged. Other read errors and invalid/unknown config stop
the check instead of permitting builds.
This does not cancel builds already running or add freeze checks to commands
that do not currently use them.

Direct `beta:ocp4-konflux` builds also honor the master switch before
initializing registry credentials or running build operations, except for
`test` assembly. The check is manual-aware: a global `scheduled` value permits manual
runs while blocking all non-manual ones; `weekdays` additionally permits
non-manual runs on weekends; and a global `yes`/`true` blocks everything.
Test builds use the group's `freeze_automation` policy, allowing a targeted
build while scheduled automation remains frozen. A group set to `yes` still
blocks test builds. `--ignore-locks` skips concurrency locks and does not
override the freeze. `--ignore-automation-freeze` explicitly bypasses both
global and group policies for any assembly.

## Payload synchronization

`artcd build-sync` syncs Konflux payloads and defaults to `--build-system=konflux`.
Brew assembly definitions can be generated with `gen-assembly`, but cannot use
`--auto-trigger-build-sync`.

### Unified FBC shipment generation (ART-26524)

`artcd lp-unified-fbc --product=mta --assembly=8.2.3 --assembly=8.3.3
--ocp-target-version=4.18 --create-mr` builds every selected target and creates
one **draft**, FBC-only shipment MR after all builds and shipment validations
succeed. MR generation is enabled by default; use `--no-create-mr` for build-only runs. Explicit
`--fbc-build` inputs require `--no-create-mr` and remain build-only; shipment generation requires assemblies
and normal rebase. `--dry-run` does not create an MR. This command never starts
stage or production. Review the generated MR before marking it ready for stage.

The snapshot uses the successful build's actual application, target component,
immutable image digest, source repository/commit, and NVR. Its source commit
contains `unified-provenance.json` at the standard path. The shipment has
`metadata.unified: true` and `fbc: true`, a `unified.assemblies` contributor list,
and no single metadata group or assembly. Contributor entries contain `group`,
`assembly`, `revision` (the pinned build-data commit), and `imageShipmentMR`.
There are no shipment generation/fingerprint fields or provenance-path option.
The contributing assemblies' image shipment pointers are never overwritten.

Before MR creation, the generator checks only the configured shipment-data
project for open unified MRs and active pipelines. Generated MRs carry label
`unified-fbc` and branch prefix `unified-fbc-<product-prefix>-`.
Candidate shipment files determine product/OCP overlap, independently of the
MR title. Active deliveries use their pipeline revision. The final check and
MR creation run under one per-product Redis lock. A conflict stops with links;
there is no MR update, supersession, build reuse or resume. Manual/legacy MRs
must use this label/branch convention to participate in discovery.

Before stage and immediately before each production release creation, Elliott
loads the pinned provenance and compares original/current effective image pins
(including inheritance), target FBC selections and supported target/product
coverage. Input changes require reconciliation and regeneration; unrelated
build-data changes do not invalidate the selection. Staging can precede image
production. Production additionally requires successful production watch jobs
for matching contributing image snapshots. Those snapshots must include the
image and bundle digests resolved from the pinned build records and recorded in
the catalog provenance. A digest swap under the same NVR blocks production.
Approval, merge and stage success alone are insufficient.
Unified production is still a manual `prod-pipeline`
action and must be last. Existing prod-pipeline owns failed delivery retries.
After any production attempt, changes to the unified shipment set (other than
release-result/advisory bookkeeping) require a new MR.

Shipment CI supplies `SHIPMENT_MR_URL` to group-less `elliott release new` jobs;
this is required for actual unified release creation. `elliott release watch
--product=mta` selects the tenant without inventing a source execution assembly.
Configured stage/prod plans, admissions, applications, components and package
allowances must exist; unresolved mappings or invalid metadata fail the run.
Catalog freshness work in ART-23826 remains separate; existing builder checks
are preserved. Unified and ordinary FBC shipments both use `validate-lp-prod`
when its global switch is enabled. That switch remains disabled because of the
current resource issue; unified shipments have no permanent exemption.

Deploy the compatible art-tools image and shipment-ci consumer changes before
enabling `create-mr=true` in ARTC. The art-config companion exposes this optional
parameter for MTA, COO and Quay, defaulting to true; set `create-mr=false` for build-only runs. Its Task needs the existing
`gitlab-token` secret in addition to the build credentials. No KRD resources are
provisioned automatically.
