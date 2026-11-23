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

## Private fixes in layered product stream catalogs

Set `konflux.private_fixes_git_repo` in the LP group's `group.yml`
to the HTTPS URL of a private GitHub repo accessible to the ART GitHub App.
The bundle and FBC jobs verify that the repo is private before writing build
source. Existing Jenkins and Tekton jobs carry exact selections as
`OPERATOR_NVR=BUNDLE_NVR` in the operator NVR list.

The standard catalog uses a bundle built from eligible `p2` images and is
published to `art-fbc`. If the latest operator or an operand contains a
private fix, a separate `p3` bundle and its FBCs are published to
`quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc-priv`. For example,
an operand-only fix uses the same `p2` operator in both bundles and selects
the latest eligible public or private operand for each. FBC NVRs keep the
same format.

Before enabling the flow, grant the Konflux build identities push access to
the private repository and the bundle and FBC jobs pull access to their
inputs. Give approved teams temporary pull access to the catalog and every
image its bundle needs. When the fix clears, use the standard build flow and
revoke temporary access.

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
custom build-data paths or refs: whenever it is set to anything other than
`false`/`no`, that value overrides the group's own setting. Set it back to
`false` to restore each group's existing freeze policy.

The switch is read on every freeze check. A missing file leaves group policies
in effect so this code can be deployed before the config is merged. Other read
errors and invalid/unknown config stop the check instead of permitting builds.
This does not cancel builds already running or add freeze checks to commands
that do not currently use them.

Direct `beta:ocp4-konflux` builds also honor the master switch before
initializing registry credentials or running build operations, for every
assembly. The check is manual-aware: a global `scheduled` value permits manual
runs while blocking all non-manual ones; `weekdays` additionally permits
non-manual runs on weekends; and a global `yes`/`true` blocks everything.
`--ignore-locks` skips concurrency locks and does not override the freeze.

## Payload synchronization

`artcd build-sync` syncs Konflux payloads and defaults to `--build-system=konflux`.
Brew assembly definitions can be generated with `gen-assembly`, but cannot use
`--auto-trigger-build-sync`.
