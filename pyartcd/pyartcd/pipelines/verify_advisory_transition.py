"""Move verified release advisories from QE to REL_PREP."""

import json
import logging

import click
from artcommonlib import exectools
from artcommonlib.gitlab import GitLabClient

from pyartcd import constants, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)
VERIFY_RELEASE_SUCCESS_LABEL = "verify-release-success"
ADVISORY_TRANSITION_SUCCESS_LABEL = "advisory-transition-success"


class VerifyAdvisoryTransitionPipeline:
    def __init__(self, runtime: Runtime, version: str, assembly: str):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.group = f"openshift-{version}"
        self.working_dir = runtime.working_dir / "verify_advisory_transition"

    async def run(self):
        # load_group_config resolves assembly inheritance through the standard tools.
        config = await util.load_group_config(
            group=self.group, assembly=self.assembly, doozer_data_path=constants.OCP_BUILD_DATA_URL
        )
        mr_url = config.get("shipment", {}).get("url")
        if not mr_url:
            raise ValueError(f"No shipment MR configured for {self.group}/{self.assembly}")

        gitlab_client = GitLabClient.from_url(mr_url)
        mr = gitlab_client.get_mr_from_url(mr_url)
        labels = set(mr.labels or [])
        if ADVISORY_TRANSITION_SUCCESS_LABEL in labels:
            LOGGER.info("Advisories already processed for %s, skipping", self.assembly)
            return
        if VERIFY_RELEASE_SUCCESS_LABEL not in labels:
            raise ValueError(f"Shipment MR must have {VERIFY_RELEASE_SUCCESS_LABEL}: {mr_url}")

        self.working_dir.mkdir(parents=True, exist_ok=True)
        elliott_base = [
            "elliott",
            f"--group={self.group}",
            f"--assembly={self.assembly}",
            f"--data-path={constants.OCP_BUILD_DATA_URL}",
            f"--working-dir={self.working_dir / 'elliott-working'}",
        ]
        # Metadata accessibility belongs to this flow, not verify-release.
        await exectools.cmd_assert_async(elliott_base + ["verify-metadata-url"])
        # Elliott owns assembly selection (including MicroShift exclusion) and
        # reads the actual states. A failed check is expected while advisories
        # are still in QE; API/configuration errors must never look like QE.
        rc, stdout, stderr = await exectools.cmd_gather_async(
            elliott_base + ["verify-advisory-status", "--output=json"], check=False
        )
        if rc not in (0, 1):
            raise RuntimeError(f"Unable to inspect advisory states (rc={rc}): {stderr}")
        verification = json.loads(stdout)
        advisories = verification.get("advisories", [])
        if (
            verification.get("error")
            or not advisories
            or any(a.get("error") or not a.get("status") or int(a["advisory_id"]) <= 0 for a in advisories)
        ):
            raise ValueError("Unable to determine valid assembly advisory states")
        for advisory in advisories:
            if advisory["status"] != "QE":
                continue
            advisory_id = advisory["advisory_id"]
            LOGGER.info("Moving advisory %s from QE to REL_PREP for %s", advisory_id, self.assembly)
            cmd = elliott_base + ["change-state", "-s", "REL_PREP", "--from", "QE", "--advisory", str(advisory_id)]
            if self.runtime.dry_run:
                cmd.append("--dry-run")
            await exectools.cmd_assert_async(cmd)

        if self.runtime.dry_run:
            # Dry-run transitions leave QE advisories unchanged, so verifying
            # their hypothetical final state would incorrectly fail the preview.
            LOGGER.info("[DRY-RUN] Would verify final advisory states and add %s", ADVISORY_TRANSITION_SUCCESS_LABEL)
            return
        await exectools.cmd_assert_async(elliott_base + ["verify-advisory-status", "--mark"])


@cli.command("verify-advisory-transition", short_help="Move verified release advisories from QE to REL_PREP")
@click.option("--version", required=True, help="OCP version (e.g. 4.22)")
@click.option("--assembly", required=True, help="Assembly name (e.g. 4.22.9)")
@pass_runtime
@click_coroutine
async def verify_advisory_transition_cli(runtime: Runtime, version: str, assembly: str):
    """Verify metadata URLs, move QE advisories (excluding MicroShift), and label the MR.

    Requires verify-release-success on the shipment MR. Only QE advisories are
    transitioned. Elliott verifies the final states and adds advisory-transition-success
    only after all active advisories have progressed past QE. No open-MR
    requirement applies. Dry runs check metadata and use change-state --dry-run
    without updating the MR.
    """
    await VerifyAdvisoryTransitionPipeline(runtime, version, assembly).run()
