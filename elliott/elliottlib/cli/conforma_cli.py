import asyncio
import json
import os
import sys
import tempfile
from typing import Dict, List, Tuple

import click
import yaml
from artcommonlib import logutil
from artcommonlib.exectools import cmd_assert_async, cmd_gather_async
from artcommonlib.konflux.konflux_build_record import KonfluxRecord
from artcommonlib.util import new_roundtrip_yaml_handler
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.cli.snapshot_cli import CreateSnapshotCli, get_build_records_by_nvrs
from elliottlib.runtime import Runtime

LOGGER = logutil.get_logger(__name__)

yaml = new_roundtrip_yaml_handler()


class ConformaVerifyCli:
    def __init__(
        self,
        runtime: Runtime,
        nvrs: list,
        policy_path: str = None,
        konflux_kubeconfig: str = None,
        pull_secret: str = None,
    ):
        self.runtime = runtime
        if not nvrs:
            raise ValueError("nvrs must be provided")
        self.nvrs = nvrs
        self.policy_path = (
            policy_path
            or "https://gitlab.cee.redhat.com/releng/konflux-release-data/-/blob/main/config/kflux-ocp-p01.7ayg.p1/product/EnterpriseContractPolicy/registry-ocp-art-prod.yaml"
        )
        self.konflux_kubeconfig = konflux_kubeconfig
        self.pull_secret = pull_secret

    async def run(self) -> Dict[str, Dict]:
        """Run conforma verification for all NVRs and return results."""
        LOGGER.info("Verifying %d NVRs with Conforma", len(self.nvrs))

        pipeline = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config={
                'kubeconfig': self.konflux_kubeconfig,
                'namespace': KONFLUX_DEFAULT_NAMESPACE,
                'context': None,
            },
            image_repo_pull_secret=self.pull_secret,
            builds=self.nvrs,
            dry_run=True,
            job_url=None,
        )
        snapshots = await pipeline.run()
        if len(snapshots) != 1:
            raise ValueError(
                "Expected exactly one snapshot, got %d. Do not provide NVRs of multiple kinds (image/bundle/fbc).",
                len(snapshots),
            )
        snapshot_spec = snapshots[0].spec

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            yaml.dump(snapshot_spec.to_dict(), temp_file)

        # Setup verification environment
        with tempfile.TemporaryDirectory() as temp_dir:
            policy_file, cosign_pub_file = await self._setup_verification_files(temp_dir)
            result = await self._verify_snapshot(temp_file_path, policy_file, cosign_pub_file)

        results_file = "results.yaml"
        with open(results_file, 'w') as f:
            yaml.dump(result, f)
        LOGGER.info("Detailed results saved to %s", results_file)

        return result

    async def _fetch_build_records(self) -> List[KonfluxRecord]:
        """Fetch build records from DB for the given NVRs."""
        LOGGER.info("Fetching build records from DB...")
        records_map = await get_build_records_by_nvrs(self.runtime, self.nvrs, strict=True)
        return list(records_map.values())

    async def _setup_verification_files(self, temp_dir: str) -> Tuple[str, str]:
        """Download policy.yaml and extract cosign public key."""
        LOGGER.info("Setting up verification files...")

        policy_file = os.path.join(temp_dir, "policy.yaml")
        cosign_pub_file = os.path.join(temp_dir, "cosign.pub")

        # Download and process EnterpriseContractPolicy
        if self.policy_path.startswith(('http://', 'https://')):
            # Download the EnterpriseContractPolicy YAML
            temp_download = os.path.join(temp_dir, "ec_policy.yaml")

            # Convert GitLab blob URL to raw URL if needed
            download_url = self.policy_path
            if "gitlab.cee.redhat.com" in download_url and "/-/blob/" in download_url:
                download_url = download_url.replace("/-/blob/", "/-/raw/")

            cmd = ["wget", "-q", "-O", temp_download, download_url]
            await cmd_assert_async(cmd)

            # Parse the YAML and extract the spec section
            with open(temp_download, 'r') as f:
                full_policy = yaml.load(f)

            if 'spec' not in full_policy:
                raise RuntimeError("Downloaded EnterpriseContractPolicy does not contain a 'spec' section")

            # Write just the spec content to the policy file
            with open(policy_file, 'w') as f:
                yaml.dump(full_policy['spec'], f)

        else:
            # Local file path - check if it's a full EnterpriseContractPolicy or just the spec
            if not os.path.exists(self.policy_path):
                raise FileNotFoundError(f"Policy file not found: {self.policy_path}")

            with open(self.policy_path, 'r') as f:
                policy_content = yaml.load(f)

            # If it's a full EnterpriseContractPolicy, extract the spec
            if 'kind' in policy_content and policy_content['kind'] == 'EnterpriseContractPolicy':
                if 'spec' not in policy_content:
                    raise RuntimeError("EnterpriseContractPolicy does not contain a 'spec' section")

                with open(policy_file, 'w') as f:
                    yaml.dump(policy_content['spec'], f)
            else:
                # Assume it's already a spec-only file
                policy_file = self.policy_path

        # Extract cosign public key
        cmd = ["oc", "get", "-n", "openshift-pipelines", "secret", "public-key", "-o", "json"]
        if self.konflux_kubeconfig:
            cmd.extend(["--kubeconfig", self.konflux_kubeconfig])
        _, stdout, _ = await cmd_gather_async(cmd)

        secret_data = json.loads(stdout)
        import base64

        cosign_pub_data = base64.b64decode(secret_data['data']['cosign.pub']).decode('utf-8')

        with open(cosign_pub_file, 'w') as f:
            f.write(cosign_pub_data)

        LOGGER.info("Verification files prepared: policy=%s, cosign.pub=%s", policy_file, cosign_pub_file)
        return policy_file, cosign_pub_file

    async def _verify_images(
        self, build_records: List[KonfluxRecord], policy_file: str, cosign_pub_file: str
    ) -> Dict[str, Dict]:
        """Run ec validate for each image pullspec."""
        LOGGER.info("Running ec validation for %d images...", len(build_records))

        tasks = []
        for record in build_records:
            task = self._verify_single_image(record, policy_file, cosign_pub_file)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine results with NVRs
        verification_results = {}
        for record, result in zip(build_records, results):
            nvr = str(record.nvr)
            if isinstance(result, Exception):
                verification_results[nvr] = {
                    "pullspec": record.image_pullspec,
                    "success": False,
                    "error": str(result),
                    "output": None,
                }
            else:
                verification_results[nvr] = result

        return verification_results

    async def _verify_snapshot(self, snapshot_spec_filepath: str, policy_file: str, cosign_pub_file: str) -> Dict:
        """Run ec validate for a given snapshot"""

        cmd = [
            "ec",
            "validate",
            "image",
            "--images",
            snapshot_spec_filepath,
            "--public-key",
            cosign_pub_file,
            "--policy",
            policy_file,
            "--ignore-rekor",
            "--output",
            "yaml",
        ]

        # Run ec validate command
        rc, stdout, stderr = await cmd_gather_async(cmd, check=False)

        # Parse YAML output
        output_data = None
        if stdout:
            try:
                output_data = yaml.load(stdout)
            except yaml.YAMLError as e:
                LOGGER.warning("Failed to parse YAML output: %s", e)
                output_data = {"raw_output": stdout}

        success = rc == 0
        result = {
            "snapshot": snapshot_spec_filepath,
            "success": success,
            "output": output_data,
            "stderr": stderr if stderr else None,
        }

        if success:
            LOGGER.info("✓ Verification passed ")
        else:
            LOGGER.warning("✗ Verification failed")

        return result


@cli.group("conforma", short_help="Commands for Conforma verification")
def conforma_cli():
    pass


@conforma_cli.command("verify", short_help="Verify given builds (NVRs) with Conforma")
@click.argument('nvrs', metavar='<NVR>', nargs=-1, required=False, default=None)
@click.option(
    "--nvrs-file",
    "-f",
    "nvrs_file",
    help="File to read NVRs from, `-` to read from STDIN.",
    type=click.File("rt"),
)
@click.option(
    "--konflux-policy",
    metavar="PATH_OR_URL",
    help="Path to EnterpriseContractPolicy YAML file or URL. Defaults to the official OCP Konflux policy.",
)
@click.option(
    '--konflux-kubeconfig',
    metavar='PATH',
    help='Path to the kubeconfig file to use. Can also be set via KUBECONFIG env var.',
)
@click.option(
    '--pull-secret',
    metavar='PATH',
    help='Path to the pull secret file to use. For example, if the images are in quay.io/org/repo then provide the pull secret to read from that repo.',
)
@click.pass_obj
@click_coroutine
async def verify_conforma_cli(
    runtime: Runtime,
    nvrs_file,
    nvrs,
    konflux_policy,
    konflux_kubeconfig,
    pull_secret,
):
    """
    Verify given builds (NVRs) with Conforma

    \b
    $ elliott -g openshift-4.18 conforma verify --nvrs-file nvrs.txt

    \b
    $ elliott -g openshift-4.18 conforma verify nvr1 nvr2 nvr3

    \b
    $ elliott -g openshift-4.18 conforma verify --konflux-policy /path/to/custom/policy.yaml nvr1 nvr2

    \b
    $ elliott -g openshift-4.18 conforma verify --kubeconfig /path/to/kubeconfig nvr1 nvr2
    """
    if bool(nvrs) and bool(nvrs_file):
        raise click.BadParameter("Use only one of nvrs arguments or --nvrs-file")

    if nvrs_file:
        if nvrs_file == "-":
            nvrs_file = sys.stdin
        nvrs = [line.strip() for line in nvrs_file.readlines()]

    if not nvrs:
        raise click.BadParameter("Must provide NVRs either as arguments or via --nvrs-file")

    if not konflux_kubeconfig:
        konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    if not pull_secret:
        pull_secret = os.environ.get('KONFLUX_PULL_SECRET')

    pipeline = ConformaVerifyCli(
        runtime=runtime,
        nvrs=nvrs,
        policy_path=konflux_policy,
        konflux_kubeconfig=konflux_kubeconfig,
        pull_secret=pull_secret,
    )
    results = await pipeline.run()

    click.echo(f"Verification complete. See results.yaml for details.")

    if not results["success"]:
        sys.exit(1)
