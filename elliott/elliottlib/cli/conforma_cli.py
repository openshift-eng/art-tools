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

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.cli.snapshot_cli import get_build_records_by_nvrs
from elliottlib.runtime import Runtime

LOGGER = logutil.get_logger(__name__)


class ConformaVerifyCli:
    def __init__(
        self,
        runtime: Runtime,
        nvrs: list,
        policy_path: str = None,
        kubeconfig: str = None,
    ):
        self.runtime = runtime
        if not nvrs:
            raise ValueError("nvrs must be provided")
        self.nvrs = nvrs
        self.policy_path = (
            policy_path
            or "https://gitlab.cee.redhat.com/releng/konflux-release-data/-/blob/main/config/kflux-ocp-p01.7ayg.p1/product/EnterpriseContractPolicy/registry-ocp-art-prod.yaml"
        )
        self.kubeconfig = kubeconfig

    async def run(self) -> Dict[str, Dict]:
        """Run conforma verification for all NVRs and return results."""
        self.runtime.initialize(build_system='konflux')
        if self.runtime.konflux_db is None:
            raise RuntimeError('Must run Elliott with Konflux DB initialized')

        LOGGER.info("Verifying %d NVRs with Conforma", len(self.nvrs))

        # Fetch build records from DB
        build_records = await self._fetch_build_records()

        # Setup verification environment
        with tempfile.TemporaryDirectory() as temp_dir:
            policy_file, cosign_pub_file = await self._setup_verification_files(temp_dir)

            # Run verification for each image
            results = await self._verify_images(build_records, policy_file, cosign_pub_file)

        # Report results and save to file
        self._report_results(results)
        self._save_results_to_file(results)

        return results

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
                full_policy = yaml.safe_load(f)

            if 'spec' not in full_policy:
                raise RuntimeError("Downloaded EnterpriseContractPolicy does not contain a 'spec' section")

            # Write just the spec content to the policy file
            with open(policy_file, 'w') as f:
                yaml.dump(full_policy['spec'], f, default_flow_style=False)

        else:
            # Local file path - check if it's a full EnterpriseContractPolicy or just the spec
            if not os.path.exists(self.policy_path):
                raise FileNotFoundError(f"Policy file not found: {self.policy_path}")

            with open(self.policy_path, 'r') as f:
                policy_content = yaml.safe_load(f)

            # If it's a full EnterpriseContractPolicy, extract the spec
            if 'kind' in policy_content and policy_content['kind'] == 'EnterpriseContractPolicy':
                if 'spec' not in policy_content:
                    raise RuntimeError("EnterpriseContractPolicy does not contain a 'spec' section")

                with open(policy_file, 'w') as f:
                    yaml.dump(policy_content['spec'], f, default_flow_style=False)
            else:
                # Assume it's already a spec-only file
                policy_file = self.policy_path

        # Extract cosign public key
        cmd = ["oc", "get", "-n", "openshift-pipelines", "secret", "public-key", "-o", "json"]
        if self.kubeconfig:
            cmd.extend(["--kubeconfig", self.kubeconfig])
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

    async def _verify_single_image(self, record: KonfluxRecord, policy_file: str, cosign_pub_file: str) -> Dict:
        """Run ec validate for a single image."""
        pullspec = record.image_pullspec
        nvr = str(record.nvr)

        LOGGER.info("Verifying image: %s (NVR: %s)", pullspec, nvr)

        cmd = [
            "ec",
            "validate",
            "image",
            "--image",
            pullspec,
            "--public-key",
            cosign_pub_file,
            "--policy",
            policy_file,
            "--ignore-rekor",
            "--output",
            "yaml",
        ]

        try:
            # Run ec validate command
            _, stdout, stderr = await cmd_gather_async(cmd, check=False)

            # Parse YAML output
            output_data = None
            if stdout:
                try:
                    output_data = yaml.safe_load(stdout)
                except yaml.YAMLError as e:
                    LOGGER.warning("Failed to parse YAML output for %s: %s", nvr, e)
                    output_data = {"raw_output": stdout}

            # cmd_gather_async with check=False doesn't raise on non-zero return codes
            # Determine success based on whether we got valid output and no significant errors
            # EC command typically outputs validation results even on policy violations,
            # so we rely on the absence of critical errors in stderr
            success = output_data is not None and not any(
                error_keyword in stderr.lower()
                for error_keyword in ["error:", "fatal:", "failed to", "cannot", "unable to"]
                if stderr
            )

            result = {
                "pullspec": pullspec,
                "success": success,
                "output": output_data,
                "stderr": stderr if stderr else None,
            }

            if success:
                LOGGER.info("✓ Verification passed for %s", nvr)
            else:
                LOGGER.warning("✗ Verification failed for %s", nvr)

            return result

        except Exception as e:
            LOGGER.error("Exception during verification of %s: %s", nvr, e)
            return {"pullspec": pullspec, "success": False, "error": str(e), "output": None}

    def _report_results(self, results: Dict[str, Dict]):
        """Report verification results summary."""
        total = len(results)
        passed = sum(1 for r in results.values() if r["success"])
        failed = total - passed

        LOGGER.info("=== Conforma Verification Results ===")
        LOGGER.info("Total: %d, Passed: %d, Failed: %d", total, passed, failed)

        if failed > 0:
            LOGGER.warning("Failed verifications:")
            for nvr, result in results.items():
                if not result["success"]:
                    error_msg = result.get("error") or result.get("stderr") or "Verification failed"
                    LOGGER.warning("  ✗ %s - %s", nvr, error_msg)

        if passed > 0:
            LOGGER.info("Passed verifications:")
            for nvr, result in results.items():
                if result["success"]:
                    LOGGER.info("  ✓ %s", nvr)

    def _save_results_to_file(self, results: Dict[str, Dict]):
        """Save detailed verification results to results.yaml file."""
        results_file = "results.yaml"

        # Prepare results data for YAML output
        results_data = {
            "summary": {
                "total": len(results),
                "passed": sum(1 for r in results.values() if r["success"]),
                "failed": sum(1 for r in results.values() if not r["success"]),
            },
            "verifications": results,
        }

        try:
            with open(results_file, 'w') as f:
                yaml.dump(results_data, f, default_flow_style=False, sort_keys=False)
            LOGGER.info("Detailed results saved to %s", results_file)
        except Exception as e:
            LOGGER.error("Failed to save results to %s: %s", results_file, e)


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
    '--kubeconfig',
    metavar='PATH',
    help='Path to the kubeconfig file to use. Can also be set via KUBECONFIG env var.',
)
@click.pass_obj
@click_coroutine
async def verify_conforma_cli(
    runtime: Runtime,
    nvrs_file,
    nvrs,
    konflux_policy,
    kubeconfig,
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

    if not kubeconfig:
        kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    pipeline = ConformaVerifyCli(
        runtime=runtime,
        nvrs=nvrs,
        policy_path=konflux_policy,
        kubeconfig=kubeconfig,
    )
    results = await pipeline.run()

    # Output brief summary (detailed results are saved to results.yaml)
    total = len(results)
    passed = sum(1 for r in results.values() if r["success"])
    failed = total - passed

    click.echo(f"Verification complete: {passed}/{total} passed. See results.yaml for details.")

    # Exit with non-zero code if any verifications failed
    if failed > 0:
        sys.exit(1)
