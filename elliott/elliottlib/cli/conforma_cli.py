import base64
import copy
import json
import os
import sys
import tempfile
from typing import Dict, List, Tuple

import click
from artcommonlib import logutil
from artcommonlib.exectools import cmd_assert_async, cmd_gather_async
from artcommonlib.rhcos import get_container_configs
from artcommonlib.util import new_roundtrip_yaml_handler
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.cli.snapshot_cli import CreateSnapshotCli, get_build_records_by_nvrs
from elliottlib.runtime import Runtime
from elliottlib.util import get_nvrs_from_release

LOGGER = logutil.get_logger(__name__)

yaml = new_roundtrip_yaml_handler()


class ConformaVerifyCli:
    def __init__(
        self,
        runtime: Runtime,
        pullspec: str,
        nvrs: list,
        env: str = None,
        policy_path: str = None,
        konflux_kubeconfig: str = None,
        pull_secret: str = None,
        batch_size: int = 10,
        fail_fast: bool = False,
    ):
        if not pullspec and not nvrs:
            raise ValueError("Either pullspec or nvrs must be provided")

        self.runtime = runtime
        self.pullspec = pullspec
        self.nvrs = nvrs
        self.nvrs_by_pullspec = None
        self.policy_path = policy_path
        self.konflux_kubeconfig = konflux_kubeconfig
        self.pull_secret = pull_secret
        self.env = env
        self.batch_size = batch_size
        self.fail_fast = fail_fast

    @staticmethod
    def get_policy_url(application: str, env: str) -> str:
        gitlab_base_url = "https://gitlab.cee.redhat.com/releng/konflux-release-data/-/blob/main/"
        policy_url_map = {
            "stage": f"{gitlab_base_url}config/kflux-ocp-p01.7ayg.p1/product/EnterpriseContractPolicy/registry-ocp-art-stage.yaml",
            "prod": f"{gitlab_base_url}config/kflux-ocp-p01.7ayg.p1/product/EnterpriseContractPolicy/registry-ocp-art-prod.yaml",
            "fbc-stage": f"{gitlab_base_url}config/common/product/EnterpriseContractPolicy/fbc-stage.yaml",
            "fbc-prod": f"{gitlab_base_url}config/common/product/EnterpriseContractPolicy/fbc-standard.yaml",
        }
        key = env
        if "fbc" in application:
            key = f"fbc-{env}"
        return policy_url_map[key]

    def _split_into_batches(self, items: List[str], batch_size: int) -> List[List[str]]:
        """Split a list into batches of specified size."""
        batches = []
        for i in range(0, len(items), batch_size):
            batches.append(items[i : i + batch_size])
        return batches

    def _merge_results(self, batch_results: List[Dict]) -> Dict:
        """Merge results from multiple batches."""
        if not batch_results:
            return {
                "success": False,
                "total_nvrs": 0,
                "failed_nvrs": 0,
                "nvr_results": {},
                "output": {},
            }

        merged_result = {
            "success": True,
            "total_nvrs": 0,
            "failed_nvrs": 0,
            "nvr_results": {},
            "output": {
                "components": [],
                "success": True,
            },
        }

        for batch_result in batch_results:
            # Merge success status (all batches must succeed)
            merged_result["success"] &= batch_result["success"]

            # Merge NVR counts
            merged_result["total_nvrs"] += batch_result["total_nvrs"]
            merged_result["failed_nvrs"] += batch_result["failed_nvrs"]

            # Merge NVR results
            merged_result["nvr_results"].update(batch_result["nvr_results"])

            # Merge output components
            if "output" in batch_result and "components" in batch_result["output"]:
                merged_result["output"]["components"].extend(batch_result["output"]["components"])

            # Update overall output success
            if "output" in batch_result:
                merged_result["output"]["success"] &= batch_result["output"].get("success", True)

        return merged_result

    async def _run_batch(self, batch_nvrs: List[str], policy_file: str, cosign_pub_file: str) -> Dict:
        """Run verification for a single batch of NVRs."""
        LOGGER.info("Processing batch of %d NVRs: %s", len(batch_nvrs), batch_nvrs)

        snapshot_pipeline = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config={
                'kubeconfig': self.konflux_kubeconfig,
                'namespace': KONFLUX_DEFAULT_NAMESPACE,
                'context': None,
            },
            image_repo_pull_secret=self.pull_secret,
            builds=batch_nvrs,
            dry_run=True,
            job_url=None,
        )
        snapshots = await snapshot_pipeline.run()
        if len(snapshots) != 1:
            raise ValueError(
                "Expected exactly one snapshot, got %d. Do not provide NVRs of multiple kinds (image/bundle/fbc).",
                len(snapshots),
            )
        snapshot_spec = snapshots[0].spec

        pullspec_by_name = {comp.name: comp.containerImage for comp in snapshot_spec.components}

        # we need to fetch build records so we can map nvrs to pullspecs
        build_records_by_nvrs = await get_build_records_by_nvrs(self.runtime, batch_nvrs)
        nvrs_by_pullspec = {str(record.image_pullspec): record.nvr for record in build_records_by_nvrs.values()}

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            yaml.dump(snapshot_spec.to_dict(), temp_file)

        # Use pre-created verification files
        result = await self._verify_batch_snapshot(
            temp_file_path, policy_file, cosign_pub_file, pullspec_by_name, nvrs_by_pullspec
        )

        # Clean up temp file
        os.unlink(temp_file_path)

        return result

    async def _determine_policy_path_if_needed(self) -> None:
        """Determine policy path if not already set by running a small sample to get application type."""
        if self.policy_path:
            return  # Already set

        # We need to run a small sample to determine the application type
        sample_nvrs = self.nvrs[:1]  # Just use first NVR to determine application
        LOGGER.info("Determining application type from sample NVR: %s", sample_nvrs[0])

        snapshot_pipeline = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config={
                'kubeconfig': self.konflux_kubeconfig,
                'namespace': KONFLUX_DEFAULT_NAMESPACE,
                'context': None,
            },
            image_repo_pull_secret=self.pull_secret,
            builds=sample_nvrs,
            dry_run=True,
            job_url=None,
        )
        snapshots = await snapshot_pipeline.run()
        if len(snapshots) != 1:
            raise ValueError(
                "Expected exactly one snapshot, got %d. Do not provide NVRs of multiple kinds (image/bundle/fbc).",
                len(snapshots),
            )
        snapshot_spec = snapshots[0].spec

        self.policy_path = self.get_policy_url(snapshot_spec.application, self.env)
        LOGGER.info(
            "Determined policy for application %s and env %s: %s", snapshot_spec.application, self.env, self.policy_path
        )

    async def run(self) -> Dict[str, Dict]:
        """Run conforma verification for all NVRs in batches and return merged results."""

        self.runtime.initialize(build_system='konflux', mode='images')

        if self.pullspec:
            rhcos_images = {c['name'] for c in get_container_configs(self.runtime)}
            nvr_map = await get_nvrs_from_release(self.pullspec, rhcos_images, LOGGER)
            self.nvrs = []
            for component, (v, r) in nvr_map.items():
                self.nvrs.append(f"{component}-{v}-{r}")

        LOGGER.info("Verifying %d NVRs with Conforma in batches of %d", len(self.nvrs), self.batch_size)

        # Split NVRs into batches
        batches = self._split_into_batches(self.nvrs, self.batch_size)
        LOGGER.info("Processing %d batches", len(batches))

        # Determine policy path if not already set
        await self._determine_policy_path_if_needed()

        # Setup verification files once for all batches
        with tempfile.TemporaryDirectory() as temp_dir:
            LOGGER.info("Setting up verification files (once for all batches)...")
            policy_file, cosign_pub_file = await self._setup_verification_files(temp_dir)

            # Process each batch
            batch_results = []
            for i, batch_nvrs in enumerate(batches, 1):
                LOGGER.info("Processing batch %d/%d", i, len(batches))
                try:
                    batch_result = await self._run_batch(batch_nvrs, policy_file, cosign_pub_file)
                    batch_results.append(batch_result)

                    # Log batch results for real-time monitoring
                    LOGGER.info("=== Batch %d/%d Results ===", i, len(batches))
                    LOGGER.info("Batch success: %s", batch_result['success'])
                    LOGGER.info("Failed NVRs in batch: %d/%d", batch_result['failed_nvrs'], batch_result['total_nvrs'])

                    if batch_result['nvr_results']:
                        LOGGER.info("NVR Results:")
                        for nvr, result in batch_result['nvr_results'].items():
                            status = "✓ PASS" if result['success'] else "✗ FAIL"
                            violations_info = ""
                            if 'violations' in result:
                                violations_info = f" ({result['violations']['total']} violations: {', '.join(result['violations']['codes'])})"
                            LOGGER.info("  %s: %s%s", nvr, status, violations_info)
                    else:
                        LOGGER.info("  No NVR results available")

                except Exception as e:
                    LOGGER.error("Failed to process batch %d: %s", i, e)
                    # Create a failed result for this batch
                    failed_result = {
                        "success": False,
                        "total_nvrs": len(batch_nvrs),
                        "failed_nvrs": len(batch_nvrs),
                        "nvr_results": {nvr: {"success": False} for nvr in batch_nvrs},
                        "output": {},
                    }
                    batch_results.append(failed_result)

                    # Log failed batch info
                    LOGGER.error("=== Batch %d/%d Results ===", i, len(batches))
                    LOGGER.error("Batch success: False (Exception: %s)", e)
                    LOGGER.error("Failed NVRs in batch: %d/%d", len(batch_nvrs), len(batch_nvrs))
                    LOGGER.error("NVR Results:")
                    for nvr in batch_nvrs:
                        LOGGER.error("  %s: ✗ FAIL (batch exception)", nvr)

                # Write progressive results after each batch
                progressive_result = self._merge_results(batch_results)
                results_file = "results.yaml"

                # Create truncated version for YAML (only first 10 violation codes)
                truncated_for_yaml = self._truncate_violations_for_yaml(progressive_result)
                with open(results_file, 'w') as f:
                    yaml.dump(truncated_for_yaml, f)
                LOGGER.info(
                    "Progressive results updated in %s (processed %d/%d batches)", results_file, i, len(batches)
                )

                if self.fail_fast and not progressive_result["success"]:
                    LOGGER.warning(
                        "Failing fast due to failed NVR(s). %d NVRs failed so far. Stopping processing at batch %d/%d.",
                        progressive_result["failed_nvrs"],
                        i,
                        len(batches),
                    )
                    break

        # Merge results from all batches (final merge, though results.yaml is already up to date)
        merged_result = self._merge_results(batch_results)

        # Final log message
        if len(batch_results) < len(batches):
            LOGGER.info(
                "Processing stopped early at batch %d/%d due to fail-fast. Final results in %s",
                len(batch_results),
                len(batches),
                results_file,
            )
        else:
            LOGGER.info("All batches complete. Final results in %s", results_file)

        return merged_result

    async def _setup_verification_files(self, temp_dir: str) -> Tuple[str, str]:
        """Download policy.yaml and extract cosign public key."""
        LOGGER.info("Setting up verification files...")

        if not self.policy_path:
            raise RuntimeError("Policy path must be determined before setting up verification files")

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

        # workaround for https://gitlab.cee.redhat.com/releng/konflux-release-data/-/merge_requests/8746
        # setting spec.sources.ruleData.policy_intention explicitly to match managed-pipeline runs
        policy_content = yaml.load(open(policy_file, 'r'))
        policy_intention_set = False
        for source in policy_content['sources']:
            if source['name'] != "Release Policies":
                continue
            rule_data = source.get('ruleData', {})
            policy_intention = rule_data.get('policy_intention')
            if policy_intention:
                if self.env == "stage" and policy_intention != "staging":
                    raise ValueError(
                        f"Policy intention is not set to 'staging' for stage environment: {policy_intention}"
                    )
                if self.env == "prod" and policy_intention not in ["release", "production"]:
                    raise ValueError(
                        f"Policy intention is not set to 'release' or 'production' for prod environment: {policy_intention}"
                    )
                LOGGER.info(f"Policy intention was set to {policy_intention} in {policy_file}")
                policy_intention_set = True
            else:
                if self.env == "stage":
                    rule_data['policy_intention'] = "staging"
                else:
                    rule_data['policy_intention'] = "production"
                with open(policy_file, 'w') as f:
                    yaml.dump(policy_content, f)
                LOGGER.info(
                    f"Policy intention was not set in {policy_file}, setting to {rule_data['policy_intention']}"
                )
                policy_intention_set = True
            # there should be only one source with name "Release Policies"
            break
        if not policy_intention_set:
            raise ValueError(f"Policy intention was not set in {policy_file}")

        # Extract cosign public key
        cmd = ["oc", "get", "-n", "openshift-pipelines", "secret", "public-key", "-o", "json"]
        if self.konflux_kubeconfig:
            cmd.extend(["--kubeconfig", self.konflux_kubeconfig])
        _, stdout, _ = await cmd_gather_async(cmd)
        secret_data = json.loads(stdout)
        cosign_pub_data = base64.b64decode(secret_data['data']['cosign.pub']).decode('utf-8')

        with open(cosign_pub_file, 'w') as f:
            f.write(cosign_pub_data)

        LOGGER.info("Verification files prepared: policy=%s, cosign.pub=%s", policy_file, cosign_pub_file)
        return policy_file, cosign_pub_file

    async def _verify_batch_snapshot(
        self,
        snapshot_spec_filepath: str,
        policy_file: str,
        cosign_pub_file: str,
        pullspec_by_name: Dict,
        nvrs_by_pullspec: Dict,
    ) -> Dict:
        """Run ec validate for a given snapshot batch"""

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
            "--timeout",
            "30m",
            "--output",
            "yaml",
        ]

        # Run ec validate command
        rc, stdout, _ = await cmd_gather_async(cmd, stderr=None, check=False)
        if not stdout:
            # Get NVRs for this batch from nvrs_by_pullspec
            batch_nvrs = list(nvrs_by_pullspec.values())
            return {
                "success": False,
                "total_nvrs": len(batch_nvrs),
                "failed_nvrs": len(batch_nvrs),
                "nvr_results": {},
                "output": {},
            }

        output_data = yaml.load(stdout)

        nvr_results = {}
        for component in output_data.get("components", []):
            component_name = component["name"]
            if ":" in component_name:
                component_name = component_name.split(":")[0].removesuffix("-sha256")
            main_image_pullspec = pullspec_by_name[component_name]
            nvr = nvrs_by_pullspec[main_image_pullspec]

            if nvr not in nvr_results:
                nvr_results[nvr] = {
                    "success": component["success"],
                }
            nvr_results[nvr]["success"] &= component["success"]

            if component.get("violations"):
                if "violations" not in nvr_results[nvr]:
                    nvr_results[nvr]["violations"] = {
                        "total": 0,
                        "codes": set(),
                    }
                nvr_results[nvr]["violations"]["total"] += len(component["violations"])
                nvr_results[nvr]["violations"]["codes"].update(
                    {violation["metadata"]["code"] for violation in component["violations"]}
                )

        # Convert sets to lists for clean YAML output
        for nvr_data in nvr_results.values():
            if "violations" in nvr_data and "codes" in nvr_data["violations"]:
                nvr_data["violations"]["codes"] = list(nvr_data["violations"]["codes"])

        result = {
            "success": rc == 0,
            "total_nvrs": len(nvr_results),
            "failed_nvrs": len([nvr_data for nvr_data in nvr_results.values() if not nvr_data["success"]]),
            "nvr_results": nvr_results,
            "output": output_data,
        }

        if result["success"]:
            LOGGER.info("✓ Batch verification passed")
        else:
            LOGGER.warning("✗ Batch verification failed")

        return result

    def _truncate_violations_for_yaml(self, result: Dict) -> Dict:
        """Create a truncated version of results for YAML output (only first 10 violation codes)."""
        truncated = copy.deepcopy(result)

        if "nvr_results" in truncated:
            for nvr_data in truncated["nvr_results"].values():
                if "violations" in nvr_data and "codes" in nvr_data["violations"]:
                    # Keep only first 10 violation codes for cleaner YAML output
                    nvr_data["violations"]["codes"] = nvr_data["violations"]["codes"][:10]

        return truncated


@cli.command("verify-conforma", short_help="Verify given builds (NVRs) with Conforma")
@click.argument('nvrs', metavar='<NVR>', nargs=-1, required=False, default=None)
@click.option(
    "--nvrs-file",
    "-f",
    "nvrs_file",
    help="File to read NVRs from, `-` to read from STDIN.",
    type=click.File("rt"),
)
@click.option(
    "--pullspec",
    default=None,
    help="Pullspec of OCP payload to fetch NVRs from. e.g. registry.ci.openshift.org/ocp/konflux-release:4.19.0-0.konflux-nightly-2025-07-28-143852",
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
@click.option(
    '--env',
    metavar='ENV',
    help='Environment to use for the policy. Defaults to "prod".',
    type=click.Choice(["stage", "prod"]),
    default="prod",
)
@click.option(
    '--batch-size',
    metavar='SIZE',
    help='Number of NVRs to process in each batch. Defaults to 10.',
    type=int,
    default=10,
)
@click.option(
    '--fail-fast',
    is_flag=True,
    default=False,
    help='Stop processing immediately if any NVR fails in a batch.',
)
@click.pass_obj
@click_coroutine
async def verify_conforma_cli(
    runtime: Runtime,
    nvrs_file,
    nvrs,
    pullspec,
    konflux_policy,
    konflux_kubeconfig,
    pull_secret,
    env,
    batch_size,
    fail_fast,
):
    """
    Verify given builds (NVRs) with Conforma

    \b
    $ elliott -g openshift-4.18 verify-conforma --nvrs-file nvrs.txt

    \b
    $ elliott -g openshift-4.18 verify-conforma nvr1 nvr2 nvr3

    \b
    $ elliott -g openshift-4.18 verify-conforma --konflux-policy /path/to/custom/policy.yaml nvr1 nvr2

    \b
    $ elliott -g openshift-4.18 verify-conforma --kubeconfig /path/to/kubeconfig nvr1 nvr2

    \b
    $ elliott -g openshift-4.18 verify-conforma --batch-size 5 --fail-fast --nvrs-file nvrs.txt
    """
    if env and konflux_policy:
        raise click.BadParameter("Cannot specify both --env and --konflux-policy")

    if sum(map(bool, [nvrs, nvrs_file, pullspec])) != 1:
        raise click.BadParameter("Use only one of nvrs arguments, --nvrs-file or --pullspec")

    if nvrs_file:
        if nvrs_file == "-":
            nvrs_file = sys.stdin
        nvrs = [line.strip() for line in nvrs_file.readlines()]

    if not konflux_kubeconfig:
        konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    if not pull_secret:
        pull_secret = os.environ.get('KONFLUX_ART_IMAGES_AUTH_FILE')

    pipeline = ConformaVerifyCli(
        runtime=runtime,
        pullspec=pullspec,
        nvrs=nvrs,
        policy_path=konflux_policy,
        konflux_kubeconfig=konflux_kubeconfig,
        pull_secret=pull_secret,
        env=env,
        batch_size=batch_size,
        fail_fast=fail_fast,
    )
    results = await pipeline.run()

    click.echo(
        f"Verification complete. See results.yaml for details. {results['failed_nvrs']} failed out of {results['total_nvrs']} total."
    )

    if not results["success"]:
        sys.exit(1)
