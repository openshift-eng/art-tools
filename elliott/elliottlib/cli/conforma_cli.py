import base64
import json
import os
import sys
import tempfile
from typing import Dict, Tuple

import click
from artcommonlib import logutil
from artcommonlib.exectools import cmd_assert_async, cmd_gather_async
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
        env: str = None,
        policy_path: str = None,
        konflux_kubeconfig: str = None,
        pull_secret: str = None,
    ):
        self.runtime = runtime
        if not nvrs:
            raise ValueError("nvrs must be provided")
        self.nvrs = nvrs
        self.nvrs_by_pullspec = None
        self.policy_path = policy_path
        self.konflux_kubeconfig = konflux_kubeconfig
        self.pull_secret = pull_secret
        self.env = env

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

    async def run(self) -> Dict[str, Dict]:
        """Run conforma verification for all NVRs and return results."""
        LOGGER.info("Verifying %d NVRs with Conforma", len(self.nvrs))

        snapshot_pipeline = CreateSnapshotCli(
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
        snapshots = await snapshot_pipeline.run()
        if len(snapshots) != 1:
            raise ValueError(
                "Expected exactly one snapshot, got %d. Do not provide NVRs of multiple kinds (image/bundle/fbc).",
                len(snapshots),
            )
        snapshot_spec = snapshots[0].spec
        if not self.policy_path:
            self.policy_path = self.get_policy_url(snapshot_spec.application, self.env)
            LOGGER.info(
                "Found policy for application %s and env %s: %s", snapshot_spec.application, self.env, self.policy_path
            )

        self.pullspec_by_name = {comp.name: comp.containerImage for comp in snapshot_spec.components}

        # we need to fetch build records so we can map nvrs to pullspecs
        build_records_by_nvrs = await get_build_records_by_nvrs(self.runtime, self.nvrs, strict=True)
        self.nvrs_by_pullspec = {str(record.image_pullspec): record.nvr for record in build_records_by_nvrs.values()}

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
        output_data = yaml.load(stdout)

        nvr_results = {}
        for component in output_data["components"]:
            component_name = component["name"]
            if ":" in component_name:
                component_name = component_name.split(":")[0].removesuffix("-sha256")
            main_image_pullspec = self.pullspec_by_name[component_name]
            nvr = self.nvrs_by_pullspec[main_image_pullspec]

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

        success = rc == 0
        result = {
            "nvr_results": nvr_results,
            "success": success,
            "output": output_data,
            "stderr": stderr if stderr else None,
        }

        if success:
            LOGGER.info("✓ Verification passed")
        else:
            LOGGER.warning("✗ Verification failed")

        return result


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
@click.pass_obj
@click_coroutine
async def verify_conforma_cli(
    runtime: Runtime,
    nvrs_file,
    nvrs,
    konflux_policy,
    konflux_kubeconfig,
    pull_secret,
    env,
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
    if env and konflux_policy:
        raise click.BadParameter("Cannot specify both --env and --konflux-policy")

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
        pull_secret = os.environ.get('KONFLUX_ART_IMAGES_AUTH_FILE')

    pipeline = ConformaVerifyCli(
        runtime=runtime,
        nvrs=nvrs,
        policy_path=konflux_policy,
        konflux_kubeconfig=konflux_kubeconfig,
        pull_secret=pull_secret,
        env=env,
    )
    results = await pipeline.run()

    click.echo("Verification complete. See results.yaml for details.")

    if not results["success"]:
        sys.exit(1)
