import json
import logging
import os
import shutil
from datetime import datetime, timezone
from functools import cached_property
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse

import click
import gitlab
from artcommonlib import exectools
from artcommonlib.assembly import AssemblyTypes, assembly_group_config
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.model import Model
from artcommonlib.util import new_roundtrip_yaml_handler
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.shipment_model import ShipmentConfig, Spec
from ghapi.all import GhApi

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime
from pyartcd.slack import SlackClient
from pyartcd.util import (
    get_assembly_type,
    get_release_name_for_assembly,
)

_LOGGER = logging.getLogger(__name__)
yaml = new_roundtrip_yaml_handler()


class PrepareReleaseKonfluxPipeline:
    def __init__(
        self,
        slack_client: SlackClient,
        runtime: Runtime,
        group: Optional[str],
        assembly: Optional[str],
        build_repo_url: Optional[str],
        shipment_repo_url: Optional[str],
        github_token: Optional[str],
        gitlab_token: Optional[str],
        job_url: Optional[str],
    ) -> None:
        self.runtime = runtime
        self.assembly = assembly
        self.group = group
        self._slack_client = slack_client
        self.github_token = github_token
        self.gitlab_token = gitlab_token

        self.gitlab_url = self.runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        self.application = KonfluxImageBuilder.get_application_name(self.group)
        self.working_dir = self.runtime.working_dir.absolute()
        self.elliott_working_dir = self.working_dir / "elliott-working"
        self._build_repo_dir = self.working_dir / "ocp-build-data-push"
        self._shipment_repo_dir = self.working_dir / "shipment-data-push"
        self.job_url = job_url
        self.dry_run = self.runtime.dry_run
        self.product = 'ocp'  # assume that product is ocp for now

        # Have clear pull and push targets for both the build and shipment repos
        build_repo_vars = self._build_repo_vars(build_repo_url)
        self.build_repo_pull_url, self.build_data_gitref, self.build_data_push_url = build_repo_vars
        self.shipment_repo_pull_url, self.shipment_repo_push_url = self._shipment_repo_vars(shipment_repo_url)
        self.build_data_repo = GitRepository(self._build_repo_dir, self.dry_run)
        self.shipment_data_repo = GitRepository(self._shipment_repo_dir, self.dry_run)

        # these will be initialized later
        self.releases_config = None
        self.group_config = None

        group_param = f'--group={group}'
        if self.build_data_gitref:
            group_param += f'@{self.build_data_gitref}'

        self._elliott_base_command = [
            'elliott',
            group_param,
            f'--assembly={self.assembly}',
            '--build-system=konflux',
            f'--working-dir={self.elliott_working_dir}',
            f'--data-path={self.build_repo_pull_url}',
        ]

    @staticmethod
    def basic_auth_url(url: str, token: str) -> str:
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme
        rest_of_the_url = url[len(scheme + "://") :]
        # the assumption here is that username can be anything
        # so we use oauth2 as a placeholder username
        # and the token as the password
        return f'https://oauth2:{token}@{rest_of_the_url}'

    def _build_repo_vars(self, build_repo_url: Optional[str]):
        build_repo_pull_url = (
            build_repo_url
            or self.runtime.config.get("build_config", {}).get("ocp_build_data_url")
            or constants.OCP_BUILD_DATA_URL
        )
        build_data_gitref = None
        if "@" in build_repo_pull_url:
            build_repo_pull_url, build_data_gitref = build_repo_pull_url.split("@", 1)

        build_data_push_url = (
            self.runtime.config.get("build_config", {}).get("ocp_build_data_push_url") or constants.OCP_BUILD_DATA_URL
        )
        return build_repo_pull_url, build_data_gitref, build_data_push_url

    def _shipment_repo_vars(self, shipment_repo_url: Optional[str]):
        shipment_repo_pull_url = (
            shipment_repo_url
            or self.runtime.config.get("shipment_config", {}).get("shipment_data_url")
            or SHIPMENT_DATA_URL_TEMPLATE.format(self.product)
        )
        shipment_repo_push_url = self.runtime.config.get("shipment_config", {}).get(
            "shipment_data_push_url"
        ) or SHIPMENT_DATA_URL_TEMPLATE.format(self.product)
        return shipment_repo_pull_url, shipment_repo_push_url

    @cached_property
    def _errata_api(self) -> AsyncErrataAPI:
        return AsyncErrataAPI()

    @cached_property
    def _gitlab(self) -> gitlab.Gitlab:
        gl = gitlab.Gitlab(self.gitlab_url, private_token=self.gitlab_token)
        gl.auth()
        return gl

    @property
    def release_name(self) -> str:
        return get_release_name_for_assembly(self.group, self.releases_config, self.assembly)

    @property
    def assembly_group_config(self) -> dict:
        return self.releases_config["releases"][self.assembly].setdefault("assembly", {}).setdefault("group", {})

    @property
    def shipment_config(self) -> dict:
        shipment_key = next(k for k in self.assembly_group_config.keys() if k.startswith("shipment"))
        return self.assembly_group_config.get(shipment_key, [])

    def setup_working_dir(self):
        self.working_dir.mkdir(parents=True, exist_ok=True)
        if self._build_repo_dir.exists():
            shutil.rmtree(self._build_repo_dir, ignore_errors=True)
        if self._shipment_repo_dir.exists():
            shutil.rmtree(self._shipment_repo_dir, ignore_errors=True)
        if self.elliott_working_dir.exists():
            shutil.rmtree(self.elliott_working_dir, ignore_errors=True)

    async def setup_repos(self):
        await self.build_data_repo.setup(
            remote_url=self.build_data_push_url,
            upstream_remote_url=self.build_repo_pull_url,
        )
        await self.build_data_repo.fetch_switch_branch(self.build_data_gitref or self.group)
        await self.shipment_data_repo.setup(
            remote_url=self.basic_auth_url(self.shipment_repo_push_url, self.gitlab_token),
            upstream_remote_url=self.shipment_repo_pull_url,
        )
        await self.shipment_data_repo.fetch_switch_branch("main")

        self.releases_config = yaml.load(await self.build_data_repo.read_file("releases.yml"))
        self.group_config = yaml.load(await self.build_data_repo.read_file("group.yml"))

    async def validate_assembly(self):
        # validate assembly and init release vars
        if self.releases_config.get("releases", {}).get(self.assembly) is None:
            raise ValueError(f"Assembly not found: {self.assembly}")
        assembly_type = get_assembly_type(self.releases_config, self.assembly)
        if assembly_type == AssemblyTypes.STREAM:
            raise ValueError("Preparing a release from a stream assembly is no longer supported.")

        # validate product from group config
        merged_group_config = assembly_group_config(
            Model(self.releases_config), self.assembly, Model(self.group_config)
        ).primitive()
        group_product = merged_group_config.get("product", self.product)
        if group_product != self.product:
            raise ValueError(
                f"Product mismatch: {group_product} != {self.product}. This pipeline only supports {self.product}."
            )

    async def run(self):
        self.setup_working_dir()
        await self.setup_repos()
        await self.validate_assembly()
        await self.prepare_shipment()

    async def prepare_shipment(self):
        shipment_config = self.shipment_config.copy()
        shipment_url = shipment_config.get("url")
        # if shipment MR isn't valid, complain and exit early
        if shipment_url:
            self.validate_shipment_mr(shipment_url)

        shipment_advisories = shipment_config.get("advisories")
        if not shipment_advisories:
            raise ValueError(
                "Operation not supported: shipment config should specify which advisories to create and prepare"
            )

        group_advisories = set(self.assembly_group_config.get("advisories", {}).keys())
        shipment_advisory_kinds = {advisory.get("kind") for advisory in shipment_advisories}
        common = shipment_advisory_kinds & group_advisories
        if common:
            raise ValueError(
                f"shipment config should not specify advisories that are already defined in assembly.group.advisories: {common}"
            )

        env = shipment_config.get("env", "prod")
        if env not in ["prod", "stage"]:
            raise ValueError("shipment config `env` should be either `prod` or `stage`")

        generated_shipments: Dict[str, ShipmentConfig] = {}
        for shipment_advisory_config in shipment_advisories:
            kind = shipment_advisory_config.get("kind")
            if not kind:
                raise ValueError("shipment config should specify `kind` for an advisory")
            shipment: ShipmentConfig = await self.init_shipment(kind)

            # a liveID is required for prod, but not for stage
            # so if it is missing, we need to reserve one
            live_id = shipment_advisory_config.get("live_id")
            if env == "prod" and not live_id:
                _LOGGER.info("Requesting liveID for %s advisory", kind)
                if self.dry_run:
                    _LOGGER.info("[DRY-RUN] Would've reserved liveID for %s advisory", kind)
                    live_id = "DRY_RUN_LIVE_ID"
                else:
                    live_id = await self._errata_api.reserve_live_id()
                if not live_id:
                    raise ValueError(f"Failed to get liveID for {kind} advisory")
                shipment_advisory_config["live_id"] = live_id
            if live_id:
                shipment.shipment.data.releaseNotes.live_id = live_id

            # find builds for the advisory
            if kind in ("image", "extras"):
                snapshot_spec = await self.find_builds(kind)
                shipment.shipment.snapshot.spec = snapshot_spec
            else:
                _LOGGER.warning("Shipment kind %s is not supported for build finding", kind)

            generated_shipments[kind] = shipment

        # close errata API connection now that we have the liveIDs
        # since it's a cached_property, check if it got initialized
        if "_errata_api" in self.__dict__:
            self._errata_api.close()

        if not shipment_url or shipment_url == "N/A":
            shipment_mr_url = await self.create_shipment_mr(generated_shipments, env)
            shipment_config["url"] = shipment_mr_url
            await self._slack_client.say_in_thread(f"Shipment MR created: {shipment_mr_url}")
        else:
            _LOGGER.info("Shipment MR already exists: %s. Checking if it needs an update..", shipment_url)
            updated = await self.update_shipment_mr(generated_shipments, env, shipment_url)
            if updated:
                await self._slack_client.say_in_thread(f"Shipment MR updated: {shipment_url}")

        await self.create_update_build_data_pr(shipment_config)

    async def init_shipment(self, kind: str) -> ShipmentConfig:
        create_cmd = self._elliott_base_command + [
            f'--shipment-path={self.shipment_repo_pull_url}',
            "shipment",
            "init",
            f"--advisory-key={kind}",
            f"--application={self.application}",
        ]
        rc, stdout, stderr = await exectools.cmd_gather_async(create_cmd, check=False)
        if stderr:
            _LOGGER.info("Shipment init command stderr:\n %s", stderr)
        if stdout:
            _LOGGER.info("Shipment init command stdout:\n %s", stdout)
        if rc != 0:
            raise RuntimeError(f"cmd failed with exit code {rc}: {create_cmd}")

        out = yaml.load(stdout)
        shipment = ShipmentConfig(**out)
        return shipment

    async def find_builds(self, kind: str) -> Spec:
        if kind not in ("image", "extras"):
            raise ValueError(f"Invalid kind: {kind}. Only image and extras are supported")
        payload = True if kind == "image" else False

        find_builds_cmd = self._elliott_base_command + [
            "find-builds",
            "--kind=image",
            "--payload" if payload else "--non-payload",
            "--json=-",
        ]
        rc, stdout, stderr = await exectools.cmd_gather_async(find_builds_cmd)
        if stderr:
            _LOGGER.info("Shipment find-builds command stderr:\n %s", stderr)
        if stdout:
            _LOGGER.info("Shipment find-builds command stdout:\n %s", stdout)
        if rc != 0:
            raise RuntimeError(f"cmd failed with exit code {rc}: {find_builds_cmd}")

        builds = []
        if stdout:
            out = json.loads(stdout)
            builds = out.get("builds", [])
        return Spec(nvrs=builds)

    def validate_shipment_mr(self, shipment_url: str):
        # Parse the shipment URL to extract project and MR details
        parsed_url = urlparse(shipment_url)
        target_project_path = parsed_url.path.strip('/').split('/-/merge_requests')[0]
        mr_id = parsed_url.path.split('/')[-1]

        # Load the existing MR
        project = self._gitlab.projects.get(target_project_path)
        mr = project.mergerequests.get(mr_id)

        # Make sure MR is valid
        # and aligns with the push and pull repos
        if mr.state != "opened":
            raise ValueError(f"MR state {mr.state} is not opened. This is not supported.")

        if target_project_path not in self.shipment_repo_pull_url:
            raise ValueError(
                f"MR target project {target_project_path} does not match the pull repo {self.shipment_repo_pull_url}"
            )

        source_project_path = self._gitlab.projects.get(mr.source_project_id).path_with_namespace
        if source_project_path not in self.shipment_repo_push_url:
            raise ValueError(
                f"MR source project {source_project_path} does not match the push repo {self.shipment_repo_push_url}"
            )

        if mr.target_branch != "main":
            raise ValueError(f"MR target branch {mr.target_branch} is not main. This is not supported.")

        _LOGGER.info("Shipment MR is valid: %s", shipment_url)

    async def update_shipment_data(
        self, shipments: Dict[str, ShipmentConfig], env: str, commit_message: str, branch: str
    ) -> bool:
        """Update shipment data repo with the given shipment config files.
        Commits the changes and push to the remote repo."""

        relative_target_dir = Path("shipment") / self.product / self.group / self.application / env
        target_dir = self.shipment_data_repo._directory / relative_target_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        # Get the timestamp from the branch name
        # which we need for filenames
        # The branch name is expected to be in the format: prepare-shipment-<assembly>-<timestamp>
        timestamp = branch.split("-")[-1]

        for advisory_key, shipment_config in shipments.items():
            filename = f"{self.assembly}.{advisory_key}.{timestamp}.yaml"
            filepath = relative_target_dir / filename
            _LOGGER.info("Updating shipment file: %s", filename)
            yaml_data = yaml.dump(shipment_config.model_dump(exclude_unset=True, exclude_none=True))
            await self.shipment_data_repo.write_file(filepath, yaml_data)
        await self.shipment_data_repo.log_diff()
        return await self.shipment_data_repo.commit_push(commit_message)

    async def update_shipment_mr(self, shipments: Dict[str, ShipmentConfig], env: str, shipment_url: str) -> bool:
        """Update existing shipment MR with the given shipment config files."""

        _LOGGER.info("Updating shipment MR: %s", shipment_url)

        # Parse the shipment URL to extract project and MR details
        parsed_url = urlparse(shipment_url)
        target_project_path = parsed_url.path.strip('/').split('/-/merge_requests')[0]
        mr_id = parsed_url.path.split('/')[-1]

        # Load the existing MR
        project = self._gitlab.projects.get(target_project_path)
        mr = project.mergerequests.get(mr_id)

        # Checkout the MR branch
        source_branch = mr.source_branch
        await self.shipment_data_repo.fetch_switch_branch(source_branch, remote="origin")

        # Update shipment data
        commit_message = f"Update shipment configurations for {self.release_name}"
        updated = await self.update_shipment_data(shipments, env, commit_message, source_branch)
        if not updated:
            _LOGGER.info("No changes in shipment data. MR will not be updated.")
            return False

        # Update the MR description
        description_update = f"Updated by job: {self.job_url}\n\n" if self.job_url else commit_message
        mr.description = f"{mr.description}\n\n{description_update}"

        if self.dry_run:
            _LOGGER.info("[DRY-RUN] Would have updated MR description: %s", mr.description)
        else:
            mr.save()
            _LOGGER.info("Shipment MR updated: %s", shipment_url)

        return True

    async def create_shipment_mr(self, shipment_configs: Dict[str, ShipmentConfig], env: str) -> str:
        _LOGGER.info("Creating shipment MR...")

        # Create branch name
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        source_branch = f"prepare-shipment-{self.assembly}-{timestamp}"
        target_branch = "main"

        # Create and checkout branch
        await self.shipment_data_repo.create_branch(source_branch)

        # update shipment data repo with shipment configs
        commit_message = f"Add shipment configurations for {self.release_name}"
        updated = await self.update_shipment_data(shipment_configs, env, commit_message, source_branch)
        if not updated:
            # this should not happen
            raise ValueError("Failed to update shipment data repo. Please investigate.")

        def _get_project(url):
            parsed_url = urlparse(url)
            project_path = parsed_url.path.strip('/').removesuffix('.git')
            return self._gitlab.projects.get(project_path)

        source_project = _get_project(self.shipment_repo_push_url)
        target_project = _get_project(self.shipment_repo_pull_url)

        mr_title = f"Shipment for {self.release_name}"
        mr_description = f"Created by job: {self.job_url}\n\n" if self.job_url else commit_message

        if self.dry_run:
            _LOGGER.info("[DRY-RUN] Would have created MR with title: %s", mr_title)
            mr_url = f"{self.gitlab_url}/placeholder/placeholder/-/merge_requests/placeholder"
        else:
            mr = source_project.mergerequests.create(
                {
                    'source_branch': source_branch,
                    'target_project_id': target_project.id,
                    'target_branch': target_branch,
                    'title': mr_title,
                    'description': mr_description,
                    'remove_source_branch': True,
                }
            )
            mr_url = mr.web_url
            _LOGGER.info("Created Merge Request: %s", mr_url)

        return mr_url

    async def update_build_data(self, group_shipment_config: dict, branch: str) -> bool:
        """Update releases.yml in build data repo with the given shipment assembly config.
        Commits the changes and push to the remote repo."""

        group_config = (
            self.releases_config["releases"][self.assembly].setdefault("assembly", {}).setdefault("group", {})
        )

        # Assembly key names are not always exact, they can end in special chars like !,?,-
        # to indicate special inheritance rules. So respect those
        # https://art-docs.engineering.redhat.com/assemblies/#inheritance-rules
        shipment_key = next(k for k in group_config.keys() if k.startswith("shipment"))
        group_config[shipment_key] = group_shipment_config

        if await self.build_data_repo.does_branch_exist_on_remote(branch, remote="origin"):
            await self.build_data_repo.fetch_switch_branch(branch, remote="origin")
        else:
            await self.build_data_repo.create_branch(branch)

        await self.build_data_repo.write_file("releases.yml", yaml.dump(self.releases_config))
        await self.build_data_repo.log_diff()

        commit_message = f"Update shipment for assembly {self.assembly}"
        return await self.build_data_repo.commit_push(commit_message)

    async def create_update_build_data_pr(self, shipment_config: dict) -> bool:
        branch = f"update-shipment-{self.release_name}"
        updated = await self.update_build_data(shipment_config, branch)
        if not updated:
            return False

        api = GhApi()
        target_repo = self.build_repo_pull_url.split('/')[-1].replace('.git', '')
        source_owner = self.build_data_push_url.split('/')[-2]
        target_owner = self.build_repo_pull_url.split('/')[-2]

        head = f"{source_owner}:{branch}"
        base = self.build_data_gitref or self.group
        api = GhApi(owner=target_owner, repo=target_repo, token=self.github_token)
        existing_prs = api.pulls.list(
            state="open",
            head=head,
            base=base,
        )
        if not existing_prs.items:
            pr_title = f"Update shipment for assembly {self.assembly}"
            pr_body = f"This PR updates the shipment data for assembly {self.assembly}."
            if self.job_url:
                pr_body += f"\n\nCreated by job: {self.job_url}"

            if self.dry_run:
                _LOGGER.info("[DRY-RUN] Would have created a new PR with title '%s'", pr_title)
                return True

            result = api.pulls.create(
                head=head,
                base=base,
                title=pr_title,
                body=pr_body,
                maintainer_can_modify=True,
            )
            _LOGGER.info("PR to update shipment created: %s", result.html_url)
            await self._slack_client.say_in_thread(f"PR to update shipment created: {result.html_url}")
        else:
            _LOGGER.info("Existing PR to update shipment found: %s", existing_prs.items[0].html_url)
            pull_number = existing_prs.items[0].number

            if self.dry_run:
                _LOGGER.info("[DRY-RUN] Would have updated PR with number %s", pull_number)
                return True

            pr_body = existing_prs.items[0].body
            if self.job_url:
                pr_body += f"\n\nUpdated by job: {self.job_url}"
            result = api.pulls.update(
                pull_number=pull_number,
                body=pr_body,
            )
            _LOGGER.info("PR to update shipment updated: %s", result.html_url)
            await self._slack_client.say_in_thread(f"PR to update shipment updated: {result.html_url}")

        return True


@cli.command("prepare-release-konflux")
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The group to operate on e.g. openshift-4.18",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The assembly to operate on e.g. 4.18.5",
)
@click.option(
    '--build-repo-url',
    help='ocp-build-data repo to use. Defaults to group branch - to use a different branch/commit use repo@branch',
)
@click.option(
    '--shipment-repo-url',
    help='shipment-data repo to use for reading and as shipment MR target. Defaults to main branch. Should reside in gitlab.cee.redhat.com',
)
@pass_runtime
@click_coroutine
async def prepare_release(
    runtime: Runtime, group: str, assembly: str, build_repo_url: Optional[str], shipment_repo_url: Optional[str]
):
    job_url = os.environ.get('BUILD_URL')

    github_token = os.environ.get('GITHUB_TOKEN')
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is required to create a pull request")

    gitlab_token = os.environ.get("GITLAB_TOKEN")
    if not gitlab_token:
        raise ValueError("GITLAB_TOKEN environment variable is required to create a merge request")

    if assembly == "stream":
        raise click.BadParameter("Release cannot be prepared from stream assembly.")

    slack_client = runtime.new_slack_client()
    slack_client.bind_channel(group)
    await slack_client.say_in_thread(f":construction: prepare-release-konflux for {assembly} :construction:")

    try:
        # start pipeline
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=slack_client,
            runtime=runtime,
            group=group,
            assembly=assembly,
            build_repo_url=build_repo_url,
            shipment_repo_url=shipment_repo_url,
            github_token=github_token,
            gitlab_token=gitlab_token,
            job_url=job_url,
        )
        await pipeline.run()
        await slack_client.say_in_thread(f":white_check_mark: prepare-release-konflux for {assembly} completes.")
    except Exception as e:
        await slack_client.say_in_thread(f":warning: prepare-release-konflux for {assembly} has result FAILURE.")
        raise e
