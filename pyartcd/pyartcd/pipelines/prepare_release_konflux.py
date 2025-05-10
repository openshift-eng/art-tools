import json
import logging
import os
import re
import shutil
from datetime import datetime, timezone
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import aiofiles
import click
import gitlab
import semver
from artcommonlib import exectools, git_helper
from artcommonlib.assembly import AssemblyTypes, assembly_group_config
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.git_helper import gather_git_async, run_git_async
from artcommonlib.model import Model
from artcommonlib.util import convert_remote_git_to_ssh, new_roundtrip_yaml_handler
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.shipment_model import Issue, Issues, ShipmentConfig, Spec
from ghapi.all import GhApi

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
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
    ) -> None:
        self.runtime = runtime
        self.assembly = assembly
        self.release_name = None
        self.product = None
        self.group = group
        self.releases_config = None
        self._slack_client = slack_client
        self._issues_by_kind = None

        # product is needed for shipment and is specified in the group config
        # assume that the product is ocp for now
        self.product = 'ocp'

        # We want to have clear pull and push targets for both the build data repo and the shipment data repo
        # so that later on when we create PR/MR to update each of them, we know exactly where to push
        # and what the target repo/branches are.

        self.build_data_repo_pull_url = (
            build_repo_url
            or self.runtime.config.get("build_config", {}).get("ocp_build_data_url")
            or constants.OCP_BUILD_DATA_URL
        )
        self.build_data_gitref = None
        if "@" in self.build_data_repo_pull_url:
            self.build_data_repo_pull_url, self.build_data_gitref = self.build_data_repo_pull_url.split("@", 1)

        self.build_data_repo_push_url = (
            self.runtime.config.get("build_config", {}).get("ocp_build_data_push_url") or self.build_data_repo_pull_url
        )

        self.shipment_data_repo_pull_url = (
            shipment_repo_url
            or self.runtime.config.get("shipment_config", {}).get("shipment_data_url")
            or SHIPMENT_DATA_URL_TEMPLATE.format(self.product)
        )
        self.shipment_data_repo_push_url = (
            self.runtime.config.get("shipment_config", {}).get("shipment_data_push_url")
            or self.shipment_data_repo_pull_url
        )

        self.github_token = os.environ.get('GITHUB_TOKEN')
        if not self.github_token:
            raise ValueError("GITHUB_TOKEN environment variable is required to create a pull request")

        self.gitlab_token = os.environ.get("GITLAB_TOKEN")
        if not self.gitlab_token:
            raise ValueError("GITLAB_TOKEN environment variable is required to create a merge request")
        self.gitlab_url = self.runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        if self.gitlab_url not in self.shipment_data_repo_pull_url:
            raise ValueError(
                f"Invalid shipment data URL: {self.shipment_data_repo_pull_url} must be in {self.gitlab_url}"
            )
        if self.gitlab_url not in self.shipment_data_repo_push_url:
            raise ValueError(
                f"Invalid shipment data URL: {self.shipment_data_repo_push_url} must be in {self.gitlab_url}"
            )

        group_match = re.fullmatch(r"openshift-(\d+).(\d+)", self.group)
        if not group_match:
            raise ValueError(f"Invalid group name: {group}")
        self.release_version = (int(group_match[1]), int(group_match[2]), 0)
        self.application = KonfluxImageBuilder.get_application_name(self.group)

        if self.assembly == "stream":
            raise ValueError("Release cannot be prepared from stream assembly.")

        self.working_dir = self.runtime.working_dir.absolute()
        self.dry_run = self.runtime.dry_run
        self.elliott_working_dir = self.working_dir / "elliott-working"

        group_param = f'--group={group}'
        if self.build_data_gitref:
            group_param += f'@{self.build_data_gitref}'

        self._elliott_base_command = [
            'elliott',
            group_param,
            f'--assembly={self.assembly}',
            f'--build-system=konflux',
            f'--working-dir={self.elliott_working_dir}',
            f'--data-path={self.build_data_repo_pull_url}',
        ]
        self._build_repo_dir = self.working_dir / "ocp-build-data-push"
        self._shipment_repo_dir = self.working_dir / "shipment-data-push"

        self.job_url = os.getenv('BUILD_URL')

    async def run(self):
        self.working_dir.mkdir(parents=True, exist_ok=True)
        shutil.rmtree(self._build_repo_dir, ignore_errors=True)
        shutil.rmtree(self._shipment_repo_dir, ignore_errors=True)
        shutil.rmtree(self.elliott_working_dir, ignore_errors=True)

        group_config = await self._load_group_config()
        self.releases_config = await self._load_releases_config()
        if self.releases_config.get("releases", {}).get(self.assembly) is None:
            raise ValueError(f"Assembly not found: {self.assembly}")

        assembly_type = get_assembly_type(self.releases_config, self.assembly)

        if assembly_type == AssemblyTypes.STREAM:
            raise ValueError("Preparing a release from a stream assembly is no longer supported.")

        release_config = self.releases_config.get("releases", {}).get(self.assembly, {})
        if not release_config:
            raise ValueError(
                f"Assembly {self.assembly} is not explicitly defined in releases.yml for group {self.group}."
            )

        self.release_name = get_release_name_for_assembly(self.group, self.releases_config, self.assembly)
        self.release_version = semver.VersionInfo.parse(self.release_name).to_tuple()

        global_group_config = assembly_group_config(
            Model(self.releases_config), self.assembly, Model(group_config)
        ).primitive()

        # earlier we assumed the value of the product
        # assert here
        group_product = global_group_config.get("product", self.product)
        if group_product != self.product:
            raise ValueError(f"Product mismatch: {group_product} != {self.product}")

        await self.prepare_shipment()

    async def prepare_shipment(self):
        group_config = (
            self.releases_config["releases"][self.assembly].setdefault("assembly", {}).setdefault("group", {})
        )
        shipment_key = next(k for k in group_config.keys() if k.startswith("shipments"))
        shipments = group_config.get(shipment_key, []).copy()

        # restrict to only one shipment for now
        if len(shipments) != 1:
            raise ValueError("Operation not supported: shipments should have atleast and only one entry (for now)")

        shipment_config = shipments[0]
        shipment_url = shipment_config.get("url", "")
        shipment_advisories = shipment_config.get("advisories", [])
        if not shipment_advisories:
            raise ValueError(
                "Operation not supported: shipment config should specify which advisories to create and prepare"
            )

        group_advisories = set(group_config.get("advisories", {}).keys())
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
                    _LOGGER.warning("Dry run: Would've reserved liveID for %s advisory", kind)
                    live_id = "DRY_RUN_LIVE_ID"
                else:
                    live_id = await self._errata_api.reserve_live_id()
                if not live_id:
                    raise ValueError(f"Failed to get liveID for {kind} advisory")
                shipment_advisory_config["live_id"] = live_id
            if live_id:
                getattr(shipment.shipment.environments, env).liveID = live_id

            # find builds for the advisory
            if kind in ("image", "extras"):
                snapshot_spec = await self.find_builds(kind)
                shipment.shipment.snapshot.spec = snapshot_spec
            else:
                _LOGGER.warning("Shipment kind %s is not supported for build finding", kind)

            # find issues for the advisory
            issues = await self.find_issues(kind)
            shipment.shipment.data.releaseNotes.issues = issues

            generated_shipments[kind] = shipment

        if not shipment_url or shipment_url == "N/A":
            shipment_mr_url = await self.create_shipment_mr(generated_shipments, env)
            shipment_config["url"] = shipment_mr_url
            # await self._slack_client.say_in_thread(f"Shipment MR created: {shipment_mr_url}")
        else:
            _LOGGER.info("Shipment MR already exists: %s. Checking if it needs an update..", shipment_url)
            await self.update_shipment_mr(generated_shipments, env, shipment_url)

        if shipments != group_config.get(shipment_key):
            await self.create_update_build_data_pr(shipments)

    @cached_property
    def _errata_api(self):
        return AsyncErrataAPI()

    async def _load_group_config(self) -> Dict:
        repo = self._build_repo_dir
        if not repo.exists():
            await self.clone_build_data()
        async with aiofiles.open(repo / "group.yml", "r") as f:
            content = await f.read()
        return yaml.load(content)

    async def _load_releases_config(self) -> Dict:
        repo = self._build_repo_dir
        if not repo.exists():
            await self.clone_build_data()
        path = repo / "releases.yml"
        if not path.exists():
            return None
        async with aiofiles.open(path, "r") as f:
            content = await f.read()
        return yaml.load(content)

    async def init_shipment(self, kind: str) -> ShipmentConfig:
        create_cmd = self._elliott_base_command + [
            f'--shipment-path={self.shipment_data_repo_pull_url}',
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
        ]
        rc, stdout, stderr = await exectools.cmd_gather_async(find_builds_cmd)
        if stderr:
            _LOGGER.info("Shipment find-builds command stderr:\n %s", stderr)
        if stdout:
            _LOGGER.info("Shipment find-builds command stdout:\n %s", stdout)
        if rc != 0:
            raise RuntimeError(f"cmd failed with exit code {rc}: {find_builds_cmd}")

        # stdout will be all builds separated by new line
        builds = []
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            builds.append(line)

        return Spec(nvrs=builds)

    async def find_issues(self, kind: str) -> Issues:
        if self._issues_by_kind:
            return self._issues_by_kind.get(kind)

        find_bugs_cmd = self._elliott_base_command + [
            "find-bugs:sweep",
            "--output=json",
            "--permissive",  # TODO: this will be decided based on advisory type, logic in prepare-release
        ]
        rc, stdout, stderr = await exectools.cmd_gather_async(find_bugs_cmd)
        if stderr:
            _LOGGER.info("Shipment find bugs command stderr:\n %s", stderr)
        if stdout:
            _LOGGER.info("Shipment find bugs command stdout:\n %s", stdout)
        if rc != 0:
            raise RuntimeError(f"cmd failed with exit code {rc}: {find_bugs_cmd}")

        self._issues_by_kind = {}
        # in case of no bugs, stdout will be empty
        if stdout:
            for advisory_kind, bugs in json.loads(stdout).items():
                if not bugs:
                    continue
                issues = Issues(fixed=[Issue(id=b, source="issues.redhat.com") for b in bugs])
                self._issues_by_kind[advisory_kind] = issues

        return self._issues_by_kind.get(kind)

    async def update_shipment_data(
        self, shipments: Dict[str, ShipmentConfig], env: str, commit_message: str, branch: str
    ) -> bool:
        relative_target_dir = Path("shipment") / self.product / self.group / self.application / env
        target_dir = self._shipment_repo_dir / relative_target_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        added_files = []

        # Get the timestamp from the branch name
        # which we need for filenames
        # The branch name is expected to be in the format: prepare-shipment-<assembly>-<timestamp>
        timestamp = branch.split("-")[-1]

        for advisory_key, shipment_config in shipments.items():
            filename = f"{self.assembly}.{advisory_key}.{timestamp}.yaml"
            filepath = target_dir / filename
            _LOGGER.info("Updating shipment file: %s", filepath)
            out = StringIO()
            yaml.dump(shipment_config.model_dump(exclude_unset=True, exclude_none=True), out)
            async with aiofiles.open(filepath, "w") as f:
                await f.write(out.getvalue())
            # Use relative path for git add
            added_files.append(str(filepath.relative_to(self._shipment_repo_dir)))

        # Dump diff to stdout
        await run_git_async(["-C", str(self._shipment_repo_dir), "--no-pager", "diff"])

        # Add files
        await run_git_async(["-C", str(self._shipment_repo_dir), "add"] + added_files)

        # Make sure there are changes to commit
        rc, _, _ = await gather_git_async(
            ["-C", str(self._shipment_repo_dir), "diff-index", "--quiet", "HEAD"], check=False
        )
        if rc == 0:
            _LOGGER.info("No changes in shipment data")
            return False

        await run_git_async(["-C", str(self._shipment_repo_dir), "commit", "-m", commit_message])

        # Push branch
        if not self.dry_run:
            _LOGGER.info("Pushing branch %s upstream...", branch)
            await run_git_async(["-C", str(self._shipment_repo_dir), "push", "-u", "push", branch])
        else:
            _LOGGER.info("[DRY-RUN] Would have pushed branch %s", branch)
        return True

    async def update_shipment_mr(self, shipments: Dict[str, Dict], env: str, shipment_url: str) -> bool:
        _LOGGER.info("Updating shipment MR: %s", shipment_url)

        # Clone the shipment data repository if not already cloned
        await self.clone_shipment_data()

        # Parse the shipment URL to extract project and MR details
        parsed_url = urlparse(shipment_url)
        project_path = parsed_url.path.strip('/').split('/-/merge_requests')[0]
        mr_id = parsed_url.path.split('/')[-1]

        # Load the existing MR
        gl = gitlab.Gitlab(self.gitlab_url, private_token=self.gitlab_token)
        gl.auth()
        project = gl.projects.get(project_path)
        mr = project.mergerequests.get(mr_id)

        # Checkout the MR branch
        source_branch = mr.source_branch
        await run_git_async(["-C", str(self._shipment_repo_dir), "fetch", "origin", f"{source_branch}:{source_branch}"])
        await run_git_async(["-C", str(self._shipment_repo_dir), "checkout", source_branch])

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
        await self.clone_shipment_data()

        # Create branch name
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        source_branch = f"prepare-shipment-{self.assembly}-{timestamp}"
        target_branch = "main"

        # Create and checkout branch
        await run_git_async(["-C", str(self._shipment_repo_dir), "checkout", "-b", source_branch])

        # update shipment data repo with shipment configs
        commit_message = f"Add shipment configurations for {self.release_name}"
        updated = await self.update_shipment_data(shipment_configs, env, commit_message, source_branch)
        if not updated:
            # this should not happen
            raise ValueError("Failed to update shipment data repo. Please investigate.")

        gl = gitlab.Gitlab(self.gitlab_url, private_token=self.gitlab_token)
        gl.auth()

        def _get_project(url):
            parsed_url = urlparse(url)
            project_path = parsed_url.path.strip('/').removesuffix('.git')
            return gl.projects.get(project_path)

        source_project = _get_project(self.shipment_data_repo_push_url)
        target_project = _get_project(self.shipment_data_repo_pull_url)

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

    async def clone_repo(self, local_path: Path, repo_url: str, branch: str):
        args = [
            "-C",
            str(self.working_dir),
            "clone",
            "-b",
            branch,
            "--depth=1",
            repo_url,
            str(local_path),
        ]
        await git_helper.run_git_async(args)

    async def clone_build_data(self):
        await self.clone_repo(self._build_repo_dir, self.build_data_repo_pull_url, self.build_data_gitref or self.group)

        # setup push remote
        push_url = convert_remote_git_to_ssh(self.build_data_repo_push_url)
        await run_git_async(["-C", str(self._build_repo_dir), "remote", "add", "push", push_url])

    async def clone_shipment_data(self):
        # this assumes that repo is publicly accessible
        await self.clone_repo(self._shipment_repo_dir, self.shipment_data_repo_pull_url, "main")

        # setup push remote
        parsed_url = urlparse(self.shipment_data_repo_push_url)
        scheme = parsed_url.scheme
        rest_of_the_url = self.shipment_data_repo_pull_url[len(scheme + "://") :]
        push_url = f'https://oauth2:{self.gitlab_token}@{rest_of_the_url}'
        await run_git_async(["-C", str(self._shipment_repo_dir), "remote", "add", "push", push_url])

    async def update_build_data(self, group_shipment_config: List[Dict], branch: str) -> bool:
        repo = self._build_repo_dir

        # Check if branch doesn't already exist upstream
        # if it does, assume that it is updated
        check_branch_cmd = ["-C", str(repo), "ls-remote", "--heads", "push", branch]
        _, stdout, _ = await gather_git_async(check_branch_cmd)
        branch_exists = branch in stdout
        if branch_exists:
            _LOGGER.info("Branch %s already exists upstream. Skipping push.", branch)
            return False

        group_config = (
            self.releases_config["releases"][self.assembly].setdefault("assembly", {}).setdefault("group", {})
        )

        # Assembly key names are not always exact, they can end in special chars like !,?,-
        # to indicate special inheritance rules. So respect those
        # https://art-docs.engineering.redhat.com/assemblies/#inheritance-rules
        shipment_key = next(k for k in group_config.keys() if k.startswith("shipments"))
        group_config[shipment_key] = group_shipment_config

        await run_git_async(["-C", str(repo), "checkout", "-b", branch])

        out = StringIO()
        yaml.dump(self.releases_config, out)
        async with aiofiles.open(repo / "releases.yml", "w") as f:
            await f.write(out.getvalue())

        # Dump diff to stdout
        await run_git_async(["-C", str(repo), "--no-pager", "diff"])

        # Add release config to git
        await run_git_async(["-C", str(repo), "add", "releases.yml"])

        # Make sure there are changes to commit
        rc, _, _ = await gather_git_async(["-C", str(repo), "diff-index", "--quiet", "HEAD"], check=False)
        if rc == 0:
            _LOGGER.info("No changes in releases.yml")
            return False

        # Commit changes
        await run_git_async(["-C", str(repo), "commit", "-m", f"Update shipment for assembly {self.assembly}"])

        # Push changes to a new branch
        cmd = ["-C", str(repo), "push", "-u", "push", branch]
        if self.dry_run:
            _LOGGER.info("Would have created new branch upstream: %s", " ".join(cmd))
        else:
            _LOGGER.info("Pushing changes to upstream...")
            await run_git_async(cmd)

        return True

    async def create_update_build_data_pr(self, shipments) -> bool:
        branch = f"update-shipment-{self.release_name}"
        updated = await self.update_build_data(shipments, branch)

        api = GhApi()
        target_repo = self.build_data_repo_pull_url.split('/')[-1].replace('.git', '')
        source_owner = self.build_data_repo_push_url.split('/')[-2]
        target_owner = self.build_data_repo_pull_url.split('/')[-2]

        head = f"{source_owner}:{branch}"
        base = self.build_data_gitref or self.group
        api = GhApi(owner=target_owner, repo=target_repo, token=self.github_token)
        existing_prs = api.pulls.list(
            state="open",
            head=head,
            base=base,
        )
        if not existing_prs.items:
            if not updated:
                raise ValueError("Failed to update build data repo. Please investigate.")

            pr_title = f"Update shipment for assembly {self.assembly}"
            pr_body = f"This PR updates the shipment data for assembly {self.assembly}."
            if self.job_url:
                pr_body += f"\n\nCreated by job: {self.job_url}"

            if self.dry_run:
                _LOGGER.info("Dry run: Would have created a new PR with title '%s'", pr_title)
            else:
                result = api.pulls.create(
                    head=head,
                    base=base,
                    title=pr_title,
                    body=pr_body,
                    maintainer_can_modify=True,
                )
                _LOGGER.info("PR to update shipments created: %s", result.html_url)
                # await self._slack_client.say_in_thread(f"PR to update shipments created: {result.html_url}")
        else:
            _LOGGER.info("Existing PR to update shipments found: %s", existing_prs.items[0].html_url)
            pull_number = existing_prs.items[0].number
            if self.dry_run:
                _LOGGER.info("Dry run: Would have updated PR with number %s", pull_number)
            elif updated:
                pr_body = existing_prs.items[0].body
                if self.job_url:
                    pr_body += f"\n\nUpdated by job: {self.job_url}"
                result = api.pulls.update(
                    pull_number=pull_number,
                    body=pr_body,
                )
                _LOGGER.info("PR to update shipments updated: %s", result.html_url)
                # await self._slack_client.say_in_thread(f"PR to update shipments updated: {result.html_url}")

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
    # slack_client = runtime.new_slack_client()
    # slack_client.bind_channel(group)
    # await slack_client.say_in_thread(f":construction: prepare-release-konflux for {assembly} :construction:")

    try:
        # start pipeline
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client='',  # slack_client,
            runtime=runtime,
            group=group,
            assembly=assembly,
            build_repo_url=build_repo_url,
            shipment_repo_url=shipment_repo_url,
        )
        await pipeline.run()
        # await slack_client.say_in_thread(f":white_check_mark: prepare-release-konflux for {assembly} completes.")
    except Exception as e:
        # await slack_client.say_in_thread(f":warning: prepare-release-konflux for {assembly} has result FAILURE.")
        raise e  # return failed status to jenkins
