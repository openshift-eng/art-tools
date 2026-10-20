import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import click
from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib.build_visibility import is_nvr_embargoed
from artcommonlib.gitlab import GitLabClient
from gitlab.exceptions import GitlabGetError

from pyartcd import constants, oc, util
from pyartcd import record as record_util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.pipelines.binary_release_konflux import BinaryReleaseKonfluxPipeline
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)

DELIVERY_IMAGE = "openshift-install-delivery"
PAYLOAD_PARENTS = {"inst": "installer", "art": "installer-artifacts", "bm": "baremetal-installer"}
KRD_URL = "https://gitlab.cee.redhat.com"
KRD_PROJECT = "releng/konflux-release-data"
KRD_RPA_DIR = "config/kflux-ocp-p01.7ayg.p1/product/ReleasePlanAdmission/ocp-art"
KRD_CGW_VERSION_DIR = "data/external/developer-portal/openshift-install"
FROM_LINE_RE = re.compile(r"^FROM\s+--platform=linux/(\S+)\s+\S+@(sha256:[0-9a-f]{64})\s+AS\s+(inst|art|bm)_(\S+)\s*$")
PRODUCT_VERSION_RE = re.compile(r'^([ \t]*productVersionName:[ \t]*)"([^"]*)"[ \t]*$', re.MULTILINE)


@dataclass(frozen=True)
class ClientTypeConfig:
    shipment_kind: str
    rpa_prefix: str
    ga: bool


CLIENT_TYPES = {
    "ocp": ClientTypeConfig(shipment_kind="cdn", rpa_prefix="ocp-art-cdn", ga=True),
    "ocp-dev-preview": ClientTypeConfig(
        shipment_kind="cdn-dev-preview", rpa_prefix="ocp-art-cdn-dev-preview", ga=False
    ),
}


def cgw_version_file_content(release_name: str, ga: bool, release_date: str) -> str:
    """
    Return the developer-portal product version file for one release.

    :param release_name: Release name, e.g. 4.22.18; becomes the CGW versionName.
    :param ga: True for GA (ocp) releases, False for dev-preview releases.
    :param release_date: Date in YYYY-MM-DD format.
    """
    return (
        "---\n"
        f'versionName: "{release_name}"\n'
        f"ga: {'true' if ga else 'false'}\n"
        'termsAndConditions: "Anonymous Download"\n'
        "hidden: false\n"
        "invisible: false\n"
        f'releaseDate: "{release_date}"\n'
    )


def rpa_product_version_name(rpa_text: str) -> str:
    """
    Return the single quoted productVersionName value of an RPA.

    :param rpa_text: ReleasePlanAdmission YAML text.
    """
    matches = PRODUCT_VERSION_RE.findall(rpa_text)
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one quoted productVersionName in the RPA, found {len(matches)}")
    return matches[0][1]


def set_rpa_product_version_name(rpa_text: str, release_name: str) -> str:
    """
    Return rpa_text with its single productVersionName set to release_name.

    :param rpa_text: ReleasePlanAdmission YAML text.
    :param release_name: New productVersionName value.
    """
    rpa_product_version_name(rpa_text)
    return PRODUCT_VERSION_RE.sub(lambda m: f'{m.group(1)}"{release_name}"', rpa_text)


def parse_delivery_dockerfile_parents(dockerfile_text: str) -> Dict[str, Dict[str, str]]:
    """
    Return {brew_arch: {payload_tag: digest}} from the FROM --platform lines of a rebased delivery Dockerfile.

    :param dockerfile_text: Content of the rebased Dockerfile.
    """
    parents: Dict[str, Dict[str, str]] = {}
    for line in dockerfile_text.splitlines():
        match = FROM_LINE_RE.match(line.strip())
        if not match:
            continue
        platform_arch, digest, prefix, stage_arch = match.groups()
        if platform_arch != stage_arch:
            raise ValueError(f"Stage {prefix}_{stage_arch} is built for linux/{platform_arch}: {line}")
        parents.setdefault(brew_arch_for_go_arch(platform_arch), {})[PAYLOAD_PARENTS[prefix]] = digest
    return parents


class InstallerCdnReleasePipeline:
    """
    Build the openshift-install delivery image for one promoted OCP release and ship it to the Konflux CDN.

    The release values injected into the binaries (release digests per arch and multi, release name) are read
    from the public release payloads, passed to the Konflux build as build args, and the parent images used by
    the build are checked against the payload before anything is released.
    """

    def __init__(
        self,
        runtime: Runtime,
        version: str,
        assembly: str,
        release_name: str,
        client_type: str,
        data_path: str = constants.OCP_BUILD_DATA_URL,
        data_gitref: str = "",
        nvr: Optional[str] = None,
        skip_cgw_mr: bool = False,
        cgw_mr_timeout_hours: float = 24.0,
        poll_seconds: int = 300,
        build_only: bool = False,
    ):
        if client_type not in CLIENT_TYPES:
            raise ValueError(f"Unsupported client type {client_type!r}; expected one of {sorted(CLIENT_TYPES)}")
        if not re.fullmatch(r"\d+\.\d+", version):
            raise ValueError(f"Version must be MAJOR.MINOR, got {version!r}")
        self.runtime = runtime
        self.version = version
        self.major, self.minor = version.split(".")
        self.group = f"openshift-{version}"
        self.assembly = assembly
        self.release_name = release_name
        self.client_type = client_type
        self.client_config = CLIENT_TYPES[client_type]
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.nvr = nvr
        self.skip_cgw_mr = skip_cgw_mr
        self.build_only = build_only
        self.cgw_mr_timeout_seconds = int(cgw_mr_timeout_hours * 3600)
        self.poll_seconds = poll_seconds
        self.dry_run = runtime.dry_run
        self.registry_config = os.environ.get("QUAY_AUTH_FILE")
        self.release_plan_key = f"openshift-{self.major}-{self.minor}-{self.client_config.shipment_kind}"
        self.rpa_names = [
            f"{self.client_config.rpa_prefix}-{env}-{self.major}-{self.minor}" for env in ("stage", "prod")
        ]
        group_param = f"--group={self.group}" + (f"@{data_gitref}" if data_gitref else "")
        self._doozer_base_command = [
            "doozer",
            f"--assembly={assembly}",
            f"--working-dir={runtime.doozer_working}",
            f"--data-path={data_path}",
            "--build-system=konflux",
            group_param,
            "--variant=ocp",
        ]

    async def delivery_image_configured(self) -> bool:
        cmd = self._doozer_base_command + ["--load-disabled", f"--images={DELIVERY_IMAGE}", "images:list", "--json"]
        rc, out, err = await exectools.cmd_gather_async(cmd, check=False)
        if rc == 0:
            return DELIVERY_IMAGE in json.loads(out.strip().splitlines()[-1]).get("images", [])
        if "were either missing or filtered out" in err:
            return False
        raise RuntimeError(f"Failed to list {DELIVERY_IMAGE} in {self.group}: {err}")

    async def collect_release_values(self, arches: List[str], multi: bool):
        """
        Read release digests and payload parent digests for every release flavor of this release.

        :param arches: Brew arches of the group, e.g. ["x86_64", "aarch64"].
        :param multi: True when the group publishes a multi payload.
        :return: (build_args, expected_parents) where expected_parents is {brew_arch: {payload_tag: digest}}.
        """
        build_args = [f"RELEASE_VERSION={self.release_name}"]
        expected_parents: Dict[str, Dict[str, str]] = {}
        for arch in arches:
            pullspec = f"{constants.RELEASE_IMAGE_REPO}:{self.release_name}-{arch}"
            info = await oc.get_release_image_info(
                pullspec, raise_if_not_found=True, registry_config=self.registry_config
            )
            version = info.get("metadata", {}).get("version")
            if version != self.release_name:
                raise ValueError(f"{pullspec} has release version {version!r}, expected {self.release_name!r}")
            build_args.append(f"DIGEST_{arch.upper()}={info['digest']}")
            tags = {t["name"]: t["from"]["name"] for t in info["references"]["spec"]["tags"]}
            expected_parents[arch] = {}
            for tag in PAYLOAD_PARENTS.values():
                if tag not in tags:
                    raise ValueError(f"{pullspec} has no {tag} image")
                expected_parents[arch][tag] = tags[tag].split("@", 1)[1]
        if multi:
            pullspec = f"{constants.RELEASE_IMAGE_REPO}:{self.release_name}-multi"
            info = await oc.get_image_info(pullspec, raise_if_not_found=True, registry_config=self.registry_config)
            if not isinstance(info, list) or not info or not info[0].get("listDigest"):
                raise ValueError(f"{pullspec} is not a manifest list")
            build_args.append(f"DIGEST_MULTI={info[0]['listDigest']}")
        return build_args, expected_parents

    async def build_delivery_image(self, build_args: List[str]) -> dict:
        """
        Rebase and build the delivery image with the release values as build args; return its record.log entry.

        :param build_args: KEY=VALUE build args for the Konflux build.
        """
        base = self._doozer_base_command + ["--load-disabled", "--latest-parent-version", f"--images={DELIVERY_IMAGE}"]
        version = f"v{self.version}.0"
        release = util.default_release_suffix()
        rebase = base + ["beta:images:konflux:rebase", f"--version={version}", f"--release={release}"]
        rebase.append(f"--message=Updating Dockerfile version and release {version}-{release} for {self.release_name}")
        if not self.dry_run:
            rebase.append("--push")
        await exectools.cmd_assert_async(rebase)

        build = base + ["beta:images:konflux:build", "--konflux-namespace=ocp-art-tenant"]
        kubeconfig = os.environ.get("KONFLUX_SA_KUBECONFIG")
        if kubeconfig:
            build.extend(["--konflux-kubeconfig", kubeconfig])
        for arg in build_args:
            build.extend(["--build-arg", arg])
        if self.dry_run:
            build.append("--dry-run")
        await exectools.cmd_assert_async(build)

        record_log_path = Path(self.runtime.doozer_working, "record.log")
        with record_log_path.open() as f:
            record_log = record_util.parse_record_log(f)
        entries = [e for e in record_log.get("image_build_konflux", []) if e.get("name") == DELIVERY_IMAGE]
        if not entries or entries[-1].get("status") != "0":
            raise RuntimeError(f"{DELIVERY_IMAGE} build failed; see {record_log_path}")
        return entries[-1]

    @staticmethod
    def verify_parents(dockerfile_text: str, expected_parents: Dict[str, Dict[str, str]]):
        """
        Fail unless the build used exactly the installer, installer-artifacts and baremetal-installer of the payload.

        :param dockerfile_text: Content of the rebased delivery Dockerfile.
        :param expected_parents: {brew_arch: {payload_tag: digest}} from the release payloads.
        """
        actual = parse_delivery_dockerfile_parents(dockerfile_text)
        if actual != expected_parents:
            raise RuntimeError(
                f"Delivery image parents do not match the release payload.\nexpected: {expected_parents}\nactual: {actual}"
            )

    def _gitlab(self) -> GitLabClient:
        return GitLabClient(KRD_URL, os.environ["GITLAB_TOKEN"], self.dry_run)

    @staticmethod
    def _read_file(project, path: str) -> Optional[str]:
        try:
            return project.files.get(file_path=path, ref="main").decode().decode("utf-8")
        except GitlabGetError as err:
            if err.response_code == 404:
                return None
            raise

    async def ensure_cgw_version(self):
        gitlab_client = self._gitlab()
        project = gitlab_client.get_project(KRD_PROJECT)
        version_path = f"{KRD_CGW_VERSION_DIR}/{self.release_name}.yaml"
        rpa_paths = [f"{KRD_RPA_DIR}/{name}.yaml" for name in self.rpa_names]
        rpa_texts = {path: self._read_file(project, path) for path in rpa_paths}
        missing = [path for path, text in rpa_texts.items() if text is None]
        if missing:
            raise RuntimeError(f"{self.group} is not onboarded: missing {', '.join(missing)} in {KRD_PROJECT}")
        version_text = self._read_file(project, version_path)
        if version_text is not None and all(
            rpa_product_version_name(text) == self.release_name for text in rpa_texts.values()
        ):
            LOGGER.info("CGW version %s and RPAs %s are already on main", self.release_name, self.rpa_names)
            return

        branch_prefix = f"installer-cdn-{self.major}-{self.minor}-{self.client_type}-"
        branch = f"{branch_prefix}{self.release_name}"
        open_mrs = gitlab_client.list_merge_requests(KRD_PROJECT, state="opened", project=project, target_branch="main")
        ours = [mr for mr in open_mrs if mr.source_branch.startswith(branch_prefix)]
        others = [mr.web_url for mr in ours if mr.source_branch != branch]
        if others:
            raise RuntimeError(
                f"Another openshift-install {self.client_type} CGW MR for {self.version} is still open: "
                f"{', '.join(others)}. Merge or close it (and let its CDN release finish) before {self.release_name}."
            )
        existing = [mr for mr in ours if mr.source_branch == branch]
        if existing:
            mr = existing[0]
            LOGGER.info("Reusing open MR %s", mr.web_url)
        else:
            actions = []
            if version_text is None:
                release_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                actions.append(
                    {
                        "action": "create",
                        "file_path": version_path,
                        "content": cgw_version_file_content(self.release_name, self.client_config.ga, release_date),
                    }
                )
            for path, text in rpa_texts.items():
                if rpa_product_version_name(text) != self.release_name:
                    actions.append(
                        {
                            "action": "update",
                            "file_path": path,
                            "content": set_rpa_product_version_name(text, self.release_name),
                        }
                    )
            title = f"openshift-install {self.release_name}: CGW version and {self.client_config.rpa_prefix} RPAs"
            if self.dry_run:
                LOGGER.info("[DRY-RUN] Would create branch %s and MR %r with actions %s", branch, title, actions)
                return
            project.commits.create(
                {
                    "branch": branch,
                    "start_branch": "main",
                    "commit_message": f"{title}\n\nCreated by {os.environ.get('BUILD_URL', 'installer-cdn-release')}",
                    "actions": actions,
                }
            )
            mr = project.mergerequests.create(
                {
                    "source_branch": branch,
                    "target_branch": "main",
                    "title": title,
                    "description": (
                        f"Adds CGW product version `{self.release_name}` for `openshift-install` and points "
                        f"{', '.join(self.rpa_names)} at it, so the Konflux CDN release of {self.release_name} "
                        "publishes to its own version directory. Created by the installer-cdn-release job: "
                        f"{os.environ.get('BUILD_URL', 'n/a')}"
                    ),
                    "remove_source_branch": True,
                }
            )
            LOGGER.info("Created MR %s", mr.web_url)
        await self._wait_for_merge_and_sync(project, mr)

    async def _wait_for_merge_and_sync(self, project, mr):
        waited = 0
        sha = None
        while waited <= self.cgw_mr_timeout_seconds:
            mr = project.mergerequests.get(mr.iid)
            if mr.state == "closed":
                raise RuntimeError(f"MR {mr.web_url} was closed without merging")
            if mr.state == "merged":
                sha = mr.merge_commit_sha or mr.squash_commit_sha or mr.sha
                break
            LOGGER.info("Waiting for %s to be merged (state: %s)", mr.web_url, mr.state)
            await asyncio.sleep(self.poll_seconds)
            waited += self.poll_seconds
        if not sha:
            raise TimeoutError(f"MR {mr.web_url} was not merged within {self.cgw_mr_timeout_seconds}s; re-run later")
        while waited <= self.cgw_mr_timeout_seconds:
            for pipeline in project.pipelines.list(ref="main", sha=sha, get_all=True):
                for job in pipeline.jobs.list(get_all=True):
                    if job.name != "sync_cgw":
                        continue
                    if job.status == "success":
                        LOGGER.info("sync_cgw succeeded for %s", sha)
                        return
                    if job.status in ("failed", "canceled"):
                        raise RuntimeError(f"sync_cgw {job.status} for {sha}: {job.web_url}")
            LOGGER.info("Waiting for sync_cgw on main at %s", sha)
            await asyncio.sleep(self.poll_seconds)
            waited += self.poll_seconds
        raise TimeoutError(f"sync_cgw did not succeed for {sha} within {self.cgw_mr_timeout_seconds}s")

    async def run(self):
        if not await self.delivery_image_configured():
            LOGGER.warning("%s has no %s image; nothing to do", self.group, DELIVERY_IMAGE)
            return

        group_config = await util.load_group_config(
            self.group, self.assembly, doozer_data_path=self.data_path, doozer_data_gitref=self.data_gitref
        )
        arches = list(group_config.get("arches", []))
        multi = bool(group_config.get("multi_arch", {}).get("enabled", False))
        build_args, expected_parents = await self.collect_release_values(arches, multi)
        LOGGER.info("Release values for %s: %s", self.release_name, build_args)

        nvr = self.nvr
        if not nvr:
            entry = await self.build_delivery_image(build_args)
            if not self.dry_run:
                self.verify_parents(Path(entry["dockerfile"]).read_text(), expected_parents)
            nvr = entry.get("nvrs", "").split(",")[0]
            if self.dry_run and (not nvr or nvr == "n/a"):
                LOGGER.info("[DRY-RUN] No NVR built; stopping before the release steps")
                return
        LOGGER.info("Delivery image NVR: %s", nvr)
        if self.build_only:
            LOGGER.info("--build-only: not releasing %s", nvr)
            return

        if is_nvr_embargoed(nvr):
            LOGGER.warning(
                "%s is embargoed; not releasing it to the CDN (the mirror.openshift.com path is unaffected)", nvr
            )
            return

        if not self.skip_cgw_mr:
            await self.ensure_cgw_version()

        pipeline = BinaryReleaseKonfluxPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            nvrs=[nvr],
            create_mr=True,
            release_plan_key=self.release_plan_key,
            shipment_kind=self.client_config.shipment_kind,
        )
        await pipeline.run()


@cli.command("installer-cdn-release")
@click.option("--version", "version", required=True, help="OCP MAJOR.MINOR, e.g. 4.22")
@click.option("--assembly", required=True, help="Assembly that was promoted, e.g. 4.22.18")
@click.option("--release-name", required=True, help="Promoted release name, e.g. 4.22.18 or 4.23.0-ec.1")
@click.option(
    "--client-type",
    required=True,
    type=click.Choice(sorted(CLIENT_TYPES)),
    help="ocp for GA releases, ocp-dev-preview otherwise (same rule as promote)",
)
@click.option("--data-path", default=constants.OCP_BUILD_DATA_URL, help="ocp-build-data repo to use")
@click.option("--data-gitref", default="", help="Git ref of ocp-build-data to use")
@click.option("--nvr", default=None, help="Release this delivery image NVR instead of building a new one")
@click.option("--skip-cgw-mr", is_flag=True, help="Do not create or wait for the konflux-release-data MR")
@click.option("--cgw-mr-timeout-hours", type=float, default=24.0, help="How long to wait for the MR and sync_cgw")
@click.option(
    "--build-only", is_flag=True, help="Build and verify the delivery image, then stop before any release step"
)
@pass_runtime
@click_coroutine
async def installer_cdn_release(
    runtime: Runtime,
    version: str,
    assembly: str,
    release_name: str,
    client_type: str,
    data_path: str,
    data_gitref: str,
    nvr: Optional[str],
    skip_cgw_mr: bool,
    cgw_mr_timeout_hours: float,
    build_only: bool,
):
    """
    Build the openshift-install delivery image for a promoted release and ship it through Konflux push-artifacts-to-cdn.

    \b
    $ artcd installer-cdn-release --version 4.22 --assembly 4.22.18 --release-name 4.22.18 --client-type ocp
    """
    pipeline = InstallerCdnReleasePipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        release_name=release_name,
        client_type=client_type,
        data_path=data_path,
        data_gitref=data_gitref,
        nvr=nvr,
        skip_cgw_mr=skip_cgw_mr,
        cgw_mr_timeout_hours=cgw_mr_timeout_hours,
        build_only=build_only,
    )
    await pipeline.run()
