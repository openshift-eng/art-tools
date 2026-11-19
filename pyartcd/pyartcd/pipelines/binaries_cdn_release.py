import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import click
from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib.build_visibility import is_nvr_embargoed
from artcommonlib.gitlab import GitLabClient
from gitlab.exceptions import GitlabGetError
from semver import VersionInfo

from pyartcd import constants, oc, util
from pyartcd import record as record_util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.pipelines.binary_release_konflux import BinaryReleaseKonfluxPipeline
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)

KRD_URL = "https://gitlab.cee.redhat.com"
KRD_PROJECT = "releng/konflux-release-data"
KRD_RPA_DIR = "config/kflux-ocp-p01.7ayg.p1/product/ReleasePlanAdmission/ocp-art"
KRD_CGW_DIR = "data/external/developer-portal"
FROM_LINE_RE = re.compile(r"^FROM\s+--platform=linux/(\S+)\s+\S+@(sha256:[0-9a-f]{64})\s+AS\s+([a-z]+)_(\S+)\s*$")
PRODUCT_VERSION_RE = re.compile(r'^[ \t]*productVersionName:[ \t]*"([^"]*)"[ \t]*$', re.MULTILINE)


@dataclass(frozen=True)
class ClientTypeConfig:
    shipment_kind: str
    rpa_prefix: str


@dataclass(frozen=True)
class BinaryProfile:
    """
    What the job needs to know about one binaries/ member.

    payload_parents maps the Dockerfile stage prefix (<prefix>_<goarch>) to the release payload tag whose image
    the stage must be built from. cgw_product is the developer-portal product directory in konflux-release-data.
    """

    payload_parents: Dict[str, str]
    cgw_product: str


BINARIES = {
    "openshift-install": BinaryProfile(
        payload_parents={"inst": "installer", "art": "installer-artifacts", "bm": "baremetal-installer"},
        cgw_product="openshift-install",
    ),
}


CLIENT_TYPES = {
    "ocp": ClientTypeConfig(shipment_kind="binaries", rpa_prefix="ocp-art-binaries"),
    "ocp-dev-preview": ClientTypeConfig(
        shipment_kind="binaries-dev-preview", rpa_prefix="ocp-art-binaries-dev-preview"
    ),
}


def binaries_cdn_enabled(group_config: dict) -> bool:
    """
    Return True when group.yml (or the assembly) sets binaries.cdn.enabled: true. Missing means off.

    :param group_config: Group config, assembly overrides applied.
    """
    return bool(((group_config.get("binaries") or {}).get("cdn") or {}).get("enabled", False))


def cgw_version_name(version: str, client_type: str) -> str:
    """
    Return the CGW product version that every release of version and client_type publishes into.

    Dev-preview shares MAJOR.MINOR with GA because the dev-preview RPAs on konflux-release-data say MAJOR.MINOR too.

    :param version: OCP MAJOR.MINOR, e.g. 4.22.
    :param client_type: ocp or ocp-dev-preview.
    """
    return version


def rpa_product_version_names(rpa_text: str) -> List[str]:
    """
    Return every quoted productVersionName value of an RPA.

    :param rpa_text: ReleasePlanAdmission YAML text.
    """
    return PRODUCT_VERSION_RE.findall(rpa_text)


def parse_binary_dockerfile_parents(dockerfile_text: str, payload_parents: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    """
    Return {brew_arch: {payload_tag: digest}} from the FROM --platform lines of a rebased binaries Dockerfile.

    :param dockerfile_text: Content of the rebased Dockerfile.
    :param payload_parents: {stage prefix: payload tag} of the binary, see BinaryProfile.
    """
    parents: Dict[str, Dict[str, str]] = {}
    for line in dockerfile_text.splitlines():
        match = FROM_LINE_RE.match(line.strip())
        if not match or match.group(3) not in payload_parents:
            continue
        platform_arch, digest, prefix, stage_arch = match.groups()
        if platform_arch != stage_arch:
            raise ValueError(f"Stage {prefix}_{stage_arch} is built for linux/{platform_arch}: {line}")
        parents.setdefault(brew_arch_for_go_arch(platform_arch), {})[payload_parents[prefix]] = digest
    return parents


class BinariesCdnReleasePipeline:
    """
    Build one binaries/ member (for example openshift-install) for one promoted OCP release and ship it to the
    Konflux CDN.

    The release values injected into the binaries (release digests per arch and multi, release name) are read
    from the public release payloads, passed to the Konflux build as build args, and the parent images used by
    the build are checked against the payload before anything is released.

    There is one CGW product version per MAJOR.MINOR (for example 4.22), like oc-mirror's 2.0. Every z-stream
    publishes into it and CGW replaces the previous release's files in place, so /pub/cgw/<product>/<version>/
    always holds the latest release (mirror.openshift.com keeps the history). The version file and the RPAs that
    point at it are added to konflux-release-data by hand once per MAJOR.MINOR; this job only checks that they are
    on main before it ships.
    """

    def __init__(
        self,
        runtime: Runtime,
        binary: str,
        version: str,
        assembly: str,
        release_name: str,
        client_type: str,
        data_path: str = constants.OCP_BUILD_DATA_URL,
        data_gitref: str = "",
        nvr: Optional[str] = None,
        build_only: bool = False,
    ):
        if binary not in BINARIES:
            raise ValueError(f"Unsupported binary {binary!r}; expected one of {sorted(BINARIES)}")
        if client_type not in CLIENT_TYPES:
            raise ValueError(f"Unsupported client type {client_type!r}; expected one of {sorted(CLIENT_TYPES)}")
        if not re.fullmatch(r"\d+\.\d+", version):
            raise ValueError(f"Version must be MAJOR.MINOR, got {version!r}")
        if not release_name.startswith(f"{version}.") or not VersionInfo.is_valid(release_name):
            raise ValueError(f"Release name must be a {version}.Z semver release name, got {release_name!r}")
        self.runtime = runtime
        self.binary = binary
        self.profile = BINARIES[binary]
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
        self.build_only = build_only
        self.dry_run = runtime.dry_run
        self.registry_config = os.environ.get("QUAY_AUTH_FILE")
        self.release_plan_key = f"openshift-{self.major}-{self.minor}-{self.client_config.shipment_kind}"
        self.rpa_names = [
            f"{self.client_config.rpa_prefix}-{env}-{self.major}-{self.minor}" for env in ("stage", "prod")
        ]
        self.cgw_version = cgw_version_name(version, client_type)
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

    async def binary_configured(self) -> bool:
        cmd = self._doozer_base_command + [f"--binaries={self.binary}", "binaries:list", "--json"]
        rc, out, err = await exectools.cmd_gather_async(cmd, check=False)
        if rc == 0:
            return self.binary in json.loads(out.strip().splitlines()[-1]).get("binaries", [])
        if "were either missing or filtered out" in err:
            return False
        raise RuntimeError(f"Failed to list binaries/{self.binary} in {self.group}: {err}")

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
            for tag in self.profile.payload_parents.values():
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

    async def build_binary(self, build_args: List[str]) -> dict:
        """
        Rebase and build the binary with the release values as build args; return its record.log entry.

        :param build_args: KEY=VALUE build args for the Konflux build.
        """
        base = self._doozer_base_command + ["--latest-parent-version", f"--binaries={self.binary}"]
        version = f"v{self.version}.0"
        release = util.default_release_suffix()
        rebase = base + ["beta:binaries:konflux:rebase", f"--version={version}", f"--release={release}"]
        rebase.append(f"--message=Updating Dockerfile version and release {version}-{release} for {self.release_name}")
        if not self.dry_run:
            rebase.append("--push")
        await exectools.cmd_assert_async(rebase)

        build = base + ["beta:binaries:konflux:build", "--konflux-namespace=ocp-art-tenant"]
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
        entries = [e for e in record_log.get("binary_build_konflux", []) if e.get("name") == self.binary]
        if not entries or entries[-1].get("status") != "0":
            raise RuntimeError(f"binaries/{self.binary} build failed; see {record_log_path}")
        return entries[-1]

    def verify_parents(self, dockerfile_text: str, expected_parents: Dict[str, Dict[str, str]]):
        """
        Fail unless the build used exactly the payload images of the binary's payload_parents.

        :param dockerfile_text: Content of the rebased binaries Dockerfile.
        :param expected_parents: {brew_arch: {payload_tag: digest}} from the release payloads.
        """
        actual = parse_binary_dockerfile_parents(dockerfile_text, self.profile.payload_parents)
        if actual != expected_parents:
            raise RuntimeError(
                f"binaries/{self.binary} parents do not match the release payload.\n"
                f"expected: {expected_parents}\nactual: {actual}"
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

    def verify_cgw_onboarding(self):
        project = self._gitlab().get_project(KRD_PROJECT)
        problems = []
        for name in self.rpa_names:
            path = f"{KRD_RPA_DIR}/{name}.yaml"
            text = self._read_file(project, path)
            if text is None:
                problems.append(f"missing {path}")
                continue
            names = rpa_product_version_names(text)
            if names != [self.cgw_version]:
                problems.append(f'{path} has productVersionName {names}, expected exactly one "{self.cgw_version}"')
        version_path = f"{KRD_CGW_DIR}/{self.profile.cgw_product}/{self.cgw_version}.yaml"
        if self._read_file(project, version_path) is None:
            problems.append(f"missing {version_path}")
        if problems:
            raise RuntimeError(
                f"{self.group} is not onboarded for {self.binary} {self.client_type} CDN releases: "
                f"{'; '.join(problems)} in {KRD_PROJECT} main. Every {self.version} {self.client_type} release "
                f"publishes into CGW version {self.cgw_version}; add or fix these there by hand, then re-run with "
                "the NVR logged above (--nvr) to skip the build."
            )
        LOGGER.info("CGW version %s and RPAs %s are on %s main", self.cgw_version, self.rpa_names, KRD_PROJECT)

    async def run(self):
        if not await self.binary_configured():
            LOGGER.warning("%s has no binaries/%s.yml; nothing to do", self.group, self.binary)
            return

        group_config = await util.load_group_config(
            self.group, self.assembly, doozer_data_path=self.data_path, doozer_data_gitref=self.data_gitref
        )
        if not binaries_cdn_enabled(group_config) and not self.build_only:
            LOGGER.warning(
                "binaries.cdn.enabled is not true for %s (assembly %s); not releasing. Use --build-only to test the build.",
                self.group,
                self.assembly,
            )
            return
        arches = list(group_config.get("arches", []))
        multi = bool(group_config.get("multi_arch", {}).get("enabled", False))
        build_args, expected_parents = await self.collect_release_values(arches, multi)
        LOGGER.info("Release values for %s: %s", self.release_name, build_args)

        nvr = self.nvr
        if not nvr:
            entry = await self.build_binary(build_args)
            if not self.dry_run:
                self.verify_parents(Path(entry["dockerfile"]).read_text(), expected_parents)
            nvr = entry.get("nvrs", "").split(",")[0]
        LOGGER.info("binaries/%s NVR: %s", self.binary, nvr)
        if self.build_only:
            LOGGER.info("--build-only: not releasing %s", nvr)
            return
        if self.dry_run and (not nvr or nvr == "n/a"):
            self.verify_cgw_onboarding()
            LOGGER.info("[DRY-RUN] No NVR built; stopping before the release steps")
            return

        if is_nvr_embargoed(nvr):
            LOGGER.warning(
                "%s is embargoed; not releasing it to the CDN (the mirror.openshift.com path is unaffected)", nvr
            )
            return

        self.verify_cgw_onboarding()

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


@cli.command("binaries-cdn-release")
@click.option("--binary", required=True, type=click.Choice(sorted(BINARIES)), help="binaries/ member to ship")
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
@click.option("--nvr", default=None, help="Release this binaries build NVR instead of building a new one")
@click.option(
    "--build-only", is_flag=True, help="Build and verify the binaries image, then stop before any release step"
)
@pass_runtime
@click_coroutine
async def binaries_cdn_release(
    runtime: Runtime,
    binary: str,
    version: str,
    assembly: str,
    release_name: str,
    client_type: str,
    data_path: str,
    data_gitref: str,
    nvr: Optional[str],
    build_only: bool,
):
    """
    Build a binaries/ member for a promoted release and ship it through Konflux push-artifacts-to-cdn.

    Every release of a MAJOR.MINOR publishes into the same CGW product version (MAJOR.MINOR) and replaces the
    previous release's files. The RPAs and the CGW version file are added to konflux-release-data by hand once
    per MAJOR.MINOR; the job only checks that they are on main and fails if they are not.

    \b
    $ artcd binaries-cdn-release --binary openshift-install --version 4.22 --assembly 4.22.18 \\
        --release-name 4.22.18 --client-type ocp
    """
    pipeline = BinariesCdnReleasePipeline(
        runtime=runtime,
        binary=binary,
        version=version,
        assembly=assembly,
        release_name=release_name,
        client_type=client_type,
        data_path=data_path,
        data_gitref=data_gitref,
        nvr=nvr,
        build_only=build_only,
    )
    await pipeline.run()
