import asyncio
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence, Tuple

from artcommonlib import exectools

from pyartcd import constants, util
from pyartcd.constants import PLASHET_REMOTES

WORKING_DIR = "plashet-working"

PREVIOUS_PACKAGES = [
    "conmon",
    "cri-o",
    "cri-tools",
    "crun",
    "haproxy",
    "ignition",
    "kernel",
    "kernel-rt",
    "libreswan",  # can disappear after ovn dropped the pin in dockerfile
    "nmstate",
    "openshift",
    "openvswitch",
    "ovn",
    "podman",
    "python3-openvswitch",
    "spdlog",
]

IRONIC_PREVIOUS_PACKAGES_FOR_4_15_plus = [
    "openstack-ironic",
    "openstack-ironic-inspector",
    "openstack-ironic-python-agent",
    "python-ironic-lib",
    "python-sushy",
]


class PlashetBuilder:
    def __init__(
        self,
        version,
        release: str = '',
        assembly: str = 'stream',
        doozer_working: str = 'doozer-working',
        data_path: str = constants.OCP_BUILD_DATA_URL,
        data_gitref: str = '',
        dry_run: bool = False,
        copy_links: bool = False,
    ):
        """
        :param version: e.g. 4.14
        :param release: e.g. 202304181947.p?
        :param assembly: e.g. assembly name, defaults to 'stream'
        :param doozer_working: Doozer working dir
        :param data_path: ocp-build-data fork to use
        :param data_gitref: Doozer data path git [branch / tag / sha] to use
        :param dry_run: do not actually run the command, just log it
        :param copy_links: transform symlink into referent file/dir
        """

        self.logger = logging.getLogger(__name__)

        self.version = version
        self.major, self.minor = version.split('.')
        self.assembly = assembly
        self.doozer_working = doozer_working
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.dry_run = dry_run
        self.copy_links = copy_links

        self._revision = release.replace('.p?', '')  # e.g. '202304181947' from '202304181947.p?'
        self._timestamp = datetime.strptime(self._revision, '%Y%m%d%H%M%S')
        self._plashet_config = {}
        self._signing_mode = ''
        self._plashets_built = {}  # hold the information of all built plashet repos
        self._signing_advisory = None
        self._arches = []
        self._group_param = ''

    def plashet_config_for_major_minor(self):
        return {
            "rhel-10-server-ose-rpms-embargoed": {
                "slug": "el10-embargoed",
                "tag": f"rhaos-{self.major}.{self.minor}-rhel-10-candidate",
                "product_version": f"OSE-{self.major}.{self.minor}-RHEL-10",
                "include_embargoed": True,
                "embargoed_tags": [f"rhaos-{self.major}.{self.minor}-rhel-10-embargoed"],
                "include_previous_packages": PREVIOUS_PACKAGES,
            },
            "rhel-10-server-ose-rpms": {
                "slug": "el10",
                "tag": f"rhaos-{self.major}.{self.minor}-rhel-10-candidate",
                "product_version": f"OSE-{self.major}.{self.minor}-RHEL-10",
                "include_embargoed": False,
                "embargoed_tags": [f"rhaos-{self.major}.{self.minor}-rhel-10-embargoed"],
                "include_previous_packages": PREVIOUS_PACKAGES,
            },
            "rhel-9-server-ose-rpms-embargoed": {
                "slug": "el9-embargoed",
                "tag": f"rhaos-{self.major}.{self.minor}-rhel-9-candidate",
                "product_version": f"OSE-{self.major}.{self.minor}-RHEL-9",
                "include_embargoed": True,
                "embargoed_tags": [f"rhaos-{self.major}.{self.minor}-rhel-9-embargoed"],
                "include_previous_packages": PREVIOUS_PACKAGES,
            },
            "rhel-9-server-ose-rpms": {
                "slug": "el9",
                "tag": f"rhaos-{self.major}.{self.minor}-rhel-9-candidate",
                "product_version": f"OSE-{self.major}.{self.minor}-RHEL-9",
                "include_embargoed": False,
                "embargoed_tags": [f"rhaos-{self.major}.{self.minor}-rhel-9-embargoed"],
                "include_previous_packages": PREVIOUS_PACKAGES,
            },
            "rhel-9-server-ironic-rpms": {
                "slug": "ironic-el9",
                "tag": f"rhaos-{self.major}.{self.minor}-ironic-rhel-9-candidate",
                "product_version": f"OSE-IRONIC-{self.major}.{self.minor}-RHEL-9",
                "include_embargoed": False,
                "embargoed_tags": [],  # unlikely to exist until we begin using -gating tag
                # FIXME: This is a short-term workaround for 4.15+ until prevalidation repo is in use
                # For more info about why this is needed, see https://github.com/openshift-eng/aos-cd-jobs/pull/3920
                "include_previous_packages": IRONIC_PREVIOUS_PACKAGES_FOR_4_15_plus
                if (int(self.major), int(self.minor)) >= (4, 15)
                else [],
            },
            "rhel-8-server-ose-rpms-embargoed": {
                "slug": "el8-embargoed",
                "tag": f"rhaos-{self.major}.{self.minor}-rhel-8-candidate",
                "product_version": f"OSE-{self.major}.{self.minor}-RHEL-8",
                "include_embargoed": True,
                "embargoed_tags": [f"rhaos-{self.major}.{self.minor}-rhel-8-embargoed"],
                "include_previous_packages": PREVIOUS_PACKAGES,
            },
            "rhel-8-server-ose-rpms": {
                "slug": "el8",
                "tag": f"rhaos-{self.major}.{self.minor}-rhel-8-candidate",
                "product_version": f"OSE-{self.major}.{self.minor}-RHEL-8",
                "include_embargoed": False,
                "embargoed_tags": [f"rhaos-{self.major}.{self.minor}-rhel-8-embargoed"],
                "include_previous_packages": PREVIOUS_PACKAGES,
            },
            "rhel-8-server-ironic-rpms": {
                "slug": "ironic-el8",
                "tag": f"rhaos-{self.major}.{self.minor}-ironic-rhel-8-candidate",
                "product_version": f"OSE-IRONIC-{self.major}.{self.minor}-RHEL-8",
                "include_embargoed": False,
                "embargoed_tags": [],  # unlikely to exist until we begin using -gating tag
                "include_previous_packages": [],
            },
            "rhel-server-ose-rpms-embargoed": {
                "slug": "el7-embargoed",
                "tag": f"rhaos-{self.major}.{self.minor}-rhel-7-candidate",
                "product_version": f"RHEL-7-OSE-{self.major}.{self.minor}",
                "include_embargoed": True,
                "embargoed_tags": [f"rhaos-{self.major}.{self.minor}-rhel-7-embargoed"],
                "include_previous_packages": PREVIOUS_PACKAGES,
            },
            "rhel-server-ose-rpms": {
                "slug": "el7",
                "tag": f"rhaos-{self.major}.{self.minor}-rhel-7-candidate",
                "product_version": f"RHEL-7-OSE-{self.major}.{self.minor}",
                "include_embargoed": False,
                "embargoed_tags": [f"rhaos-{self.major}.{self.minor}-rhel-7-embargoed"],
                "include_previous_packages": PREVIOUS_PACKAGES,
            },
            "rhel-9-server-microshift-rpms": {
                "slug": "microshift-el9",
                "tag": f"rhaos-{self.major}.{self.minor}-rhel-9-candidate",
                "product_version": f"OSE-{self.major}.{self.minor}-RHEL-9",
                "include_embargoed": False,
                "embargoed_tags": [],
                "include_previous_packages": [],
            },
        }

    async def load_and_configure(self, repos):
        # Load group config
        group_config = await util.load_group_config(
            group=f'openshift-{self.version}',
            assembly=self.assembly,
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )

        # Check if assemblies are enabled for current group
        if not group_config.get('assemblies', {}).get('enabled'):
            self.assembly = 'stream'
            self.logger.warning("Assembly name reset to 'stream' because assemblies are not enabled in ocp-build-data.")

        # Get plashet repos
        group_repos = group_config.get('repos', {}).keys()
        if repos:
            self.logger.info(f"Filtering plashet repos to only the given ones: {repos}")
            group_repos = [repo for repo in group_repos if repo in repos]
        group_plashet_config = self.plashet_config_for_major_minor()
        self._plashet_config = {
            repo: group_plashet_config[repo] for repo in group_plashet_config if repo in group_repos
        }
        self.logger.info("Building plashet repos: %s", ", ".join(self._plashet_config.keys()))

        # Check release state
        self._signing_mode = await util.get_signing_mode(group_config=group_config)

        # Create plashet repos on ocp-artifacts
        # We can't safely run doozer config:plashet from-tags in parallel as this moment.
        # Build plashet repos one by one.
        self._signing_advisory = group_config.get('signing_advisory', '0')
        self._arches = group_config['arches']
        self._group_param = f'openshift-{self.version}'
        if self.data_gitref:
            self._group_param += f'@{self.data_gitref}'

    async def build_plashet(self, repo_type, config):
        self.logger.info('Building plashet repo for %s', repo_type)
        slug = config['slug']
        name = f'{self._timestamp.year}-{self._timestamp.month:02}/{self._revision}'
        base_dir = Path(WORKING_DIR, f'plashets/{self.major}.{self.minor}/{self.assembly}/{slug}')
        local_path = await self.build_plashet_from_tags(
            base_dir=base_dir,
            name=name,
            include_embargoed=config['include_embargoed'],
            embargoed_tags=config['embargoed_tags'],
            tag_pvs=((config["tag"], config['product_version']),),
            include_previous_packages=config['include_previous_packages'],
        )

        self.logger.info('Plashet repo for %s created: %s', repo_type, local_path)
        symlink_path = self.create_latest_symlink(base_dir=base_dir, plashet_name=name)
        self.logger.info('Symlink for %s created: %s', repo_type, symlink_path)

        remote_base_dir = Path(f'/mnt/data/pub/RHOCP/plashets/{self.major}.{self.minor}/{self.assembly}/{slug}')
        self.logger.info('Copying %s to remote host...', base_dir)

        await asyncio.gather(
            *[
                self.copy_to_remote(plashet_remote['host'], base_dir, remote_base_dir)
                for plashet_remote in PLASHET_REMOTES
            ]
        )

        self._plashets_built[repo_type] = {
            'plashetDirName': self._revision,
            'localPlashetPath': str(local_path),
        }

    async def build_plashets(self, repos: Sequence[str] = ()) -> dict:
        """
        Unless no RPMs have changed, create multiple yum repos (one for each arch) of RPMs
        based on -candidate tags. Based on release state, those repos can be signed
        (release state) or unsigned (pre-release state)

        :param repos: (optional) limit the repos to build to this list. If empty, build all repos. e.g. ['rhel-8-server-ose-rpms']

        Returns a list describing the plashets that have been built. The dict will look like this:
        {
            'repo-name-1': {
                'plashetDirName': str,
                'localPlashetPath: str
            },

            ...

            'repo-name-n': {
                'plashetDirName': str,
                'localPlashetPath: str
            },
        }
        """

        await self.load_and_configure(repos)
        await asyncio.gather(
            *[self.build_plashet(repo_type, config) for repo_type, config in self._plashet_config.items()]
        )
        return self._plashets_built

    async def build_plashet_from_tags(
        self,
        base_dir: os.PathLike,
        name: str,
        include_embargoed: bool,
        tag_pvs: Sequence[Tuple[str, str]],
        embargoed_tags: Optional[Sequence[str]],
        include_previous_packages: Optional[Sequence[str]] = None,
        poll_for: int = 0,
    ):
        """
        Builds Plashet repo with "from-tags"
        """

        repo_path = Path(base_dir, name)
        if repo_path.exists():
            shutil.rmtree(repo_path)
        cmd = [
            "doozer",
            f'--data-path={self.data_path}',
            "--working-dir",
            self.doozer_working,
            "--group",
            self._group_param,
            "--assembly",
            self.assembly,
            "config:plashet",
            "--base-dir",
            str(base_dir),
            "--name",
            name,
            "--repo-subdir",
            "os",
        ]
        for arch in self._arches:
            cmd.extend(["--arch", arch, self._signing_mode])
        cmd.extend(
            [
                "from-tags",
                "--signing-advisory-id",
                f"{self._signing_advisory or 54765}",
                "--signing-advisory-mode",
                "clean",
                "--inherit",
            ]
        )
        if include_embargoed:
            cmd.append("--include-embargoed")
        if embargoed_tags:
            for t in embargoed_tags:
                cmd.extend(["--embargoed-brew-tag", t])
        for tag, pv in tag_pvs:
            cmd.extend(["--brew-tag", tag, pv])
        for pkg in include_previous_packages:
            cmd.extend(["--include-previous-for", pkg])
        if poll_for:
            cmd.extend(["--poll-for", str(poll_for)])

        if self.dry_run:
            repo_path.mkdir(parents=True)
            self.logger.warning("[Dry run] Would have run %s", cmd)
        else:
            self.logger.info("Executing %s", cmd)
            await exectools.cmd_assert_async(cmd, env=os.environ.copy())
        return os.path.abspath(Path(base_dir, name))

    @staticmethod
    def create_latest_symlink(base_dir: os.PathLike, plashet_name: str):
        symlink_path = Path(base_dir, "latest")
        if symlink_path.is_symlink():
            symlink_path.unlink()
        symlink_path.symlink_to(plashet_name, target_is_directory=True)
        return symlink_path

    async def copy_to_remote(
        self,
        plashet_remote_host: str,
        local_base_dir: os.PathLike,
        remote_base_dir: os.PathLike,
    ):
        """
        Copies plashet out to remote host (ocp-artifacts)
        """

        # Make sure the remote base dir exist
        local_base_dir = Path(local_base_dir)
        remote_base_dir = Path(remote_base_dir)
        cmd = [
            "ssh",
            plashet_remote_host,
            "--",
            "mkdir",
            "-p",
            "--",
            f"{remote_base_dir}",
        ]
        if self.dry_run:
            self.logger.warning("[DRY RUN] Would have run %s", cmd)
        else:
            self.logger.info("Executing %s", ' '.join(cmd))
            await exectools.cmd_assert_async(cmd, env=os.environ.copy())

        # Copy local dir to remote
        cmd = ["rsync", "-av"]
        if self.copy_links:
            cmd.append('--copy-links')
        else:
            cmd.append('--links')
        cmd.extend(
            [
                "--progress",
                "-h",
                "--no-g",
                "--omit-dir-times",
                "--chmod=Dug=rwX,ugo+r",
                "--perms",
                "--",
                f"{local_base_dir}/",
                f"{plashet_remote_host}:{remote_base_dir}",
            ]
        )

        if self.dry_run:
            self.logger.warning("[DRY RUN] Would have run %s", cmd)
        else:
            self.logger.info("Executing %s", ' '.join(cmd))
            await exectools.cmd_assert_async(cmd, env=os.environ.copy())
