import asyncio
import logging
import os
import re
import shutil
import string
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence, Tuple

from artcommonlib import exectools
from artcommonlib.config.plashet import PlashetConfig
from artcommonlib.config.repo import BrewSource, BrewTag, PlashetRepo, Repo, RepoList
from artcommonlib.util import isolate_major_minor_in_group

from pyartcd import constants, util
from pyartcd.constants import PLASHET_REMOTES

working_dir = "plashet-working"

logger = logging.getLogger('pyartcd')

previous_packages = [
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

ironic_previous_packages_for_4_15_plus = [
    "openstack-ironic",
    "openstack-ironic-inspector",
    "openstack-ironic-python-agent",
    "python-ironic-lib",
    "python-sushy",
]


def plashet_config_for_major_minor(major, minor):
    return {
        "rhel-10-server-ose-rpms-embargoed": {
            "slug": "el10-embargoed",
            "tag": f"rhaos-{major}.{minor}-rhel-10-candidate",
            "product_version": f"OSE-{major}.{minor}-RHEL-10",
            "include_embargoed": True,
            "embargoed_tags": [f"rhaos-{major}.{minor}-rhel-10-embargoed"],
            "include_previous_packages": previous_packages,
        },
        "rhel-10-server-ose-rpms": {
            "slug": "el10",
            "tag": f"rhaos-{major}.{minor}-rhel-10-candidate",
            "product_version": f"OSE-{major}.{minor}-RHEL-10",
            "include_embargoed": False,
            "embargoed_tags": [f"rhaos-{major}.{minor}-rhel-10-embargoed"],
            "include_previous_packages": previous_packages,
        },
        "rhel-9-server-ose-rpms-embargoed": {
            "slug": "el9-embargoed",
            "tag": f"rhaos-{major}.{minor}-rhel-9-candidate",
            "product_version": f"OSE-{major}.{minor}-RHEL-9",
            "include_embargoed": True,
            "embargoed_tags": [f"rhaos-{major}.{minor}-rhel-9-embargoed"],
            "include_previous_packages": previous_packages,
        },
        "rhel-9-server-ose-rpms": {
            "slug": "el9",
            "tag": f"rhaos-{major}.{minor}-rhel-9-candidate",
            "product_version": f"OSE-{major}.{minor}-RHEL-9",
            "include_embargoed": False,
            "embargoed_tags": [f"rhaos-{major}.{minor}-rhel-9-embargoed"],
            "include_previous_packages": previous_packages,
        },
        "rhel-9-server-ironic-rpms": {
            "slug": "ironic-el9",
            "tag": f"rhaos-{major}.{minor}-ironic-rhel-9-candidate",
            "product_version": f"OSE-IRONIC-{major}.{minor}-RHEL-9",
            "include_embargoed": False,
            "embargoed_tags": [],  # unlikely to exist until we begin using -gating tag
            # FIXME: This is a short-term workaround for 4.15+ until prevalidation repo is in use
            # For more info about why this is needed, see https://github.com/openshift-eng/aos-cd-jobs/pull/3920
            "include_previous_packages": ironic_previous_packages_for_4_15_plus
            if (int(major), int(minor)) >= (4, 15)
            else [],
        },
        "rhel-8-server-ose-rpms-embargoed": {
            "slug": "el8-embargoed",
            "tag": f"rhaos-{major}.{minor}-rhel-8-candidate",
            "product_version": f"OSE-{major}.{minor}-RHEL-8",
            "include_embargoed": True,
            "embargoed_tags": [f"rhaos-{major}.{minor}-rhel-8-embargoed"],
            "include_previous_packages": previous_packages,
        },
        "rhel-8-server-ose-rpms": {
            "slug": "el8",
            "tag": f"rhaos-{major}.{minor}-rhel-8-candidate",
            "product_version": f"OSE-{major}.{minor}-RHEL-8",
            "include_embargoed": False,
            "embargoed_tags": [f"rhaos-{major}.{minor}-rhel-8-embargoed"],
            "include_previous_packages": previous_packages,
        },
        "rhel-8-server-ironic-rpms": {
            "slug": "ironic-el8",
            "tag": f"rhaos-{major}.{minor}-ironic-rhel-8-candidate",
            "product_version": f"OSE-IRONIC-{major}.{minor}-RHEL-8",
            "include_embargoed": False,
            "embargoed_tags": [],  # unlikely to exist until we begin using -gating tag
            "include_previous_packages": [],
        },
        "rhel-server-ose-rpms-embargoed": {
            "slug": "el7-embargoed",
            "tag": f"rhaos-{major}.{minor}-rhel-7-candidate",
            "product_version": f"RHEL-7-OSE-{major}.{minor}",
            "include_embargoed": True,
            "embargoed_tags": [f"rhaos-{major}.{minor}-rhel-7-embargoed"],
            "include_previous_packages": previous_packages,
        },
        "rhel-server-ose-rpms": {
            "slug": "el7",
            "tag": f"rhaos-{major}.{minor}-rhel-7-candidate",
            "product_version": f"RHEL-7-OSE-{major}.{minor}",
            "include_embargoed": False,
            "embargoed_tags": [f"rhaos-{major}.{minor}-rhel-7-embargoed"],
            "include_previous_packages": previous_packages,
        },
        "rhel-9-server-microshift-rpms": {
            "slug": "microshift-el9",
            "tag": f"rhaos-{major}.{minor}-rhel-9-candidate",
            "product_version": f"OSE-{major}.{minor}-RHEL-9",
            "include_embargoed": False,
            "embargoed_tags": [],
            "include_previous_packages": [],
        },
    }


def convert_plashet_config_to_new_style(plashet_config: dict) -> list[Repo]:
    repos = []
    for name, config in plashet_config.items():
        repo = Repo(
            name=name,
            type="plashet",
        )
        repo.plashet = PlashetRepo(
            slug=config.get("slug"),
            assembly_aware=True,
            embargo_aware=True,
            include_embargoed=config.get("include_embargoed", False),
            include_previous_packages=config.get("include_previous_packages", []),
            source=BrewSource(
                type="brew",
                from_tags=[
                    BrewTag(
                        name=(tag := config.get("tag")),
                        product_version=config.get("product_version"),
                        release_tag=config.get(
                            "release_tag", tag.removesuffix("-candidate") if tag.endswith("-candidate") else tag
                        ),
                        inherit=config.get("inherit", False),
                    )
                ],
                embargoed_tags=config.get("embargoed_tags", []),
            ),
        )
        repos.append(repo)
    return repos


async def build_plashets(
    group: str,
    release: str,
    assembly: str = 'stream',
    repos: Sequence[str] = (),
    doozer_working: str = 'doozer-working',
    data_path: str = constants.OCP_BUILD_DATA_URL,
    data_gitref: str = '',
    copy_links: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Unless no RPMs have changed, create multiple yum repos (one for each arch) of RPMs
    based on -candidate tags. Based on release state, those repos can be signed
    (release state) or unsigned (pre-release state)

    :param group: e.g. openshift-4.14
    :param release: e.g. 202304181947.p?
    :param assembly: e.g. assembly name, defaults to 'stream'
    :param repos: (optional) limit the repos to build to this list. If empty, build all repos. e.g. ['rhel-8-server-ose-rpms']
    :param doozer_working: Doozer working dir
    :param data_path: ocp-build-data fork to use
    :param data_gitref: Doozer data path git [branch / tag / sha] to use
    :param dry_run: do not actually run the command, just log it
    :param copy_links: transform symlink into referent file/dir

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

    # major, minor = stream.split('.')  # e.g. ('4', '14') from '4.14'
    revision = release.replace('.p?', '')  # e.g. '202304181947' from '202304181947.p?'

    # Load group config
    group_config = await util.load_group_config(
        group=group, assembly=assembly, doozer_data_path=data_path, doozer_data_gitref=data_gitref
    )

    # Check if assemblies are enabled for current group
    if not group_config.get('assemblies', {}).get('enabled'):
        assembly = 'stream'
        logger.warning("Assembly name reset to 'stream' because assemblies are not enabled in ocp-build-data.")

    # Get plashet repo configs
    if "all_repos" in group_config:
        # This is new-style repo config.
        # i.e. Plashet configs are defined in separate files in ocp-build-data
        # and each repo has its own config file.
        # Those repo definitions are stored in the "all_repos" key of the group config.
        logger.info("Using new-style plashet configs")
        all_repos = group_config['all_repos']
        plashet_repos = [
            repo for repo in RepoList.model_validate(all_repos).root if not repo.disabled and repo.type == 'plashet'
        ]
        if repos:
            repo_set = set(repos)
            plashet_repos = [repo for repo in plashet_repos if repo.name in repo_set]
        group_vars = group_config.get("vars", {})
        major, minor = group_vars.get("MAJOR"), group_vars.get("MINOR")
        plashet_config = PlashetConfig.model_validate(group_config.get('plashet', {}))

    else:  # Fall back to old-style config
        # old-style config only supports openshift-x.y groups
        major, minor = isolate_major_minor_in_group(group)
        if major is None:
            raise ValueError("Old-style plashet config only supports openshift-x.y groups")
        logger.info("Using old-style plashet configs in group.yml")
        group_repos = group_config.get('repos', {}).keys()
        if repos:
            logger.info(f"Filtering plashet repos to only the given ones: {repos}")
            group_repos = [repo for repo in group_repos if repo in repos]
        old_style_config = plashet_config_for_major_minor(major, minor)
        old_style_config = {
            repo: old_style_config[repo] for repo in plashet_config_for_major_minor(major, minor) if repo in group_repos
        }
        plashet_repos = convert_plashet_config_to_new_style(old_style_config)
        plashet_config = PlashetConfig(
            base_dir=f"{major}.{minor}/$runtime_assembly/$slug",
            plashet_dir="$yyyy-$MM/$revision",
            create_symlinks=True,
            symlink_name='latest',
            create_repo_subdirs=True,
            repo_subdir='os',
            download_url="https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/{MAJOR}.{MINOR}/$runtime_assembly/$slug/latest/$arch/os/",
        )

    plashet_repo_names = [repo.name for repo in plashet_repos]
    logger.info("Building plashet repos: %s", ", ".join(plashet_repo_names))

    # Check release state
    signing_mode = await util.get_signing_mode(group_config=group_config)

    # Create plashet repos on ocp-artifacts
    # We can't safely run doozer config:plashet from-tags in parallel as this moment.
    # Build plashet repos one by one.

    plashets_built = {}  # hold the information of all built plashet repos
    timestamp = datetime.strptime(revision, '%Y%m%d%H%M%S')
    signing_advisory = group_config.get('signing_advisory', '0')
    arches = plashet_config.arches or group_config['arches']
    group_param = group
    if data_gitref:
        group_param += f'@{data_gitref}'

    for repo in plashet_repos:
        logger.info('Building plashet repo for %s', repo.name)
        assert repo.type == "plashet" and repo.plashet is not None
        config = repo.plashet
        slug = config.slug or repo.name
        # name = f'{timestamp.year}-{timestamp.month:02}/{revision}'
        variables = {
            'runtime_assembly': assembly,
            'slug': slug,
            'yyyy': f'{timestamp.year:04}',
            'MM': f'{timestamp.month:02}',
            'dd': f'{timestamp.day:02}',
            'hh': f'{timestamp.hour:02}',
            'mm': f'{timestamp.minute:02}',
            'ss': f'{timestamp.second:02}',
            'revision': revision,
            'MAJOR': major,
            'MINOR': minor,
        }
        base_dir_template = string.Template(plashet_config.base_dir)
        base_dir_name = base_dir_template.substitute(**variables)
        local_base_dir = Path(working_dir, base_dir_name)
        plashet_name_template = string.Template(plashet_config.plashet_dir)
        plashet_name = plashet_name_template.substitute(**variables)

        local_path = await build_plashet_from_tags(
            group_param=group_param,
            assembly=assembly,
            base_dir=local_base_dir,
            name=plashet_name,
            arches=arches,
            include_embargoed=config.include_embargoed,
            signing_mode=signing_mode,
            signing_advisory=signing_advisory,
            embargoed_tags=config.source.embargoed_tags,
            tag_pvs=((config.source.from_tags[0].name, config.source.from_tags[0].product_version),),
            include_previous_packages=config.include_previous_packages,
            repo_subdir=plashet_config.repo_subdir if plashet_config.create_repo_subdirs else None,
            data_path=data_path,
            dry_run=dry_run,
            doozer_working=doozer_working,
        )

        logger.info('Plashet repo for %s created: %s', repo.name, local_path)
        if plashet_config.create_symlinks:
            symlink_path = create_latest_symlink(
                base_dir=local_base_dir, plashet_name=plashet_name, symlink_name=plashet_config.symlink_name
            )
            logger.info('Symlink for %s created: %s', repo.name, symlink_path)

        remote_base_dir = Path("/mnt/data/pub/RHOCP/plashets", base_dir_name)
        logger.info('Copying %s to remote host...', remote_base_dir)

        await asyncio.gather(
            *[
                copy_to_remote(
                    plashet_remote['host'], local_base_dir, remote_base_dir, dry_run=dry_run, copy_links=copy_links
                )
                for plashet_remote in PLASHET_REMOTES
            ]
        )

        plashets_built[repo.name] = {
            'plashetDirName': revision,
            'localPlashetPath': str(local_path),
        }

    return plashets_built


async def build_plashet_from_tags(
    group_param: str,
    assembly: str,
    base_dir: os.PathLike,
    name: str,
    arches: Sequence[str],
    include_embargoed: bool,
    signing_mode: str,
    signing_advisory: int,
    tag_pvs: Sequence[Tuple[str, str]],
    embargoed_tags: Optional[Sequence[str]],
    include_previous_packages: Optional[Sequence[str]] = None,
    repo_subdir: str | None = 'os',
    poll_for: int = 0,
    data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_working: str = 'doozer-working',
    dry_run: bool = False,
):
    """
    Builds Plashet repo with "from-tags"
    """

    repo_path = Path(base_dir, name)
    if repo_path.exists():
        shutil.rmtree(repo_path)
    cmd = [
        "doozer",
        f'--data-path={data_path}',
        "--working-dir",
        doozer_working,
        "--group",
        group_param,
        "--assembly",
        assembly,
        "config:plashet",
        "--base-dir",
        str(base_dir),
        "--name",
        name,
    ]
    if repo_subdir:
        cmd.extend(["--repo-subdir", repo_subdir])
    for arch in arches:
        cmd.extend(["--arch", arch, signing_mode])
    cmd.extend(
        [
            "from-tags",
            "--signing-advisory-id",
            f"{signing_advisory or 54765}",
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

    if dry_run:
        repo_path.mkdir(parents=True)
        logger.warning("[Dry run] Would have run %s", cmd)
    else:
        logger.info("Executing %s", cmd)
        await exectools.cmd_assert_async(cmd, env=os.environ.copy())
    return os.path.abspath(Path(base_dir, name))


def create_latest_symlink(base_dir: os.PathLike, plashet_name: str, symlink_name: str = "latest"):
    """
    Create or update a 'latest' symlink to point to the given plashet_name directory
    """
    if not symlink_name:
        raise ValueError("symlink_name must be a non-empty string")
    symlink_path = Path(base_dir, symlink_name)
    if symlink_path.is_symlink():
        symlink_path.unlink()
    symlink_path.symlink_to(plashet_name, target_is_directory=True)
    return symlink_path


async def copy_to_remote(
    plashet_remote_host: str,
    local_base_dir: os.PathLike,
    remote_base_dir: os.PathLike,
    dry_run: bool = False,
    copy_links: bool = False,
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
    if dry_run:
        logger.warning("[DRY RUN] Would have run %s", cmd)
    else:
        logger.info("Executing %s", ' '.join(cmd))
        await exectools.cmd_assert_async(cmd, env=os.environ.copy())

    # Copy local dir to remote
    cmd = ["rsync", "-av"]
    if copy_links:
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

    if dry_run:
        logger.warning("[DRY RUN] Would have run %s", cmd)
    else:
        logger.info("Executing %s", ' '.join(cmd))
        await exectools.cmd_assert_async(cmd, env=os.environ.copy())
