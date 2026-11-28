import asyncio
import logging
import os
import shutil
import string
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence, Tuple

import yaml
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
            "exclude_packages": ["kernel", "kernel-rt"] if (int(major), int(minor)) >= (4, 13) else [],
        },
        "rhel-8-server-ose-rpms": {
            "slug": "el8",
            "tag": f"rhaos-{major}.{minor}-rhel-8-candidate",
            "product_version": f"OSE-{major}.{minor}-RHEL-8",
            "include_embargoed": False,
            "embargoed_tags": [f"rhaos-{major}.{minor}-rhel-8-embargoed"],
            "include_previous_packages": previous_packages,
            "exclude_packages": ["kernel", "kernel-rt"] if (int(major), int(minor)) >= (4, 13) else [],
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
        "rhel-10-golang-rpms": {
            "slug": "golang-el10",
            "tag": f"rhaos-{major}.{minor}-rhel-10-override",
            "product_version": f"OSE-{major}.{minor}-RHEL-10",
            "include_embargoed": False,
            "embargoed_tags": [],
            "include_previous_packages": [],
            "include_packages": ["golang", "goversioninfo"],
        },
        "rhel-9-golang-rpms": {
            "slug": "golang-el9",
            "tag": f"rhaos-{major}.{minor}-rhel-9-override",
            "product_version": f"OSE-{major}.{minor}-RHEL-9",
            "include_embargoed": False,
            "embargoed_tags": [],
            "include_previous_packages": [],
            "include_packages": ["golang", "goversioninfo"],
        },
        "rhel-8-golang-rpms": {
            "slug": "golang-el8",
            "tag": f"rhaos-{major}.{minor}-rhel-8-override",
            "product_version": f"OSE-{major}.{minor}-RHEL-8",
            "include_embargoed": False,
            "embargoed_tags": [],
            "include_previous_packages": [],
            "include_packages": ["golang", "goversioninfo"],
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
            include_packages=config.get("include_packages", []),
            exclude_packages=config.get("exclude_packages", []),
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
    version: str | None = None,
) -> dict:
    """
    Unless no RPMs have changed, create multiple yum repos (one for each arch) of RPMs
    based on -candidate tags. Based on release state, those repos can be signed
    (release state) or unsigned (pre-release state)

    :param group: e.g. openshift-4.14 or golang
    :param release: e.g. 202304181947.p?
    :param assembly: e.g. assembly name, defaults to 'stream'
    :param repos: (optional) limit the repos to build to this list. If empty, build all repos. e.g. ['rhel-8-server-ose-rpms']
    :param version: (optional) OCP version for resolving MAJOR/MINOR vars in golang groups (e.g. '4.18'). Required for golang groups.
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
    extra_vars = []
    revision = release.replace('.p?', '')  # e.g. '202304181947' from '202304181947.p?'

    # For golang groups, use version parameter to inject MAJOR/MINOR vars
    is_golang_group = group.startswith('golang')
    if is_golang_group and version:
        major, minor = version.split('.')
        extra_vars = [f"MAJOR={major}", f"MINOR={minor}"]

    # Load group config
    group_config = await util.load_group_config(
        group=group,
        assembly=assembly,
        doozer_data_path=data_path,
        doozer_data_gitref=data_gitref,
        extra_vars=extra_vars,
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
        major, minor = isolate_major_minor_in_group(group)
        if major is None:
            group_vars = group_config.get("vars", {})
            major, minor = group_vars.get("MAJOR"), group_vars.get("MINOR")
            if major is None or minor is None:
                raise ValueError(
                    f"Cannot determine MAJOR/MINOR for group '{group}': "
                    "not an openshift-x.y group and MAJOR/MINOR vars not set in group config"
                )
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
    signing_mode = await util.get_signing_mode(group=group, group_config=group_config)

    # Create plashet repos on ocp-artifacts
    # We can't safely run doozer config:plashet from-tags in parallel as this moment.
    # Build plashet repos one by one.

    plashets_built = {}  # hold the information of all built plashet repos
    # Track per-repo build info (local dirs) so an embargoed repo can locate its
    # already-built public sibling for the symlink optimization.
    built_info = {}
    timestamp = datetime.strptime(revision, '%Y%m%d%H%M%S')
    signing_advisory = group_config.get('signing_advisory', '0')
    arches = plashet_config.arches or group_config['arches']
    group_param = group
    if data_gitref:
        group_param += f'@{data_gitref}'

    # Build public siblings before any repo that symlinks to them. sorted() is stable,
    # so repos that opt into the symlink optimization (which are always the embargoed
    # flavor) are deferred until after their public siblings have been built.
    plashet_repos = sorted(plashet_repos, key=lambda r: bool(r.plashet and r.plashet.symlink_when_unembargoed))

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

        # Decide whether this embargoed repo can be a symlink to its public sibling.
        symlink_target_path = None
        if config.symlink_when_unembargoed and config.embargoed_sibling:
            sibling_info = built_info.get(config.embargoed_sibling)
            if sibling_info is None:
                # The public sibling was not built in THIS invocation (e.g. a `repos` filter
                # selected only the embargoed flavor, or the sibling is disabled). We have no
                # fresh public plashet to symlink to, so fall back to a full embargoed build.
                logger.info(
                    "Public sibling '%s' of '%s' was not built in this invocation; falling back to a full build.",
                    config.embargoed_sibling,
                    repo.name,
                )
            else:
                verdict = public_sibling_symlink_safe(sibling_info['localPlashetPath'])
                if verdict is None:
                    logger.warning(
                        "Could not read embargo signal from public sibling '%s' (plashet.yml "
                        "missing/unreadable); falling back to full build for '%s'",
                        config.embargoed_sibling,
                        repo.name,
                    )
                elif verdict is False:
                    logger.info(
                        "Public sibling '%s' shows an embargo signal; building '%s' fully",
                        config.embargoed_sibling,
                        repo.name,
                    )
                else:
                    symlink_target_path = sibling_info['localPlashetPath']
                    logger.info(
                        "Public sibling '%s' has no embargoed content or embargo concern; '%s' will be a symlink to it",
                        config.embargoed_sibling,
                        repo.name,
                    )

        if symlink_target_path is not None:
            # No embargoed content: create the embargoed flavor as a RELATIVE symlink to the
            # already-built public sibling directory instead of running a redundant full build.
            local_path = create_plashet_symlink(
                base_dir=local_base_dir,
                plashet_name=plashet_name,
                target_path=symlink_target_path,
            )
            logger.info('Plashet repo for %s created as symlink: %s -> %s', repo.name, local_path, symlink_target_path)
        else:
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
                tag_pvs=tuple((tag.name, tag.product_version) for tag in config.source.from_tags),
                include_previous_packages=config.include_previous_packages,
                include_packages=config.include_packages,
                exclude_packages=config.exclude_packages,
                repo_subdir=plashet_config.repo_subdir if plashet_config.create_repo_subdirs else None,
                data_path=data_path,
                dry_run=dry_run,
                doozer_working=doozer_working,
                version=version,
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
        built_info[repo.name] = {
            'localBaseDir': str(local_base_dir),
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
    include_packages: Optional[Sequence[str]] = None,
    exclude_packages: Optional[Sequence[str]] = None,
    repo_subdir: str | None = 'os',
    poll_for: int = 0,
    data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_working: str = 'doozer-working',
    dry_run: bool = False,
    version: str | None = None,
):
    """
    Builds Plashet repo with "from-tags"
    """

    repo_path = Path(base_dir, name)
    # A retry after a failed publication may find repo_path as a (possibly dangling) symlink
    # left by a previous symlink run. shutil.rmtree() refuses a directory symlink on py3.11,
    # so unlink symlinks first and only rmtree real directories.
    if repo_path.is_symlink():
        repo_path.unlink()
    elif repo_path.exists():
        shutil.rmtree(repo_path)
    cmd = [
        "doozer",
        f'--data-path={data_path}',
        "--working-dir",
        doozer_working,
    ]

    # Add MAJOR/MINOR vars for golang groups when version is provided
    # This is used by golang update pipeline to inject version for monobranch groups
    is_golang_group = group_param.startswith('golang')  # Handle group@gitref format
    if is_golang_group and version:
        major, minor = version.split('.')
        cmd.extend(['--var', f'MAJOR={major}', '--var', f'MINOR={minor}'])

    cmd.extend(
        [
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
    )
    if repo_subdir:
        cmd.extend(["--repo-subdir", repo_subdir])
    for arch in arches:
        cmd.extend(["--arch", arch, signing_mode])
    for pkg in include_packages or []:
        cmd.extend(["--include-package", pkg])
    for pkg in exclude_packages or []:
        cmd.extend(["--exclude-package", pkg])
    from_tags_args = ["from-tags"]
    if signing_advisory and str(signing_advisory) != '0':
        from_tags_args.extend(
            [
                "--signing-advisory-id",
                str(signing_advisory),
                "--signing-advisory-mode",
                "clean",
            ]
        )
    from_tags_args.append("--inherit")
    cmd.extend(from_tags_args)
    if include_embargoed:
        cmd.append("--include-embargoed")
    if embargoed_tags:
        for t in embargoed_tags:
            cmd.extend(["--embargoed-brew-tag", t])
    for tag, pv in tag_pvs:
        cmd.extend(["--brew-tag", tag, pv])
    exclude_set = set(exclude_packages or [])
    for pkg in include_previous_packages:
        if pkg not in exclude_set:
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


def _load_public_plashet_yml(public_plashet_path: os.PathLike) -> dict | None:
    """
    Load and parse a plashet's plashet.yml.

    Returns the parsed mapping, or ``None`` if the file is missing/unreadable (signalling the
    caller to fall back to a full build rather than risk an unsafe assumption).
    """
    plashet_yml = Path(public_plashet_path, 'plashet.yml')
    if not plashet_yml.is_file():
        return None
    try:
        with open(plashet_yml, encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError) as e:
        logger.warning("Failed to read %s: %s", plashet_yml, e)
        return None


def read_detected_as_embargoed(public_plashet_path: os.PathLike) -> list | None:
    """
    Read the authoritative embargo signal from a public plashet's plashet.yml.

    Returns the list of NVREs detected as embargoed (``extra.embargo_info.detected_as_embargoed``).
    An empty list means no embargoed content was *detected*.

    Returns ``None`` if the plashet.yml could not be found/read, signalling the caller to
    fall back to a full build rather than risk an unsafe assumption.
    """
    data = _load_public_plashet_yml(public_plashet_path)
    if data is None:
        return None
    return data.get('extra', {}).get('embargo_info', {}).get('detected_as_embargoed', [])


def _mentions_embargo(obj) -> bool:
    """
    Recursively scan the string *values* (not dict keys) of ``obj`` for a case-insensitive
    ``"embargo"`` mention. Keys are skipped so structural names like ``embargo_info`` /
    ``embargoed_permitted`` do not trivially trigger a match.
    """
    if isinstance(obj, str):
        return 'embargo' in obj.lower()
    if isinstance(obj, dict):
        return any(_mentions_embargo(v) for v in obj.values())
    if isinstance(obj, (list, tuple)):
        return any(_mentions_embargo(v) for v in obj)
    return False


def public_sibling_symlink_safe(public_plashet_path: os.PathLike) -> bool | None:
    """
    Decide whether an embargoed flavor may safely be a symlink to this public sibling.

    An empty ``detected_as_embargoed`` list is NOT sufficient: when ``include_previous_packages``
    would pull an embargoed historical build, the public build omits it and records a concern
    like "Unable to include previous ... under embargo" WITHOUT adding it to
    ``detected_as_embargoed`` -- so the embargoed full build could still differ. We therefore
    require BOTH that ``detected_as_embargoed`` is empty AND that no embargo-related concern is
    present (scanning ``extra.embargo_info`` and the ``assemble.concerns`` list, plus a top-level
    ``concerns`` list for robustness).

    Returns:
        ``True``  -> safe to symlink (no embargoed content, no embargo concern),
        ``False`` -> an embargo signal is present; build the embargoed flavor fully,
        ``None``  -> plashet.yml missing/unreadable; caller should fall back to a full build.
    """
    data = _load_public_plashet_yml(public_plashet_path)
    if data is None:
        return None
    embargo_info = data.get('extra', {}).get('embargo_info', {}) or {}
    if embargo_info.get('detected_as_embargoed'):
        return False
    concerns = data.get('assemble', {}).get('concerns', []) or []
    top_level_concerns = data.get('concerns', []) or []
    if _mentions_embargo(embargo_info) or _mentions_embargo(concerns) or _mentions_embargo(top_level_concerns):
        return False
    return True


def _is_within(child: os.PathLike, parent: os.PathLike) -> bool:
    """True when ``child`` resolves to ``parent`` itself or to a path nested under it."""
    child_abs = os.path.abspath(child)
    parent_abs = os.path.abspath(parent)
    return child_abs == parent_abs or child_abs.startswith(parent_abs + os.sep)


def create_plashet_symlink(base_dir: os.PathLike, plashet_name: str, target_path: os.PathLike) -> str:
    """
    Create the plashet directory for a flavor as a RELATIVE symlink pointing at an
    already-built plashet directory (``target_path``), instead of building it from tags.

    The symlink is created relative so that it survives the per-flavor rsync (``--links``)
    and resolves correctly on the remote host, where the two flavor base_dirs are siblings.

    Returns the absolute path to the created symlink (mirrors build_plashet_from_tags).
    """
    repo_path = Path(base_dir, plashet_name)
    base_abs = os.path.abspath(base_dir)
    dest_abs = os.path.abspath(repo_path)

    # SAFETY (path traversal): the destination must stay strictly inside base_dir. Reject an
    # absolute plashet_name or any ".." that would escape base_dir BEFORE we delete anything.
    # This is a deliberately LEXICAL check on the requested plashet_name, independent of symlinks.
    if not dest_abs.startswith(base_abs + os.sep):
        raise ValueError(
            f"create_plashet_symlink: plashet_name {plashet_name!r} escapes base_dir {base_dir} "
            f"(resolved destination {dest_abs!r})."
        )

    # SAFETY (self-reference/overlap): the lexical check above is NOT enough, because a parent or
    # ancestor directory of the destination or of the target may itself be a symlink. Two
    # lexically-different paths can then be the SAME real directory, and clearing the destination
    # would delete the public content we are about to link to. Canonicalize with realpath before
    # the overlap checks, but keep the destination's final LEAF component separate (do not follow
    # it) so an existing leaf symlink at the destination can still be unlinked safely below.
    base_real = os.path.realpath(base_dir)
    dest_parent_real = os.path.realpath(repo_path.parent)
    dest_real = os.path.join(dest_parent_real, repo_path.name)
    target_real = os.path.realpath(target_path)

    # The REAL destination parent must still live inside the REAL base_dir; otherwise a symlinked
    # ancestor is redirecting our deletes/writes outside this repo.
    if not _is_within(dest_parent_real, base_real):
        raise ValueError(
            f"create_plashet_symlink: destination parent {dest_parent_real!r} resolves outside "
            f"base_dir {base_real!r}; refusing to clear a path outside this repo."
        )

    # Never clear a destination that is, contains, or is contained by the symlink target -- doing
    # so would delete the very content we are linking to. Compare canonical (realpath) locations.
    if dest_real == target_real or _is_within(target_real, dest_real) or _is_within(dest_real, target_real):
        raise ValueError(
            f"create_plashet_symlink: destination {dest_real!r} overlaps the symlink target "
            f"{target_real!r}; refusing to create a self-referencing symlink."
        )

    # Clear any pre-existing destination. Unlink symlinks (including dangling ones) and regular
    # files; shutil.rmtree only real directories (it rejects directory symlinks on py3.11).
    if repo_path.is_symlink() or repo_path.is_file():
        repo_path.unlink()
    elif repo_path.exists():
        shutil.rmtree(repo_path)
    repo_path.parent.mkdir(parents=True, exist_ok=True)
    # Compute the target relative to the symlink's own directory so it is portable
    # across the local working dir and the remote ocp-artifacts layout. Build the relative path
    # from the already-computed RESOLVED locations (realpaths), not from lexical abspaths: an
    # ancestor directory of the destination can itself be a symlink inside base_dir (which still
    # passes the guards above), and a lexical relpath would then point the link at the wrong place.
    # Using the resolved dest_parent_real/target_real keeps the relative link correct in that case.
    rel_target = os.path.relpath(target_real, start=dest_parent_real)
    repo_path.symlink_to(rel_target, target_is_directory=True)
    return os.path.abspath(repo_path)


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
        # --links preserves symlinks as symlinks. --force lets rsync replace a pre-existing
        # non-empty remote DIRECTORY with a symlink: an embargoed-only run may have previously
        # published a full revision directory to the remote, and a later retry that takes the
        # symlink path must be able to overwrite that directory with the symlink. Without --force
        # rsync refuses to delete the non-empty directory and the transfer fails.
        # NOTE: only the --links branch needs this; --copy-links dereferences symlinks into real
        # content and must stay unchanged. We deliberately do NOT add --delete.
        cmd.append('--links')
        cmd.append('--force')
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

    # Clean up old revision directories on the remote, keeping the 3 most recent.
    keep = 3
    await run_cleanup_script(plashet_remote_host, remote_base_dir, keep=keep, dry_run=dry_run)


CLEANUP_SCRIPT = Path(__file__).parent / 'cleanup_plashets.sh'


async def run_cleanup_script(
    plashet_remote_host: str,
    remote_base_dir: os.PathLike,
    keep: int = 3,
    dry_run: bool = False,
):
    """
    Run cleanup_plashets.sh on a remote host via SSH, piping the script via stdin
    to avoid shell quoting issues.
    """
    script = CLEANUP_SCRIPT.read_text()
    args = [str(remote_base_dir), str(keep)]
    if dry_run:
        args.append("--dry-run")

    cmd = ["ssh", plashet_remote_host, "--", "bash", "-s", "--"] + args
    logger.info(
        "Cleaning up old plashet directories on %s:%s (keeping last %d)%s",
        plashet_remote_host,
        remote_base_dir,
        keep,
        " [DRY RUN]" if dry_run else "",
    )
    proc = await asyncio.subprocess.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=os.environ.copy(),
    )
    stdout, stderr = await proc.communicate(input=script.encode())
    if stdout:
        logger.info("Cleanup output from %s:\n%s", plashet_remote_host, stdout.decode().rstrip())
    if stderr:
        logger.warning("Cleanup stderr from %s:\n%s", plashet_remote_host, stderr.decode().rstrip())
    if proc.returncode != 0:
        logger.warning(
            "Cleanup script failed on %s:%s with exit code %d",
            plashet_remote_host,
            remote_base_dir,
            proc.returncode,
        )
