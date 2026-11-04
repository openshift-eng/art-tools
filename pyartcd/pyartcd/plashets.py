import asyncio
import logging
import os
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


def require_published_baseline_symlink(plashet_config, repo_name: str) -> None:
    """
    Fail-closed guard for --package-filter runs: a published baseline symlink is required to merge into.

    The seeding step downloads the baseline's plashet.yml from the published ``latest`` symlink. If the plashet
    config does not publish a symlink (``create_symlinks`` is false or ``symlink_name`` is empty), there is no
    stable baseline to merge into, so reject the filtered build instead of seeding a non-existent symlink.

    :param plashet_config: the PlashetConfig for this run.
    :param repo_name: the repo name, for a clearer error message.
    :raises ValueError: when the config does not publish a baseline symlink.
    """
    if not plashet_config.create_symlinks or not plashet_config.symlink_name:
        raise ValueError(
            f"--package-filter requires a published baseline symlink to merge into, but plashet config for repo "
            f"'{repo_name}' does not publish one (create_symlinks={plashet_config.create_symlinks}, "
            f"symlink_name={plashet_config.symlink_name!r}). Refusing the filtered build."
        )


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
    package_filter: Sequence[str] = (),
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
    :param package_filter: (optional) run-level override. When set, the plashet run is scoped to just these
        package names and MERGED into the existing plashet instead of a full regeneration. Empty = full run.

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

        # Package-filter mode merges into the current published plashet, so point the seeding step at the
        # remote "latest" baseline for this slug. The baseline's plashet.yml is downloaded locally before the
        # doozer run (see build_plashet_from_tags / seed_plashet_baseline).
        seed_latest_from = None
        if package_filter:
            # The merge seeds from the published baseline symlink. If this config does not publish one, there
            # is no stable baseline to merge into -- fail closed rather than seeding a non-existent "latest".
            require_published_baseline_symlink(plashet_config, repo.name)
            remote_base_dir = Path(constants.PLASHET_REMOTE_BASE_DIR, base_dir_name)
            seed_host = PLASHET_REMOTES[0]['host']
            seed_latest_from = f"{seed_host}:{remote_base_dir}/{plashet_config.symlink_name}"

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
            package_filter=package_filter,
            seed_latest_from=seed_latest_from,
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
    package_filter: Optional[Sequence[str]] = None,
    seed_latest_from: Optional[str] = None,
    repo_subdir: str | None = 'os',
    poll_for: int = 0,
    data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_working: str = 'doozer-working',
    dry_run: bool = False,
    version: str | None = None,
):
    """
    Builds Plashet repo with "from-tags"

    :param seed_latest_from: (package-filter only) rsync spec (``host:/path/to/latest``) of the current
        published plashet baseline. When set together with ``package_filter``, the baseline's plashet.yml is
        downloaded into the new local plashet dir BEFORE doozer runs so the doozer --package-filter merge can
        preserve the unfiltered packages. doozer fails closed if this baseline is missing.
    """

    repo_path = Path(base_dir, name)
    # In package-filter mode we MERGE into the existing plashet, so we must NOT wipe it. Without a filter we
    # keep the original behavior of starting from a clean directory for a full regeneration.
    if repo_path.exists() and not package_filter:
        shutil.rmtree(repo_path)

    # Package-filter mode: seed the destination with the current "latest" plashet baseline (its plashet.yml,
    # which is all the doozer merge needs: symlinks and repodata are rebuilt from brewroot) BEFORE invoking
    # doozer. This is what lets doozer's fail-closed --package-filter merge find a baseline to merge into.
    if package_filter and seed_latest_from:
        await seed_plashet_baseline(seed_latest_from, repo_path, dry_run=dry_run)

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
    for pkg in package_filter or []:
        cmd.extend(["--package-filter", pkg])
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
        repo_path.mkdir(parents=True, exist_ok=True)
        logger.warning("[Dry run] Would have run %s", cmd)
    else:
        logger.info("Executing %s", cmd)
        await exectools.cmd_assert_async(cmd, env=os.environ.copy())
    return os.path.abspath(Path(base_dir, name))


async def seed_plashet_baseline(seed_latest_from: str, dest_dir: os.PathLike, dry_run: bool = False):
    """
    Download the current "latest" plashet baseline manifest (plashet.yml) from ocp-artifacts into ``dest_dir``
    so a subsequent doozer --package-filter run has an existing baseline to MERGE its filtered packages into.

    Only plashet.yml is fetched: it is the sole input the doozer merge needs (it rebuilds the per-arch symlinks
    and repodata from brewroot for the combined NVRE set). If ``latest`` does not exist on the remote (e.g. a
    brand-new slug that has never been built), the rsync fails and the error propagates -- doozer would then also
    fail closed. Honors ``dry_run``.

    :param seed_latest_from: rsync source of the latest baseline dir, e.g. ``ocp-artifacts:/mnt/.../latest``.
    :param dest_dir: local plashet dir to seed (the doozer destination).
    :param dry_run: when True, only log the command that would run.
    """
    dest_dir = Path(dest_dir)
    cmd = [
        "rsync",
        "-avz",
        "--copy-links",
        "--",
        f"{seed_latest_from.rstrip('/')}/plashet.yml",
        f"{dest_dir}/plashet.yml",
    ]
    if dry_run:
        # Do not log the remote host/path (redacted); log a generic message only.
        logger.warning("[DRY RUN] Would have seeded the current 'latest' plashet baseline (plashet.yml)")
        return
    dest_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Seeding the current 'latest' plashet baseline (plashet.yml) for package-filter merge")
    await exectools.cmd_assert_async(cmd, env=os.environ.copy())


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
