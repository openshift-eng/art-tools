import atexit
import datetime
import io
import itertools
import os
import pathlib
import re
import shutil
import signal
import tempfile
import time
import urllib.parse
from contextlib import contextmanager
from multiprocessing import Lock, RLock
from typing import Dict, List, Optional, Tuple, Union

import click
import yaml
from artcommonlib import exectools, gitdata
from artcommonlib.assembly import (
    AssemblyTypes,
    assembly_basis_event,
    assembly_streams_config,
    assembly_type,
)
from artcommonlib.config import BuildDataLoader
from artcommonlib.config.plashet import PlashetConfig
from artcommonlib.config.repo import ContentSet, Repo, RepoList, RepoSync
from artcommonlib.konflux.konflux_build_record import KonfluxRecord
from artcommonlib.model import Missing, Model
from artcommonlib.pushd import Dir
from artcommonlib.runtime import GroupRuntime
from artcommonlib.util import deep_merge, isolate_el_version_in_brew_tag
from jira import JIRA
from semver import Version

from doozerlib import brew, dblib, state, util
from doozerlib.brew import brew_event_from_datetime
from doozerlib.build_status_detector import BuildStatusDetector
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.record_logger import RecordLogger
from doozerlib.repos import Repos
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.source_resolver import SourceResolver

# Values corresponds to schema for group.yml: freeze_automation. When
# 'yes', doozer itself will inhibit build/rebase related activity
# (exiting with an error if someone tries). Other values can
# be interpreted & enforced by the build pipelines (e.g. by
# invoking config:read-config).
FREEZE_AUTOMATION_YES = 'yes'
FREEZE_AUTOMATION_SCHEDULED = 'scheduled'  # inform the pipeline that only manually run tasks should be permitted
FREEZE_AUTOMATION_NO = 'no'


# doozer cancel brew builds on SIGINT (Ctrl-C)
# but Jenkins sends a SIGTERM when cancelling a job.
def handle_sigterm(*_):
    raise KeyboardInterrupt()


signal.signal(signal.SIGTERM, handle_sigterm)


def remove_tmp_working_dir(runtime):
    if runtime.remove_tmp_working_dir:
        shutil.rmtree(runtime.working_dir)
    else:
        click.echo("Temporary working directory preserved by operation: %s" % runtime.working_dir)


class Runtime(GroupRuntime):
    # Use any time it is necessary to synchronize feedback from multiple threads.
    mutex = RLock()

    # Serialize access to the shared koji session
    koji_lock = RLock()

    # Build status detector lock
    bs_lock = RLock()

    # Serialize access to the console, and record log
    log_lock = Lock()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # initialize defaults in case no value is given
        self.verbose = False
        self.load_wip = False
        self.load_disabled = False
        self.data_path = None
        self.data_dir = None
        self.group_commitish = None
        self.latest_parent_version = False
        self.rhpkg_config = None
        self._koji_client_session = None
        self.db = None
        self.session_pool = {}
        self.session_pool_available = {}
        self.brew_event = None
        self.assembly_basis_event = None
        self.assembly_type = None
        self.releases_config = None
        self.assembly = 'test'
        self.local = False
        self.stage = False
        self.upcycle = False
        self._build_status_detector = None
        self.disable_gssapi = False
        self._build_data_product_cache: Model = None

        # init cli options
        self.group = None
        self.cache_dir = None
        self.arches = None
        self.branch = None
        self.releases = None
        self.lock_runtime_uuid = None
        self.datastore = None
        self.enable_assemblies = None
        self.source = None
        self.sources = None
        self.exclude = None
        self.images = None
        self.rpms = None
        self.user = None
        self.global_opts = None
        self.cfg_obj = None

        self.stream: List[str] = []  # Click option. A list of image stream overrides from the command line.
        self.stream_overrides: Dict[str, str] = {}  # Dict of stream name -> pullspec from command line.

        self.upstreams: List[str] = []  # Click option. A list of upstream source commit to use.
        self.upstream_commitish_overrides: Dict[
            str, str
        ] = {}  # Dict from distgit key name to upstream source commit to use.

        self.downstreams: List[str] = []  # Click option. A list of distgit commits to checkout.
        self.downstream_commitish_overrides: Dict[
            str, str
        ] = {}  # Dict from distgit key name to distgit commit to check out.

        self._logger = None

        for key, val in kwargs.items():
            self.__dict__[key] = val

        self.network_mode_override = None

        if self.latest_parent_version:
            self.ignore_missing_base = True

        self._remove_tmp_working_dir = False
        self._group_config = None
        self.product = None

        self.cwd = os.getcwd()

        # If source needs to be cloned by oit directly, the directory in which it will be placed.
        self.sources_dir = None

        self.distgits_dir = None

        # A record logger writes record.log file
        self.record_logger = None

        self.brew_logs_dir = None

        self.flags_dir = None

        # Map of dist-git repo name -> ImageMetadata object. Populated when group is set.
        self.image_map: Dict[str, ImageMetadata] = {}

        # Map of dist-git repo name -> RPMMetadata object. Populated when group is set.
        self.rpm_map: Dict[str, RPMMetadata] = {}

        # Maps component name to the Image or RPM Metadata responsible for the component
        self.component_map: Dict[str, Union[ImageMetadata, RPMMetadata]] = dict()

        # Map of source code repo aliases (e.g. "ose") to a tuple representing the source resolution cache.
        # See registry_repo.
        self.source_resolutions = {}

        # Default source resolver for legacy functions
        self.source_resolver: Optional[SourceResolver] = None

        # Map of source code repo aliases (e.g. "ose") to a (public_upstream_url, public_upstream_branch) tuple.
        # See registry_repo.
        self.public_upstreams = {}

        # Will be loaded with the streams.yml Model
        self.streams = Model(dict_to_model={})

        self.uuid = None

        # Optionally available if self.fetch_rpms_for_tag() is called
        self.rpm_list = None
        self.rpm_search_tree = None

        # Used for image build ordering
        self.image_tree = {}
        self.image_order = []
        # allows mapping from name or distgit to meta
        self.image_name_map = {}
        # allows mapping from name in bundle to meta
        self.name_in_bundle_map: Dict[str, str] = {}

        # holds untouched group config
        self.raw_group_config = {}

        # Used to capture missing packages for 4.x build
        self.missing_pkgs = set()

        # Whether to prevent builds for this group. Defaults to 'no'.
        self.freeze_automation = FREEZE_AUTOMATION_NO

        self.rhpkg_config_lst = []
        if self.rhpkg_config:
            if not os.path.isfile(self.rhpkg_config):
                raise DoozerFatalError('--rhpkg-config option given is not a valid file! {}'.format(self.rhpkg_config))
            self.rhpkg_config = ' --config {} '.format(self.rhpkg_config)
            self.rhpkg_config_lst = self.rhpkg_config.split()
        else:
            self.rhpkg_config = ''

    def get_releases_config(self):
        if self.releases_config is None:
            self.releases_config = Model(self._build_data_loader.load_releases_config(self.releases))
        return self.releases_config

    @property
    def group_config(self):
        return self._group_config

    @group_config.setter
    def group_config(self, config: Model):
        self._group_config = config

    def get_group_config(self) -> Model:
        replace_vars = self.get_replace_vars(None)
        group_config = self._build_data_loader.load_group_config(
            self.assembly,
            self.get_releases_config(),
            additional_vars=replace_vars,
        )
        return Model(group_config)

    def get_errata_config(self):
        replace_vars = self.get_replace_vars(self.group_config)
        return self._build_data_loader.load_config("erratatool", default={}, replace_vars=replace_vars)

    def get_plashet_config(self):
        plashet_config_dict = self.group_config.get('plashet')
        if not plashet_config_dict:
            return None
        return PlashetConfig.model_validate(plashet_config_dict)

    def _get_repos_config(self) -> RepoList:
        """
        Get repository configuration from group_config.

        Supports both new-style (all_repos) and old-style (repos) configurations:
        - New-style: repos are defined in separate YAML files under repos/ directory
          and loaded via !include in group.ext.yml as group_config['all_repos']
        - Old-style: repos are defined directly in group.yml under the 'repos' key
          and are converted to new-style RepoList format

        :return: RepoList containing repo configurations
        """
        if 'all_repos' in self.group_config:
            # New-style repo config: repos are defined in separate files and included via group.ext.yml
            self._logger.info("Using new-style repo configurations from all_repos")
            all_repos = self.group_config['all_repos']

            # Parse using Pydantic models
            repo_list = RepoList.model_validate(all_repos)
            self._logger.info(f"Loaded {len(repo_list.root)} repos from all_repos")
            return repo_list
        else:
            # Old-style repo config: repos are defined in group.yml
            # Convert to new-style format using Pydantic models
            self._logger.info("Using old-style repo configurations from group.yml, converting to new-style format")
            old_repos = self.group_config.repos
            new_repos = []

            for repo_name, repo_data in old_repos.items():
                # Parse content_set and reposync if present
                content_set = None
                if 'content_set' in repo_data:
                    content_set = ContentSet.model_validate(repo_data['content_set'])

                reposync = RepoSync()
                if 'reposync' in repo_data:
                    reposync = RepoSync.model_validate(repo_data['reposync'])

                # Create Repo object using constructor
                repo = Repo(
                    name=repo_name,
                    type='external',
                    disabled=False,
                    conf=repo_data.get('conf'),
                    content_set=content_set,
                    reposync=reposync,
                )
                new_repos.append(repo)

            repo_list = RepoList.model_validate(new_repos)
            self._logger.info(f"Converted {len(repo_list.root)} old-style repos to new-style format")
            return repo_list

    def get_replace_vars(self, group_config: Model | None):
        replace_vars: dict = group_config.vars.primitive() if group_config and group_config.vars else {}
        # If assembly mode is enabled, `runtime_assembly` will become the assembly name.
        replace_vars['runtime_assembly'] = ''
        # If running against an assembly for a named release, release_name will become the release name.
        replace_vars['release_name'] = ''
        if self.assembly:
            replace_vars['runtime_assembly'] = self.assembly
            if self.assembly_type is not AssemblyTypes.STREAM:
                release_name = replace_vars['release_name'] = util.get_release_name_for_assembly(
                    self.group, self.get_releases_config(), self.assembly
                )
                # for example: replace_vars = {'CVES': 'None', 'IMPACT': 'Low', 'MAJOR': 4, 'MINOR': 12, 'RHCOS_EL_MAJOR': 8, 'RHCOS_EL_MINOR': 6, 'release_name': '4.12.77', 'runtime_assembly': '4.12.77'}
                if 'PATCH' not in replace_vars:
                    replace_vars['PATCH'] = Version.parse(release_name).patch
        return replace_vars

    def init_state(self):
        self.state = dict(state.TEMPLATE_BASE_STATE)
        if os.path.isfile(self.state_file):
            with io.open(self.state_file, 'r', encoding='utf-8') as f:
                self.state = yaml.full_load(f)

    def save_state(self):
        with io.open(self.state_file, 'w', encoding='utf-8') as f:
            yaml.safe_dump(self.state, f, default_flow_style=False)

    def initialize(
        self,
        mode='images',
        clone_distgits=True,
        validate_content_sets=False,
        no_group=False,
        clone_source=None,
        disabled=None,
        prevent_cloning: bool = False,
        config_only: bool = False,
        group_only: bool = False,
        build_system: str = None,
    ):
        if self.initialized:
            return

        if self.working_dir is None:
            self.working_dir = tempfile.mkdtemp(".tmp", "oit-")
            # This can be set to False by operations which want the working directory to be left around
            self.remove_tmp_working_dir = True
            atexit.register(remove_tmp_working_dir, self)
        else:
            self.working_dir = os.path.abspath(os.path.expanduser(self.working_dir))
            if not os.path.isdir(self.working_dir):
                os.makedirs(self.working_dir)

        super().initialize(build_system)

        if self.quiet and self.verbose:
            click.echo("Flags --quiet and --verbose are mutually exclusive")
            exit(1)

        self.mode = mode

        # We could mark these as required and the click library would do this for us,
        # but this seems to prevent getting help from the various commands (unless you
        # specify the required parameters). This can probably be solved more cleanly, but TODO
        if not no_group and self.group is None:
            click.echo("Group must be specified")
            exit(1)

        if self.lock_runtime_uuid:
            self.uuid = self.lock_runtime_uuid
        else:
            self.uuid = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d.%H%M%S")

        self.distgits_dir = os.path.join(self.working_dir, "distgits")
        self.distgits_diff_dir = os.path.join(self.working_dir, "distgits-diffs")
        self.sources_dir = os.path.join(self.working_dir, "sources")
        self.record_log_path = os.path.join(self.working_dir, "record.log")
        self.brew_logs_dir = os.path.join(self.working_dir, "brew-logs")
        self.flags_dir = os.path.join(self.working_dir, "flags")
        self.state_file = os.path.join(self.working_dir, 'state.yaml')

        if self.upcycle:
            # A working directory may be upcycle'd numerous times.
            # Don't let anything grow unbounded.
            shutil.rmtree(self.brew_logs_dir, ignore_errors=True)
            shutil.rmtree(self.flags_dir, ignore_errors=True)
            for path in (self.record_log_path, self.state_file, self.debug_log_path):
                if os.path.exists(path):
                    os.unlink(path)

        if not os.path.isdir(self.distgits_dir):
            os.mkdir(self.distgits_dir)

        if not os.path.isdir(self.distgits_diff_dir):
            os.mkdir(self.distgits_diff_dir)

        if not os.path.isdir(self.sources_dir):
            os.mkdir(self.sources_dir)

        if disabled is not None:
            self.load_disabled = disabled

        self.init_state()

        try:
            self.db = dblib.DB(self, self.datastore)
        except Exception as err:
            self._logger.warning('Cannot connect to the DB: %s', str(err))

        self._logger.info(f'Initial execution (cwd) directory: {os.getcwd()}')

        if no_group:
            return  # nothing past here should be run without a group

        if '@' in self.group:
            self.group, self.group_commitish = self.group.split('@', 1)
        elif self.group_commitish is None:
            self.group_commitish = self.group

        if group_only:
            return

        # For each "--stream alias image" on the command line, register its existence with
        # the runtime.
        for s in self.stream:
            self.register_stream_override(s[0], s[1])

        for upstream in self.upstreams:
            override_distgit_key = upstream[0]
            override_commitish = upstream[1]
            self._logger.warning(f'Upstream source for {override_distgit_key} being set to {override_commitish}')
            self.upstream_commitish_overrides[override_distgit_key] = override_commitish

        for upstream in self.downstreams:
            override_distgit_key = upstream[0]
            override_commitish = upstream[1]
            self._logger.warning(
                f'Downstream distgit for {override_distgit_key} will be checked out to {override_commitish}'
            )
            self.downstream_commitish_overrides[override_distgit_key] = override_commitish

        self.resolve_metadata()

        self.record_logger = RecordLogger(self.record_log_path)

        # Directory where brew-logs will be downloaded after a build
        if not os.path.isdir(self.brew_logs_dir):
            os.mkdir(self.brew_logs_dir)

        # Directory for flags between invocations in the same working-dir
        if not os.path.isdir(self.flags_dir):
            os.mkdir(self.flags_dir)

        if self.cache_dir:
            self.cache_dir = os.path.abspath(self.cache_dir)

        # get_releases_config also inits self.releases_config
        self.assembly_type = assembly_type(self.get_releases_config(), self.assembly)
        self.group_config = self.get_group_config()

        if self.group_config.name != self.group:
            raise IOError(
                f"Name in group.yml ({self.group_config.name}) does not match group name ({self.group}). Someone "
                "may have copied this group without updating group.yml (make sure to check branch)"
            )
        self.product = self.group_config.product or "ocp"

        self.hotfix = (
            False  # True indicates builds should be tagged with associated hotfix tag for the artifacts branch
        )

        if self.group_config.assemblies.enabled or self.enable_assemblies:
            if re.fullmatch(r'[\w.-]+', self.assembly) is None or self.assembly[0] == '.' or self.assembly[-1] == '.':
                raise ValueError(
                    'Assembly names may only consist of alphanumerics, ., and _, but not start or end with a dot (.).'
                )
        else:
            # If assemblies are not enabled for the group,
            # ignore this argument throughout doozer.
            self.assembly = None

        replace_vars = self.get_replace_vars(self.group_config)

        # only initialize group and assembly configs and nothing else
        if config_only:
            return

        # Read in the streams definition for this group if one exists
        streams_data = self._build_data_loader.load_config("streams", default={}, replace_vars=replace_vars)
        if streams_data:
            org_stream_model = Model(dict_to_model=streams_data)
            self.streams = assembly_streams_config(self.get_releases_config(), self.assembly, org_stream_model)

        strict_mode = True
        if (
            self.assembly_type == AssemblyTypes.STREAM
            or not self.assembly
            or self.assembly in ['stream', 'test', 'microshift']
        ):
            strict_mode = False

        self.assembly_basis_event = assembly_basis_event(
            self.get_releases_config(), self.assembly, strict=strict_mode, build_system=self.build_system
        )
        if self.assembly_basis_event:
            if self.brew_event:
                raise IOError(
                    f'Cannot run with assembly basis event {self.assembly_basis_event} and --brew-event at the same time.'
                )
            # If the assembly has a basis event, we constrain all brew calls to that event.
            if isinstance(self.assembly_basis_event, int):
                # The assembly basis event is a Brew event
                self.brew_event = self.assembly_basis_event

            else:
                # The assembly basis event for Konflux is a timestamp, e.g. 2025-04-15 13:28:09
                # Use koji.getLatestEvent() to get the latest Brew event that came before the assembly Konflux event
                self._logger.info('Computed assembly basis event: %s', self.assembly_basis_event)
                with self.shared_koji_client_session() as koji_api:
                    self.brew_event = brew_event_from_datetime(self.assembly_basis_event, koji_api)

            self._logger.info(f'Constraining brew event to assembly basis for {self.assembly}: {self.brew_event}')

        # This flag indicates builds should be tagged with associated hotfix tag for the artifacts branch
        self.hotfix = self.assembly_type is not AssemblyTypes.STREAM

        # Instantiate the default source resolver
        if 'source_alias' not in self.state:
            self.state['source_alias'] = {}
        self.source_resolver = SourceResolver(
            sources_base_dir=self.sources_dir,
            cache_dir=self.git_cache_dir,
            group_config=self.group_config,
            local=self.local,
            upcycle=self.upcycle,
            stage=self.stage,
            record_logger=self.record_logger,
            state_holder=self.state["source_alias"],
        )

        if not self.brew_event:
            self._logger.info("Basis brew event is not set. Using the latest event....")
            with self.shared_koji_client_session() as koji_session:
                # If brew event is not set as part of the assembly and not specified on the command line,
                # lock in an event so that there are no race conditions.
                self._logger.info("Getting the latest event....")
                event_info = koji_session.getLastEvent()
                self.brew_event = event_info['id']

        # register the sources
        # For each "--source alias path" on the command line, register its existence with
        # the runtime.
        for r in self.source:
            self.source_resolver.register_source_alias(r[0], r[1])

        if self.sources:
            with io.open(self.sources, 'r', encoding='utf-8') as sf:
                source_dict = yaml.full_load(sf)
                if not isinstance(source_dict, dict):
                    raise ValueError('--sources param must be a yaml file containing a single dict.')
                for key, val in source_dict.items():
                    self.source_resolver.register_source_alias(key, val)

        with Dir(self.data_dir):
            # Flattens multiple comma/space delimited lists like [ 'x', 'y,z' ] into [ 'x', 'y', 'z' ]
            def flatten_list(names):
                if not names:
                    return []
                # split csv values
                result = []
                for n in names:
                    result.append([x for x in n.replace(' ', ',').split(',') if x != ''])
                # flatten result and remove dupes using set
                return list(set([y for x in result for y in x]))

            def filter_wip(n, d):
                return d.get('mode', 'enabled') in ['wip', 'enabled']

            def filter_enabled(n, d):
                mode = d.get('mode', 'enabled')
                # Include if generally enabled OR has okd.mode: enabled
                if mode == 'enabled':
                    return True
                if mode == 'disabled':
                    okd_config = d.get('okd', {})
                    if isinstance(okd_config, dict) and okd_config.get('mode') == 'enabled':
                        return True
                return False

            def filter_disabled(n, d):
                return d.get('mode', 'enabled') in ['enabled', 'disabled']

            cli_arches_override = flatten_list(self.arches)

            if cli_arches_override:  # Highest priority overrides on command line
                self.arches = cli_arches_override
            elif (
                self.group_config.arches_override
            ):  # Allow arches_override in group.yaml to temporarily override GA architectures
                self.arches = self.group_config.arches_override
            else:
                self.arches = self.group_config.get('arches', ['x86_64'])

            # If specified, signed repo files will be generated to enforce signature checks.
            self.gpgcheck = self.group_config.build_profiles.image.signed.gpgcheck
            if self.gpgcheck is Missing:
                # We should only really be building the latest release with unsigned RPMs, so default to True
                self.gpgcheck = True

            # Load repos configuration: new-style from all_repos or old-style from group.yml
            repos_config = self._get_repos_config()
            plashet_config = self.get_plashet_config()
            self.repos = Repos(
                repos_config, self.arches, self.gpgcheck, plashet_config=plashet_config, template_vars=replace_vars
            )
            self.freeze_automation = self.group_config.freeze_automation or FREEZE_AUTOMATION_NO  # type: ignore

            if validate_content_sets:
                # as of 2023-06-09 authentication is required to validate content sets with rhsm-pulp
                if not os.environ.get("RHSM_PULP_KEY") or not os.environ.get("RHSM_PULP_CERT"):
                    self._logger.warn("Missing RHSM_PULP auth, will skip validating content sets")
                else:
                    self.repos.validate_content_sets()

            if self.branch is None:
                if self.group_config.branch is not Missing:
                    self.branch = self.group_config.branch
                    self._logger.info("Using branch from group.yml: %s" % self.branch)
                else:
                    self._logger.info(
                        "No branch specified either in group.yml or on the command line; all included images will need to specify their own."
                    )
            else:
                self._logger.info("Using branch from command line: %s" % self.branch)

            scanner = self.group_config.image_build_log_scanner
            if scanner is not Missing:
                # compile regexen and fail early if they don't
                regexen = []
                for val in scanner.matches:
                    try:
                        regexen.append(re.compile(val))
                    except Exception as e:
                        raise ValueError(
                            "could not compile image build log regex for group:\n{}\n{}".format(val, e),
                        )
                scanner.matches = regexen

            exclude_keys = flatten_list(self.exclude)
            image_ex = list(exclude_keys)
            rpm_ex = list(exclude_keys)
            image_keys = flatten_list(self.images)

            rpm_keys = flatten_list(self.rpms)

            filter_func = None
            if self.load_wip and self.load_disabled:
                pass  # use no filter, load all
            elif self.load_wip:
                filter_func = filter_wip
            elif self.load_disabled:
                filter_func = filter_disabled
            else:
                filter_func = filter_enabled

            # pre-load the image data to get the names for all images
            # eventually we can use this to allow loading images by
            # name or distgit. For now this is used elsewhere
            image_name_data = self.gitdata.load_data(path='images')

            def _register_name_in_bundle(name_in_bundle: str, distgit_key: str):
                if name_in_bundle in self.name_in_bundle_map:
                    raise ValueError(
                        f"Image {distgit_key} has name_in_bundle={name_in_bundle}, which is already taken by image {self.name_in_bundle_map[name_in_bundle]}"
                    )
                self.name_in_bundle_map[name_in_bundle] = img.key

            for img in image_name_data.values():
                name = img.data.get('name')
                short_name = name.split('/')[1]
                self.image_name_map[name] = img.key
                self.image_name_map[short_name] = img.key
                name_in_bundle = img.data.get('name_in_bundle')
                if name_in_bundle:
                    _register_name_in_bundle(name_in_bundle, img.key)
                else:
                    short_name_without_ose = short_name[4:] if short_name.startswith("ose-") else short_name
                    _register_name_in_bundle(short_name_without_ose, img.key)
                    short_name_with_ose = "ose-" + short_name_without_ose
                    _register_name_in_bundle(short_name_with_ose, img.key)

            image_data = self.gitdata.load_data(
                path='images',
                keys=image_keys,
                exclude=image_ex,
                replace_vars=replace_vars,
                filter_funcs=None if len(image_keys) else filter_func,
            )

            try:
                rpm_data = self.gitdata.load_data(
                    path='rpms',
                    keys=rpm_keys,
                    exclude=rpm_ex,
                    replace_vars=replace_vars,
                    filter_funcs=None if len(rpm_keys) else filter_func,
                )
            except gitdata.GitDataPathException:
                # some older versions have no RPMs, that's ok.
                rpm_data = {}

            missed_include = set(image_keys + rpm_keys) - set(list(image_data.keys()) + list(rpm_data.keys()))
            if len(missed_include) > 0:
                raise DoozerFatalError(
                    'The following images or rpms were either missing or filtered out: {}'.format(
                        ', '.join(missed_include)
                    )
                )

            if mode in ['images', 'both']:
                for i in image_data.values():
                    if i.key not in self.image_map:
                        metadata = ImageMetadata(
                            self,
                            i,
                            self.upstream_commitish_overrides.get(i.key),
                            clone_source=clone_source,
                            prevent_cloning=prevent_cloning,
                        )
                        self.image_map[metadata.distgit_key] = metadata
                        self.component_map[metadata.get_component_name()] = metadata
                if not self.image_map:
                    self._logger.warning(
                        "No image metadata directories found for given options within: {}".format(self.data_dir)
                    )

                for image in self.image_map.values():
                    image.resolve_parent()

                # now that ancestry is defined, make sure no cyclic dependencies
                for image in self.image_map.values():
                    for child in image.children:
                        if image.is_ancestor(child):
                            raise DoozerFatalError(
                                '{} cannot be both a parent and dependent of {}'.format(
                                    child.distgit_key, image.distgit_key
                                )
                            )

                self.generate_image_tree()

            if mode in ['rpms', 'both']:
                for r in rpm_data.values():
                    if clone_source is None:
                        # Historically, clone_source defaulted to True for rpms.
                        clone_source = True
                    metadata = RPMMetadata(
                        self,
                        r,
                        self.upstream_commitish_overrides.get(r.key),
                        clone_source=clone_source,
                        prevent_cloning=prevent_cloning,
                    )
                    self.rpm_map[metadata.distgit_key] = metadata
                    self.component_map[metadata.get_component_name()] = metadata
                if not self.rpm_map:
                    self._logger.warning(
                        "No rpm metadata directories found for given options within: {}".format(self.data_dir)
                    )

        # Make sure that the metadata is not asking us to check out the same exact distgit & branch.
        # This would almost always indicate someone has checked in duplicate metadata into a group.
        no_collide_check = {}
        for meta in list(self.rpm_map.values()) + list(self.image_map.values()):
            key = '{}/{}/#{}'.format(meta.namespace, meta.name, meta.branch())
            if key in no_collide_check:
                raise IOError(
                    'Complete duplicate distgit & branch; something wrong with metadata: {} from {} and {}'.format(
                        key, meta.config_filename, no_collide_check[key].config_filename
                    )
                )
            no_collide_check[key] = meta

        if clone_distgits:
            self.clone_distgits()

        self.initialized = True

    def get_bug_config(self):
        replace_vars = self.get_replace_vars(self.group_config)
        bug_config = self._build_data_loader.load_config("bug", default={}, replace_vars=replace_vars)
        return bug_config

    def build_jira_client(self) -> JIRA:
        """
        :return: Returns a JIRA client setup for the server in bug.yaml
        """
        major, minor = self.get_major_minor_fields()
        if (major, minor) < (4, 6):
            raise ValueError("ocp-build-data/bug.yml is not expected to be available for OCP versions < 4.6")
        bug_config = Model(self.get_bug_config())
        server = bug_config.jira_config.server or 'https://issues.redhat.com'

        token_auth = os.environ.get("JIRA_TOKEN")
        if not token_auth:
            raise ValueError(f"Jira activity requires login credentials for {server}. Set a JIRA_TOKEN env var")
        client = JIRA(server, token_auth=token_auth)
        return client

    def build_retrying_koji_client(self):
        """
        :return: Returns a new koji client instance that will automatically retry
        methods when it receives common exceptions (e.g. Connection Reset)
        Honors doozer --brew-event.
        """
        return brew.KojiWrapper([self.group_config.urls.brewhub], brew_event=self.brew_event)

    @contextmanager
    def shared_koji_client_session(self):
        """
        Context manager which offers a shared koji client session. You hold a koji specific lock in this context
        manager giving your thread exclusive access. The lock is reentrant, so don't worry about
        call a method that acquires the same lock while you hold it.
        Honors doozer --brew-event.
        Do not rerun gssapi_login on this client. We've observed client instability when this happens.
        """
        with self.koji_lock:
            if self._koji_client_session is None:
                self._koji_client_session = self.build_retrying_koji_client()
                if not self.disable_gssapi:
                    self._logger.info("Authenticating to Brew...")
                    self._koji_client_session.gssapi_login()
            yield self._koji_client_session

    @contextmanager
    def shared_build_status_detector(self) -> 'BuildStatusDetector':
        """
        Yields a shared build status detector within context.
        """
        with self.bs_lock:
            if self._build_status_detector is None:
                self._build_status_detector = BuildStatusDetector(self, self._logger)
            yield self._build_status_detector

    @contextmanager
    def pooled_koji_client_session(self, caching: bool = False):
        """
        Context manager which offers a koji client session from a limited pool. You hold a lock on this
        session until you return. It is not recommended to call other methods that acquire their
        own pooled sessions, because that may lead to deadlock if the pool is exhausted.
        Honors doozer --brew-event.
        :param caching: Set to True in order for your instance to place calls/results into
                        the global KojiWrapper cache. This is equivalent to passing
                        KojiWrapperOpts(caching=True) in each call within the session context.
        """
        session = None
        session_id = None
        while True:
            with self.mutex:
                if len(self.session_pool_available) == 0:
                    if len(self.session_pool) < 30:
                        # pool has not grown to max size;
                        new_session = self.build_retrying_koji_client()
                        session_id = len(self.session_pool)
                        self.session_pool[session_id] = new_session
                        session = new_session  # This is what we wil hand to the caller
                        break
                    else:
                        # Caller is just going to have to wait and try again
                        pass
                else:
                    session_id, session = self.session_pool_available.popitem()
                    break

            time.sleep(5)

        # Arriving here, we have a session to use.
        try:
            session.force_instance_caching = caching
            yield session
        finally:
            session.force_instance_caching = False
            # Put it back into the pool
            with self.mutex:
                self.session_pool_available[session_id] = session

    @staticmethod
    def timestamp():
        return datetime.datetime.utcnow().isoformat()

    def assert_mutation_is_permitted(self):
        """
        In group.yml, it is possible to instruct doozer to prevent all builds / mutation of distgits.
        Call this method if you are about to mutate anything. If builds are disabled, an exception will
        be thrown.
        """
        if self.freeze_automation == FREEZE_AUTOMATION_YES:
            raise DoozerFatalError(
                'Automation (builds / mutations) for this group is currently frozen (freeze_automation set to {}). Coordinate with the group owner to change this if you believe it is incorrect.'.format(
                    FREEZE_AUTOMATION_YES
                )
            )

    def image_metas(self) -> List[ImageMetadata]:
        return list(self.image_map.values())

    def ordered_image_metas(self) -> List[ImageMetadata]:
        return [self.image_map[dg] for dg in self.image_order]

    def get_global_arches(self):
        """
        :return: Returns a list of architectures that are enabled globally in group.yml.
        """
        return list(self.arches)

    def get_global_konflux_arches(self):
        """
        :return: Returns a list of architectures that are enabled globally in group.yml, for konflux.
        """

        if self.arches:
            # CLI override takes precedence
            arches = list(self.arches)

        else:
            # Use konflux arches if defined
            arches = list(self.group_config.konflux.arches)

        return arches

    def get_product_config(self) -> Model:
        """
        Returns a Model of the product.yml in ocp-build-data main branch.
        """
        if self._build_data_product_cache:
            return self._build_data_product_cache
        url = 'https://raw.githubusercontent.com/openshift-eng/ocp-build-data/main/product.yml'
        req = urllib.request.Request(url)
        req.add_header('Accept', 'application/yaml')
        self._build_data_product_cache = Model(yaml.safe_load(exectools.urlopen_assert(req).read()))
        return self._build_data_product_cache

    def filter_failed_image_trees(self, failed):
        for i in self.ordered_image_metas():
            if i.parent and i.parent.distgit_key in failed:
                failed.append(i.distgit_key)

        for f in failed:
            if f in self.image_map:
                del self.image_map[f]

        # regen order and tree
        self.generate_image_tree()

        return failed

    def generate_image_tree(self):
        self.image_tree = {}
        image_lists = {0: []}

        def add_child_branch(child, branch, level=1):
            if level not in image_lists:
                image_lists[level] = []
            for sub_child in child.children:
                if sub_child.distgit_key not in self.image_map:
                    continue  # don't add images that have been filtered out
                branch[sub_child.distgit_key] = {}
                image_lists[level].append(sub_child.distgit_key)
                add_child_branch(sub_child, branch[sub_child.distgit_key], level + 1)

        for image in self.image_map.values():
            if not image.parent:
                self.image_tree[image.distgit_key] = {}
                image_lists[0].append(image.distgit_key)
                add_child_branch(image, self.image_tree[image.distgit_key])

        levels = list(image_lists.keys())
        levels.sort()
        self.image_order = []
        for level in levels:
            for i in image_lists[level]:
                if i not in self.image_order:
                    self.image_order.append(i)

    def image_distgit_by_name(self, name):
        """Returns image meta by full name, short name, or distgit"""
        return self.image_name_map.get(name, None)

    def rpm_metas(self) -> List[RPMMetadata]:
        return list(self.rpm_map.values())

    def all_metas(self) -> List[Union[ImageMetadata, RPMMetadata]]:
        return self.image_metas() + self.rpm_metas()

    def get_payload_image_metas(self) -> List[ImageMetadata]:
        """
        :return: Returns a list of ImageMetadata that are destined for the OCP release payload. Payload images must
                    follow the correct naming convention or an exception will be thrown.
        """
        payload_images = []
        for image_meta in self.image_metas():
            if image_meta.is_payload:
                """
                <Tim Bielawa> note to self: is only for `ose-` prefixed images
                <Clayton Coleman> Yes, Get with the naming system or get out of town
                """
                if not image_meta.image_name_short.startswith("ose-"):
                    raise ValueError(
                        f"{image_meta.distgit_key} does not conform to payload naming convention with image name: {image_meta.image_name_short}"
                    )

                payload_images.append(image_meta)

        return payload_images

    def get_for_release_image_metas(self) -> List[ImageMetadata]:
        """
        :return: Returns a list of ImageMetada which are configured to be released by errata.
        """
        return filter(lambda meta: meta.for_release, self.image_metas())

    def get_non_release_image_metas(self) -> List[ImageMetadata]:
        """
        :return: Returns a list of ImageMetada which are not meant to be released by errata.
        """
        return filter(lambda meta: not meta.for_release, self.image_metas())

    def register_stream_override(self, name, image):
        self._logger.info("Registering image stream name override %s: %s" % (name, image))
        self.stream_overrides[name] = image

    @property
    def remove_tmp_working_dir(self):
        """
        Provides thread safe method of checking whether runtime should clean up the working directory.
        :return: Returns True if the directory should be deleted
        """
        with self.log_lock:
            return self._remove_tmp_working_dir

    @remove_tmp_working_dir.setter
    def remove_tmp_working_dir(self, remove):
        """
        Provides thread safe method of setting whether runtime should clean up the working directory.
        :param remove: True if the directory should be removed. Only the last value set impacts the decision.
        """
        with self.log_lock:
            self._remove_tmp_working_dir = remove

    def add_distgits_diff(self, distgit, diff):
        """
        Records the diff of changes applied to a distgit repo.
        """
        distgit_path = self.distgits_diff_dir

        with io.open(os.path.join(distgit_path, distgit + '.patch'), 'w', encoding='utf-8') as f:
            f.write(diff)

    def resolve_image(self, distgit_name, required=True) -> ImageMetadata:
        """
        Returns an ImageMetadata for the specified group member name.
        :param distgit_name: The name of an image member in this group
        :param required: If True, raise an exception if the member is not found.
        :return: The ImageMetadata object associated with the name
        """
        if distgit_name not in self.image_map:
            if not required:
                return None
            raise DoozerFatalError("Unable to find image metadata in group / included images: %s" % distgit_name)
        return self.image_map[distgit_name]

    def late_resolve_image(self, distgit_name, add=False, required=True):
        """Resolve image and retrieve meta, optionally adding to image_map.
        If image not found, error will be thrown
        :param distgit_name: Distgit key
        :param add: Add the image to image_map
        :param required: If False, return None if the image is not enabled
        :return: Image meta
        """

        if distgit_name in self.image_map:
            return self.image_map[distgit_name]

        replace_vars = self.get_replace_vars(self.group_config)
        data_obj = self.gitdata.load_data(path='images', key=distgit_name, replace_vars=replace_vars)
        if not data_obj:
            raise DoozerFatalError('Unable to resolve image metadata for {}'.format(distgit_name))

        mode = data_obj.data.get("mode", "enabled")

        # Check if image has OKD mode override that enables it
        okd_config = data_obj.data.get("okd", {})
        okd_enabled = okd_config.get("mode") == "enabled"

        # Skip loading if disabled (unless okd.mode: enabled or load_disabled is set)
        if mode == "disabled" and not self.load_disabled and not okd_enabled:
            if required:
                raise DoozerFatalError('Attempted to load image {} but it has mode {}'.format(distgit_name, mode))
            self._logger.warning("Image %s will not be loaded because it has mode %s", distgit_name, mode)
            return None

        # Skip loading if wip
        if mode == "wip" and not self.load_wip:
            if required:
                raise DoozerFatalError('Attempted to load image {} but it has mode {}'.format(distgit_name, mode))
            self._logger.warning("Image %s will not be loaded because it has mode %s", distgit_name, mode)
            return None

        # Only process dependents if this image will be added to image_map.
        # When add=False, we're just loading the image to query its latest build,
        # so we should not trigger loading its dependents as a side effect.
        meta = ImageMetadata(self, data_obj, self.upstream_commitish_overrides.get(data_obj.key), process_dependents=add)
        if add:
            self.image_map[distgit_name] = meta
        self.component_map[meta.get_component_name()] = meta
        return meta

    def resolve_brew_image_url(self, image_name_and_version):
        """
        :param image_name_and_version: The image name to resolve. The image can contain a version tag or sha.
        :return: Returns the pullspec of this image in brew.
        e.g. "openshift/jenkins:5"  => "registry-proxy.engineering.redhat.com/rh-osbs/openshift-jenkins:5"
        """

        if self.group_config.urls.brew_image_host in image_name_and_version:
            # Seems like a full brew url already
            url = image_name_and_version
        elif self.group_config.urls.brew_image_namespace is not Missing:
            # if there is a namespace, we need to flatten the image name.
            # e.g. openshift/image:latest => openshift-image:latest
            # ref: https://source.redhat.com/groups/public/container-build-system/container_build_system_wiki/pulling_pre_quay_switch_over_osbs_built_container_images_using_the_osbs_registry_proxy
            url = self.group_config.urls.brew_image_host
            ns = self.group_config.urls.brew_image_namespace
            name = image_name_and_version.replace('/', '-')
            url = "/".join((url, ns, name))
        else:
            # If there is no namespace, just add the image name to the brew image host
            url = "/".join((self.group_config.urls.brew_image_host, image_name_and_version))

        if ':' not in url.split('/')[-1]:
            # oc image info will return information about all tagged images. So be explicit
            # in indicating :latest if there is no tag.
            url += ':latest'

        return url

    def resolve_stream(self, stream_name):
        """
        :param stream_name: The name of the stream to resolve. The name can also be a stream name alias.
        :return: Resolves and returns the image stream name into its literal value.
                This is usually a lookup in streams.yml, but can also be overridden on the command line. If
                the stream_name cannot be resolved, an exception is thrown.
        """

        # If the stream has an override from the command line, return it.
        if stream_name in self.stream_overrides:
            return Model(dict_to_model={'image': self.stream_overrides[stream_name]})

        matched_streams = list(
            itertools.islice(
                ((n, s) for n, s in self.streams.items() if stream_name == n or stream_name in s.get('aliases', [])), 2
            )
        )
        if len(matched_streams) == 0:
            raise IOError(f"Unable to find definition for stream '{stream_name}'")
        if len(matched_streams) > 1:
            raise IOError(
                f"Stream name is ambiguous. Found multiple streams with name '{stream_name}': {', '.join([s[0] for s in matched_streams])}"
            )
        return Model(dict_to_model=matched_streams[0][1])

    def get_stream_names(self):
        """
        :return: Returns a list of all streams defined in streams.yaml.
        """
        return list(self.streams.keys())

    @property
    def git_cache_dir(self):
        """Returns the directory where git repos are cached.
        :return: The directory. None if caching is disabled.
        """
        if not self.cache_dir:
            return None
        os.path.join(self.cache_dir, self.user or "default", 'git')

    def export_sources(self, output):
        self._logger.info('Writing sources to {}'.format(output))
        with io.open(output, 'w', encoding='utf-8') as sources_file:
            yaml.dump(
                {k: v.source_path for k, v in self.source_resolutions.items()}, sources_file, default_flow_style=False
            )

    def auto_version(self, repo_type):
        """
        Find and return the version of the atomic-openshift package in the OCP
        RPM repository.

        This repository is the primary input for OCP images.  The group_config
        for a group specifies the location for both signed and unsigned
        rpms.  The caller must indicate which to use.
        """

        repo_url = self.repos['rhel-server-ose-rpms'].baseurl(repo_type, 'x86_64')
        self._logger.info(
            "Getting version from atomic-openshift package in {}".format(repo_url),
        )

        # create a randomish repo name to avoid erroneous cache hits
        repoid = "oit" + datetime.datetime.now().strftime("%s")
        version_query = [
            "/usr/bin/repoquery",
            "--quiet",
            "--tempcache",
            "--repoid",
            repoid,
            "--repofrompath",
            repoid + "," + repo_url,
            "--queryformat",
            "%{VERSION}",
            "atomic-openshift",
        ]
        rc, auto_version, err = exectools.cmd_gather(version_query)
        if rc != 0:
            raise RuntimeError(
                "Unable to get OCP version from RPM repository: {}".format(err),
            )

        version = "v" + auto_version.strip()

        self._logger.info("Auto-detected OCP version: {}".format(version))
        return version

    def valid_version(self, version):
        """
        Check if a version string matches an accepted pattern.
        A single lower-case 'v' followed by one or more decimal numbers,
        separated by a dot.  Examples below are not exhaustive
        Valid:
          v1, v12, v3.4, v2.12.0

        Not Valid:
          1, v1..2, av3.4, .v12  .99.12, v13-55
        """
        return re.match(r"^v\d+((\.\d+)+)?$", version) is not None

    def clone_distgits(self, n_threads=None):
        with exectools.timer(self._logger.info, 'Full runtime clone'):
            if n_threads is None:
                n_threads = self.global_opts['distgit_threads']
            return exectools.parallel_exec(lambda m, _: m.distgit_repo(), self.all_metas(), n_threads=n_threads).get()

    def push_distgits(self, n_threads=None):
        self.assert_mutation_is_permitted()

        if n_threads is None:
            n_threads = self.global_opts['distgit_threads']
        return exectools.parallel_exec(
            lambda m, _: m.distgit_repo().push(), self.all_metas(), n_threads=n_threads
        ).get()

    def get_el_targeted_default_branch(self, el_target: Optional[Union[str, int]] = None):
        if not self.branch:
            return None
        if not el_target:
            return self.branch
        # Otherwise, the caller is asking us to determine the branch for
        # a specific RHEL version. Pull apart the default group branch
        # and replace it wth the targeted version.
        el_ver: int = isolate_el_version_in_brew_tag(el_target)
        match = re.match(r'^(.*)rhel-\d+(.*)$', self.branch)
        el_specific_branch: str = f'{match.group(1)}rhel-{el_ver}{match.group(2)}'
        return el_specific_branch

    def get_default_candidate_brew_tag(self, el_target: Optional[Union[str, int]] = None):
        branch = self.get_el_targeted_default_branch(el_target=el_target)
        return branch + '-candidate' if branch else None

    def get_default_hotfix_brew_tag(self, el_target: Optional[Union[str, int]] = None):
        branch = self.get_el_targeted_default_branch(el_target=el_target)
        return branch + '-hotfix' if branch else None

    def get_candidate_brew_tags(self):
        """Return a set of known candidate tags relevant to this group"""
        tag = self.get_default_candidate_brew_tag()
        # assumptions here:
        # releases with default rhel-7 tag also have rhel 8.
        # releases with default rhel-8 tag do not also care about rhel-7.
        # adjust as needed (and just imagine rhel 9)!
        return {tag, tag.replace('-rhel-7', '-rhel-8')} if tag else set()

    def get_minor_version(self) -> str:
        """
        Returns: "<MAJOR>.<MINOR>" if the vars are defined in the group config.
        """
        return '.'.join(str(self.group_config.vars[v]) for v in ('MAJOR', 'MINOR'))

    def get_major_minor_fields(self) -> Tuple[int, int]:
        """
        Returns: (int(MAJOR), int(MINOR)) if the vars are defined in the group config.
        """
        major = int(self.group_config.vars['MAJOR'])
        minor = int(self.group_config.vars['MINOR'])
        return major, minor

    def resolve_metadata(self):
        """
        The group control data can be on a local filesystem, in a git
        repository that can be checked out, or some day in a database

        If the scheme is empty, assume file:///...
        Allow http, https, ssh and ssh+git (all valid git clone URLs)
        """

        if self.data_path is None:
            raise DoozerFatalError(
                (
                    "No metadata path provided. Must be set via one of:\n"
                    "* data_path key in {}\n"
                    "* doozer --data-path [PATH|URL]\n"
                    "* Environment variable DOOZER_DATA_PATH\n"
                ).format(self.cfg_obj.full_path)
            )

        self.gitdata = gitdata.GitData(
            data_path=self.data_path,
            clone_dir=self.working_dir,
            commitish=self.group_commitish,
            reclone=self.upcycle,
            logger=self._logger,
        )
        self._build_data_loader = BuildDataLoader(
            data_path=self.data_path,
            clone_dir=self.working_dir,
            commitish=self.group_commitish,
            build_system=self.build_system,
            upcycle=self.upcycle,
            gitdata=self.gitdata,
            logger=self._logger,
        )
        self.data_dir = self._build_data_loader.data_dir

    def get_rpm_config(self) -> dict:
        config = {}
        for key, val in self.rpm_map.items():
            config[key] = val.raw_config
        return config
