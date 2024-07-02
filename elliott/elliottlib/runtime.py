import atexit
from contextlib import contextmanager
import logging
import os
import re
import shutil
import tempfile
from multiprocessing import Lock, RLock
import time
from typing import Dict, Optional, Tuple
import warnings

import click
import yaml

from artcommonlib import logutil
from artcommonlib import exectools
from artcommonlib.assembly import AssemblyTypes, assembly_type, assembly_basis_event, assembly_group_config
from artcommonlib.model import Model, Missing
from artcommonlib.runtime import GroupRuntime
from elliottlib import brew, constants, gitdata, util
from elliottlib.exceptions import ElliottFatalError
from elliottlib.imagecfg import ImageMetadata
from elliottlib.rpmcfg import RPMMetadata
from elliottlib.bzutil import BugTracker, BugzillaBugTracker, JIRABugTracker


def remove_tmp_working_dir(runtime):
    if runtime.remove_tmp_working_dir:
        shutil.rmtree(runtime.working_dir)
    else:
        click.echo("Temporary working directory preserved by operation: %s" % runtime.working_dir)


# ============================================================================
# Runtime object definition
# ============================================================================


class Runtime(GroupRuntime):
    # pylint: disable=no-member
    # TODO: analyze/fix pylint no-member violations

    # Use any time it is necessary to synchronize feedback from multiple threads.
    mutex = RLock()

    # Serialize access to the shared koji session
    koji_lock = RLock()

    # Serialize access to the console, and record log
    log_lock = Lock()

    def __init__(self, **kwargs):
        # initialize defaults in case no value is given
        self.verbose = False
        self.quiet = False
        self.data_path = None
        self.load_wip = False
        self.load_disabled = False
        self._logger = None
        self.use_jira = True
        if str(os.environ.get('USEJIRA')).lower() in ["false", "0"]:
            self.use_jira = False
        self._bug_trackers = {}
        self.brew_event: Optional[int] = None
        self.assembly: Optional[str] = 'stream'
        self.assembly_basis_event: Optional[int] = None
        self.releases_config: Optional[Model] = None
        self.assembly_type = AssemblyTypes.STREAM

        for key, val in kwargs.items():
            self.__dict__[key] = val

        self._remove_tmp_working_dir = False
        self._group_config = None
        self.debug_log_path = None

        # Map of dist-git repo name -> ImageMetadata object. Populated when group is set.
        self.image_map: Dict[str, ImageMetadata] = {}

        # Map of dist-git repo name -> RPMMetadata object. Populated when group is set.
        self.rpm_map: Dict[str, RPMMetadata] = {}

        # Shared koji.ClientSession instance
        self._koji_client_session = None
        #
        self.session_pool = {}
        self.session_pool_available = {}

        self.initialized = False

    @property
    def logger(self):
        """ Get the runtime logger of Elliott.
        Your module should generally use `logging.getLogger(__name__)` instead of using this one.
        """
        warnings.warn("Use `logging.getLogger(__name__)` for your module instead of reusing `runtime.logger`", DeprecationWarning)
        return self._logger

    def get_major_minor(self):
        return self.group_config.vars.MAJOR, self.group_config.vars.MINOR

    def get_default_advisories(self):
        return self.group_config.get('advisories', {})

    @property
    def group_config(self):
        return self._group_config

    @group_config.setter
    def group_config(self, config: Model):
        self._group_config = config

    def get_group_config(self):
        # group.yml can contain a `vars` section which should be a
        # single level dict containing keys to str.format(**dict) replace
        # into the YAML content. If `vars` found, the format will be
        # preformed and the YAML model will reloaded from that result
        tmp_config = Model(self.gitdata.load_data(key='group').data)
        replace_vars = tmp_config.vars or Model()
        if self.assembly:
            replace_vars['runtime_assembly'] = self.assembly
        try:
            group_yml = yaml.safe_dump(tmp_config.primitive(), default_flow_style=False)
            tmp_config = Model(yaml.safe_load(group_yml.format(**replace_vars)))
        except KeyError as e:
            raise ValueError('group.yml contains template key `{}` but no value was provided'.format(e.args[0]))
        return assembly_group_config(self.get_releases_config(), self.assembly, tmp_config)

    def initialize(self, mode='none', no_group=False, disabled=None):

        if self.initialized:
            return

        if self.quiet and self.verbose:
            click.echo("Flags --quiet and --verbose are mutually exclusive")
            exit(1)

        # We could mark these as required and the click library would do this for us,
        # but this seems to prevent getting help from the various commands (unless you
        # specify the required parameters). This can probably be solved more cleanly, but TODO
        if not no_group and self.group is None:
            click.echo("Group must be specified")
            exit(1)

        if self.working_dir is None:
            self.working_dir = tempfile.mkdtemp(".tmp", "elliott-")
            # This can be set to False by operations which want the working directory to be left around
            self.remove_tmp_working_dir = True
            atexit.register(remove_tmp_working_dir, self)
        else:
            self.working_dir = os.path.abspath(self.working_dir)
            if not os.path.isdir(self.working_dir):
                os.makedirs(self.working_dir)

        self.debug_log_path = os.path.join(self.working_dir, 'debug.log')

        if not self._logger:
            self.initialize_logging()

        if disabled is not None:
            self.load_disabled = disabled

        if no_group:
            return  # nothing past here should be run without a group

        self.resolve_metadata()

        self.group_dir = self.gitdata.data_dir
        self.group_config = self.get_group_config()
        if self.group_config.name != self.group:
            raise IOError(
                "Name in group.yml does not match group name. Someone may have copied this group without updating group.yml (make sure to check branch)")

        if self.group_config.assemblies.enabled or self.enable_assemblies:
            if re.fullmatch(r'[\w.]+', self.assembly) is None or self.assembly[0] == '.' or self.assembly[-1] == '.':
                raise ValueError('Assembly names may only consist of alphanumerics, ., and _, but not start or end with a dot (.).')
        else:
            # If assemblies are not enabled for the group,
            # ignore this argument throughout doozer.
            self.assembly = None

        if self.branch is not None:
            self._logger.info("Using branch from command line: %s" % self.branch)
        elif self.group_config.branch is not Missing:
            self.branch = self.group_config.branch
            self._logger.info("Using branch from group.yml: %s" % self.branch)
        else:
            self._logger.info("No branch specified either in group.yml or on the command line; all included images will need to specify their own.")

        # Flattens a list like like [ 'x', 'y,z' ] into [ 'x.yml', 'y.yml', 'z.yml' ]
        # for later checking we need to remove from the lists, but they are tuples. Clone to list
        def flatten_list(names):
            if not names:
                return []
            # split csv values
            result = []
            for n in names:
                result.append([x for x in n.replace(' ', ',').split(',') if x != ''])
            # flatten result and remove dupes
            return list(set([y for x in result for y in x]))

        def filter_wip(n, d):
            return d.get('mode', 'enabled') in ['wip', 'enabled']

        def filter_enabled(n, d):
            return d.get('mode', 'enabled') == 'enabled'

        def filter_disabled(n, d):
            return d.get('mode', 'enabled') in ['enabled', 'disabled']

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

        replace_vars = self.group_config.vars.primitive() if self.group_config.vars else {}
        if self.assembly:
            replace_vars['runtime_assembly'] = self.assembly
        # release_name variable is currently only used in microshift rpm config to allow Doozer to pass release name to a modification script.
        # Elliott doesn't need to care about it. Set an arbitrary value until it becomes necessary.
        replace_vars['release_name'] = '(irrelevant)'

        image_data = {}
        if mode in ['images', 'both']:
            image_data = self.gitdata.load_data(path='images', keys=image_keys,
                                                exclude=image_ex,
                                                filter_funcs=None if len(image_keys) else filter_func,
                                                replace_vars=replace_vars)
            for i in image_data.values():
                self.late_resolve_image(i.key, add=True, data_obj=i)
            if not self.image_map:
                self._logger.warning("No image metadata directories found for given options within: {}".format(self.group_dir))

        if mode in ['rpms', 'both']:
            rpm_data = self.gitdata.load_data(path='rpms', keys=rpm_keys,
                                              exclude=rpm_ex,
                                              replace_vars=replace_vars,
                                              filter_funcs=None if len(rpm_keys) else filter_func)
            for r in rpm_data.values():
                metadata = RPMMetadata(self, r)
                self.rpm_map[metadata.distgit_key] = metadata
            if not self.rpm_map:
                self._logger.warning("No rpm metadata directories found for given options within: {}".format(self.group_dir))

        missed_include = set(image_keys) - set(image_data.keys())
        if len(missed_include) > 0:
            raise ElliottFatalError('The following images or rpms were either missing or filtered out: {}'.format(', '.join(missed_include)))

        strict_mode = True
        if not self.assembly or self.assembly in ['stream', 'test']:
            strict_mode = False
        self.assembly_type = assembly_type(self.get_releases_config(), self.assembly)
        self.assembly_basis_event = assembly_basis_event(self.get_releases_config(), self.assembly, strict=strict_mode)
        if self.assembly_basis_event:
            if self.brew_event:
                raise ElliottFatalError(f'Cannot run with assembly basis event {self.assembly_basis_event} and --brew-event at the same time.')
            # If the assembly has a basis event, we constrain all brew calls to that event.
            self.brew_event = self.assembly_basis_event
            self._logger.info(f'Constraining brew event to assembly basis for {self.assembly}: {self.brew_event}')

        self.initialized = True

    def initialize_logging(self):
        if self.initialized or self._logger:
            return

        # Three flags control the output modes of the command:
        # --verbose prints logs to CLI as well as to files
        # --debug increases the log level to produce more detailed internal
        #         behavior logging
        # --quiet opposes both verbose and debug
        if self.debug:
            log_level = logging.DEBUG
        elif self.quiet:
            log_level = logging.WARN
        else:
            log_level = logging.INFO

        logutil.setup_logging(log_level, self.debug_log_path)
        self._logger = logging.getLogger('elliottlib')

    def image_metas(self):
        return list(self.image_map.values())

    def rpm_metas(self):
        return list(self.rpm_map.values())

    def get_bug_tracker(self, bug_tracker_type) -> BugTracker:
        if bug_tracker_type in self._bug_trackers:
            return self._bug_trackers[bug_tracker_type]
        if bug_tracker_type == 'bugzilla':
            bug_tracker_cls = BugzillaBugTracker
        elif bug_tracker_type == 'jira':
            bug_tracker_cls = JIRABugTracker
        self._bug_trackers[bug_tracker_type] = bug_tracker_cls(bug_tracker_cls.get_config(self))
        return self._bug_trackers[bug_tracker_type]

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

    def late_resolve_image(self, distgit_name, add=False, data_obj=None):
        """Resolve image and retrieve meta, optionally adding to image_map.
        If image not found, error will be thrown"""

        if distgit_name in self.image_map:
            return self.image_map[distgit_name]
        if not data_obj:
            replace_vars = self.group_config.vars.primitive() if self.group_config.vars else {}
            if self.assembly:
                replace_vars['runtime_assembly'] = self.assembly
            data_obj = self.gitdata.load_data(path='images', key=distgit_name, replace_vars=replace_vars)
            if not data_obj:
                raise ElliottFatalError('Unable to resovle image metadata for {}'.format(distgit_name))

        meta = ImageMetadata(self, data_obj)
        if add:
            self.image_map[distgit_name] = meta
        return meta

    def resolve_metadata(self):
        """
        The group control data can be on a local filesystem, in a git
        repository that can be checked out, or some day in a database

        If the scheme is empty, assume file:///...
        Allow http, https, ssh and ssh+git (all valid git clone URLs)
        """

        if self.data_path is None:
            raise ElliottFatalError(
                ("No metadata path provided. Must be set via one of:\n"
                 "* data_path key in {}\n"
                 "* elliott --data-path [PATH|URL]\n"
                 "* Environment variable ELLIOTT_DATA_PATH\n"
                 ).format(self.cfg_obj.full_path))

        try:
            self.gitdata = gitdata.GitData(data_path=self.data_path, clone_dir=self.working_dir,
                                           branch=self.group, logger=self._logger)
            self.data_dir = self.gitdata.data_dir

        except gitdata.GitDataException as ex:
            raise ElliottFatalError(ex)

    def get_public_upstream(self, remote_git: str) -> Tuple[str, Optional[str]]:
        """
        Some upstream repo are private in order to allow CVE workflows. While we
        may want to build from a private upstream, we don't necessarily want to confuse
        end-users by referencing it in our public facing image labels / etc.
        In group.yaml, you can specify a mapping in "public_upstreams". It
        represents private_url_prefix => public_url_prefix. Remote URLs passed to this
        method which contain one of the private url prefixes will be translated
        into a new string with the public prefix in its place. If there is not
        applicable mapping, the incoming url will still be normalized into https.
        :param remote_git: The URL to analyze for private repo patterns.
        :return: tuple (url, branch)
            - url: An https normalized remote address with private repo information replaced.
            - branch: Optional public branch name if the public upstream source use a different branch name from the private upstream.
        """
        remote_https = util.convert_remote_git_to_https(remote_git)

        if self.group_config.public_upstreams:

            # We prefer the longest match in the mapping, so iterate through the entire
            # map and keep track of the longest matching private remote.
            target_priv_prefix = None
            target_pub_prefix = None
            target_pub_branch = None
            for upstream in self.group_config.public_upstreams:
                priv = upstream["private"]
                pub = upstream["public"]
                # priv can be a full repo, or an organization (e.g. git@github.com:openshift)
                # It will be treated as a prefix to be replaced
                https_priv_prefix = util.convert_remote_git_to_https(priv)  # Normalize whatever is specified in group.yaml
                https_pub_prefix = util.convert_remote_git_to_https(pub)
                if remote_https.startswith(f'{https_priv_prefix}/') or remote_https == https_priv_prefix:
                    # If we have not set the prefix yet, or if it is longer than the current contender
                    if not target_priv_prefix or len(https_priv_prefix) > len(target_pub_prefix):
                        target_priv_prefix = https_priv_prefix
                        target_pub_prefix = https_pub_prefix
                        target_pub_branch = upstream.get("public_branch")

            if target_priv_prefix:
                return (f'{target_pub_prefix}{remote_https[len(target_priv_prefix):]}', target_pub_branch)

        return (remote_https, None)

    def get_releases_config(self):
        if self.releases_config is not None:
            return self.releases_config

        load = self.gitdata.load_data(key='releases')
        if load:
            self.releases_config = Model(load.data)
        else:
            self.releases_config = Model()

        return self.releases_config

    def get_errata_config(self, **kwargs):
        return self.gitdata.load_data(key='erratatool', **kwargs).data

    def is_version_in_lifecycle_phase(self, phase: str, version: str = None) -> bool:
        """
        Determine if the version is in the specified lifecycle phase.
        :param phase: The lifecycle phase to check.
        :param version: The version to check. If ommitted the runtime's group version is used. e.g. "4.16"
        :return: True if the version is in the specified phase, False otherwise.
        """
        major, minor = self.get_major_minor()
        if version and version != f"{major}.{minor}":
            out = self.get_file_from_branch(f"openshift-{version}", 'group.yml')
            next_group_config = yaml.safe_load(out)
            actual_phase = next_group_config.get('software_lifecycle', {}).get('phase', None)
        else:
            actual_phase = self.group_config.software_lifecycle.phase
        return actual_phase == phase

    def get_file_from_branch(self, branch, filename):
        if branch == self.branch:
            raise ValueError("Do not use this method to access files from the current branch")

        # fetch branch first
        cmd = f"git -C {self.data_dir} fetch origin {branch}:{branch}"
        exectools.cmd_assert(cmd)

        cmd = f"git -C {self.data_dir} show {branch}:{filename}"
        rc, out, err = exectools.cmd_gather(cmd)
        if rc != 0:
            raise IOError(f"Failed to get {filename} from {branch} branch")
        return out

    def build_retrying_koji_client(self, caching: bool = False):
        """
        :return: Returns a new koji client instance that will automatically retry
        methods when it receives common exceptions (e.g. Connection Reset)
        Honors doozer --brew-event.
        """
        session = brew.KojiWrapper([self.group_config.urls.brewhub or constants.BREW_HUB], brew_event=self.brew_event)
        session.force_instance_caching = caching
        return session

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
                self._koji_client_session.gssapi_login()
            yield self._koji_client_session

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
