import datetime
import fnmatch
import io
import pathlib
import re
import sys
import urllib.parse
from enum import Enum
from typing import Dict, List, NamedTuple, Optional, Tuple, cast

import dateutil.parser
import requests
from artcommonlib import exectools
from artcommonlib.assembly import assembly_metadata_config
from artcommonlib.brew import BuildStates
from artcommonlib.metadata import MetadataBase
from artcommonlib.model import Model
from artcommonlib.pushd import Dir
from defusedxml import ElementTree
from dockerfile_parse import DockerfileParser
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

import doozerlib
from doozerlib.distgit import DistGitRepo, ImageDistGitRepo, RPMDistGitRepo
from doozerlib.source_resolver import SourceResolver
from doozerlib.util import isolate_git_commit_in_release


class CgitAtomFeedEntry(NamedTuple):
    title: str
    updated: datetime.datetime
    id: str
    content: str


#
# These are used as labels to index selection of a subclass.
#
DISTGIT_TYPES = {
    'image': ImageDistGitRepo,
    'rpm': RPMDistGitRepo,
}


class RebuildHintCode(Enum):
    NO_COMPONENT = (True, 0)
    NO_LATEST_BUILD = (True, 1)
    DISTGIT_ONLY_COMMIT_OLDER = (False, 2)
    DISTGIT_ONLY_COMMIT_NEWER = (True, 3)
    DELAYING_NEXT_ATTEMPT = (False, 4)
    LAST_BUILD_FAILED = (True, 5)
    NEW_UPSTREAM_COMMIT = (True, 6)
    UPSTREAM_COMMIT_MISMATCH = (True, 7)
    BUILD_IS_UP_TO_DATE = (False, 8)
    ANCESTOR_CHANGING = (True, 9)
    CONFIG_CHANGE = (True, 10)
    BUILDER_CHANGING = (True, 11)
    BUILD_ROOT_CHANGING = (True, 12)
    PACKAGE_CHANGE = (True, 13)
    ARCHES_CHANGE = (True, 14)
    DEPENDENCY_NEWER = (True, 15)
    TASK_BUNDLE_OUTDATED = (True, 16)
    NETWORK_MODE_CHANGE = (True, 17)


class RebuildHint(NamedTuple):
    code: RebuildHintCode
    reason: str

    @property
    def rebuild(self):
        return self.code.value[0]


class Metadata(MetadataBase):
    def __init__(
        self,
        meta_type: str,
        runtime: "doozerlib.Runtime",
        data_obj: Dict,
        commitish: Optional[str] = None,
        prevent_cloning: Optional[bool] = False,
    ):
        """
        :param meta_type - a string. Index to the sub-class <'rpm'|'image'>.
        :param runtime - a Runtime object.
        :param data_obj - a dictionary for the metadata configuration
        :param commitish: If not None, build from the specified upstream commit-ish instead of the branch tip.
        :param prevent_cloning: Throw an exception if upstream/downstream cloning operations are attempted.
        """
        super().__init__(meta_type, runtime, data_obj)

        self.commitish = commitish

        # For efficiency, we want to prevent some verbs from introducing changes that
        # trigger distgit or upstream cloning. Setting this flag to True will cause
        # an exception if it is attempted.
        self.prevent_cloning = prevent_cloning

        # URL and branch of public upstream source are set later by Runtime.resolve_source()
        self.public_upstream_url = None
        self.public_upstream_branch = None

        self.private_fix: Optional[bool] = None
        """ True if the source contains embargoed (private) CVE fixes. Defaulting to None means this should be auto-determined. """

        # List of Brew targets.
        # The first target is the primary target, against which tito will direct build.
        # Others are secondary targets. We will use Brew API to build against secondary
        # targets with the same distgit commit as the primary target.
        self.targets: List[str] = self.determine_targets()

        if self.runtime.assembly_basis_event and self.config.content.source.git.branch.target and not commitish:
            # Ok, so we are a release assembly like 'art1999'. We inherit from
            # 4.7.22 which was composed primarily out of "assembly.stream" builds
            # but maybe one or two pinned "assembly.4.7.22" builds.
            # An artists has been asked to create art1999 and bump a single RPM in the
            # ose-etcd image. To do that, the artist
            # adds the dependency to releases.yml for the ose-etcd distgit_key in the
            # art1999 assembly and then triggers a "rebuild" job of the image.
            # What upstream git commit do you expect to be built? Why the source from 4.7.22,
            # of course! The customer asked for an RPM bump, not to take on any of the hundreds of
            # code changes that may have take place since 4.7.22 in the 4.7 branch.
            # So how do we arrive at that? Well, it is in the brew metadata of the latest build from
            # the 4.7.22 assembly.
            # Oh, but what if the customer DOES want a different commit? Well, the artist should
            # update the release.yml for art1999 to include that commit or explicitly specify a branch.
            # How do we determine whether they have done that? Looking for any explicit overrides
            # in our assembly's metadata.
            # Let's do it!
            assembly_overrides = assembly_metadata_config(
                runtime.get_releases_config(), runtime.assembly, meta_type, self.distgit_key, Model({})
            )
            # Nice! By passing Model({}) instead of the metadata from our image yml file, we should only get fields actually defined in
            # releases.yml.
            if assembly_overrides.content.source.git.branch.target:
                # Yep.. there is an override in releases.yml. The good news is that we are done.
                # The rest of doozer code is equipped to clone that upstream commit
                # and rebase using it.
                pass
            else:
                # Ooof.. it is not defined in the assembly, so we need to find it dynamically.
                build_obj = self.get_latest_build_sync(default=None, el_target=self.determine_rhel_targets()[0])
                if build_obj:
                    self.commitish = isolate_git_commit_in_release(build_obj['nvr'])
                    self.logger.info(
                        'Pinning upstream source to commit of assembly selected build '
                        f'({build_obj["build_id"]}) -> commit {self.commitish}'
                    )
                else:
                    # If this is part of a unit test, don't make the caller's life more difficult than it already is; skip the exception.
                    if 'unittest' not in sys.modules.keys():
                        raise IOError(
                            f'Expected to find pre-existing build for {self.distgit_key} in order to pin upstream source commit'
                        )

            # If you've read this far, you may be wondering, why are we not trying to find the SOURCE_GIT_URL from the last built image?
            # Good question! Because it should be the value found in our assembly-modified image metadata!
            # The git commit starts as a branch in standard ocp-build-data metadata and its
            # commit hash is only discovered at runtime. The source git URL is literal. If it does change somewhere in the assembly
            # definitions, that's fine. This assembly should find it when looking up the content.source.git.url from the metadata.

    def distgit_remote_url(self):
        pkgs_host = self.runtime.group_config.urls.get('pkgs_host', 'pkgs.devel.redhat.com')
        # rhpkg uses a remote named like this to pull content from distgit
        if self.runtime.user:
            return f'ssh://{self.runtime.user}@{pkgs_host}/{self.qualified_name}'
        return f'ssh://{pkgs_host}/{self.qualified_name}'

    def distgit_repo(self, autoclone=True) -> DistGitRepo:
        if self._distgit_repo is None:
            self._distgit_repo = DISTGIT_TYPES[self.meta_type](self, autoclone=autoclone)
        return self._distgit_repo

    def build_root_tag(self):
        return '{}-build'.format(self.branch())

    def candidate_brew_tag(self):
        return '{}-candidate'.format(self.branch())

    def hotfix_brew_tag(self):
        return f'{self.branch()}-hotfix'

    def candidate_brew_tags(self):
        return [self.candidate_brew_tag()]

    def hotfix_brew_tags(self):
        """Returns "hotfix" Brew tags for this component.
        "Hotfix" tags are used to prevent garbage collection.
        """
        return [self.hotfix_brew_tag()]

    def get_arches(self):
        """
        :return: Returns the list of architecture this image/rpm should build for. This is an intersection
        of config specific arches & globally enabled arches in group.yml
        """
        global_arches = (
            self.runtime.get_global_konflux_arches()
            if self.runtime.build_system == 'konflux'
            else self.runtime.get_global_arches()
        )

        if self.config.arches:
            ca = self.config.arches
            intersection = list(set(global_arches) & set(ca))
            if len(intersection) != len(ca):
                self.logger.info(
                    f'Arches are being pruned by group.yml. Using computed {intersection} vs config list {ca}'
                )
            if not intersection:
                raise ValueError(f'No arches remained enabled in {self.qualified_key}')
            return intersection
        else:
            return list(global_arches)

    def cgit_atom_feed(
        self, commit_hash: Optional[str] = None, branch: Optional[str] = None
    ) -> List[CgitAtomFeedEntry]:
        """
        :param commit_hash: Specify to receive an entry for the specific commit (branch ignored if specified).
                            Returns a feed with a single entry.
        :param branch: branch name; None implies the branch specified in ocp-build-data (XOR commit_hash).
                            Returns a feed with several of the most recent entries.

        Returns a representation of the cgit atom feed. This information includes
        feed example: https://gist.github.com/jupierce/ab006c0fc83050b714f6de2ec30f1072 . This
        feed provides timestamp and commit information without having to clone distgits.

        Example urls..
        http://pkgs.devel.redhat.com/cgit/containers/cluster-etcd-operator/atom/?h=rhaos-4.8-rhel-8
        or
        http://pkgs.devel.redhat.com/cgit/containers/cluster-etcd-operator/atom/?id=35ecfa4436139442edc19585c1c81ebfaca18550
        """
        cgit_url_base = self.runtime.group_config.urls.cgit
        if not cgit_url_base:
            raise ValueError("urls.cgit is not set in group config")
        url = f"{cgit_url_base}/{urllib.parse.quote(self.qualified_name)}/atom/"
        params = {}
        if commit_hash:
            params["id"] = commit_hash
        else:
            if branch is None:
                branch = self.branch()
            if not commit_hash and branch:
                params["h"] = branch

        def _make_request():
            self.logger.info("Getting cgit atom feed %s ...", url)
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            return resp.text

        content = retry(
            reraise=True,  # reraise the original exception in case of error
            stop=stop_after_attempt(3),  # wait for 10 seconds * 3 = 30 seconds
            wait=wait_fixed(10),  # wait for 10 seconds between retries
            retry=retry_if_exception_type(),
        )(_make_request)()

        et = ElementTree.fromstring(content)

        entry_list = list()
        for et_entry in et.findall('{http://www.w3.org/2005/Atom}entry'):
            entry = CgitAtomFeedEntry(
                title=et_entry.find('{http://www.w3.org/2005/Atom}title').text,
                updated=dateutil.parser.parse(et_entry.find('{http://www.w3.org/2005/Atom}updated').text),
                id=et_entry.find('{http://www.w3.org/2005/Atom}id').text,
                content=et_entry.find('{http://www.w3.org/2005/Atom}content[@type="text"]').text,
            )
            entry_list.append(entry)

        return entry_list

    def cgit_file_url(self, filename: str, commit_hash: Optional[str] = None, branch: Optional[str] = None) -> str:
        """Construct a cgit URL to a given file associated with the commit hash pushed to distgit
        :param filename: a relative path
        :param commit_hash: commit hash; None implies the current HEAD
        :param branch: branch name; None implies the branch specified in ocp-build-data
        :return: a cgit URL
        """
        cgit_url_base = self.runtime.group_config.urls.cgit
        if not cgit_url_base:
            raise ValueError("urls.cgit is not set in group config")
        ret = f"{cgit_url_base}/{urllib.parse.quote(self.qualified_name)}/plain/{urllib.parse.quote(filename)}"
        params = {}
        if branch is None:
            branch = self.branch()
        if branch:
            params["h"] = branch
        if commit_hash:
            params["id"] = commit_hash
        if params:
            ret += "?" + urllib.parse.urlencode(params)
        return ret

    def fetch_cgit_file(self, filename, commit_hash: Optional[str] = None, branch: Optional[str] = None):
        """Retrieve the content of a cgit URL to a given file associated with the commit hash pushed to distgit
        :param filename: a relative path
        :param commit_hash: commit hash; None implies the current HEAD
        :param branch: branch name; None implies the branch specified in ocp-build-data
        :return: the content of the file
        """
        url = self.cgit_file_url(filename, commit_hash=commit_hash, branch=branch)
        try:
            req = exectools.retry(3, lambda: urllib.request.urlopen(url), check_f=lambda req: req.code == 200)
        except Exception as e:
            raise IOError(f"Failed to fetch {url}: {e}. Does branch {branch} exist?")
        return req.read()

    def get_latest_build_info(self, default=-1, **kwargs):
        """
        Queries brew to determine the most recently built release of the component
        associated with this image. This method does not rely on the "release"
        label needing to be present in the Dockerfile. kwargs will be passed on
        to get_latest_build.
        :param default: A value to return if no latest is found (if not specified, an exception will be thrown)
        :return: A tuple: (component name, version, release); e.g. ("registry-console-docker", "v3.6.173.0.75", "1")
        """
        build = self.get_latest_brew_build(default=default, **kwargs)
        if default != -1 and build == default:
            return default
        return build['name'], build['version'], build['release']

    def has_source(self):
        """
        Check whether this component has source content
        """
        return "git" in self.config.content.source or "alias" in self.config.content.source

    def needs_rebuild(self):
        if self.config.targets:
            # If this meta has multiple build targets, check currency of each
            for target in self.config.targets:
                hint = self._target_needs_rebuild(el_target=target)
                if hint.rebuild or hint.code == RebuildHintCode.DELAYING_NEXT_ATTEMPT:
                    # No need to look for more
                    return hint
            return hint
        else:
            return self._target_needs_rebuild(el_target=None)

    def _target_needs_rebuild(self, el_target=None) -> RebuildHint:
        """
        Checks whether the current upstream commit has a corresponding successful downstream build.
        Take care to not unnecessarily trigger a clone of the distgit
        or upstream source as it will dramatically increase the time needed for scan-sources.
        :param el_target: A brew build target or literal '7', '8', or rhel to perform the search for.
        :return: Returns (rebuild:<bool>, message: description of why).
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        # If a build fails, how long will we wait before trying again
        rebuild_interval = self.runtime.group_config.scan_freshness.threshold_hours or 6

        component_name = self.get_component_name()

        latest_build = self.get_latest_brew_build(default=None, el_target=el_target)

        if not latest_build:
            return RebuildHint(
                code=RebuildHintCode.NO_LATEST_BUILD,
                reason=f'Component {component_name} has no latest build '
                f'for assembly {self.runtime.assembly} '
                f'and target {el_target}',
            )

        latest_build_creation = dateutil.parser.parse(latest_build['creation_time'])
        latest_build_creation = latest_build_creation.replace(
            tzinfo=datetime.timezone.utc
        )  # If time lacks timezone info, interpret as UTC

        # Log scan-sources coordinates throughout to simplify setting up scan-sources
        # function tests to reproduce real-life scenarios.
        self.logger.debug(f'scan-sources coordinate: latest_build: {latest_build}')
        self.logger.debug(f'scan-sources coordinate: latest_build_creation_datetime: {latest_build_creation}')

        dgr = self.distgit_repo(autoclone=False)  # For scan-sources speed, we need to avoid cloning
        if not dgr.has_source():
            # This is a distgit only artifact (no upstream source)

            # If downstream has been locked to a commitish, only check the atom feed at that moment.
            distgit_commitish = self.runtime.downstream_commitish_overrides.get(self.distgit_key, None)
            atom_entries = self.cgit_atom_feed(commit_hash=distgit_commitish, branch=self.branch())
            if not atom_entries:
                raise IOError(
                    f'No atom feed entries exist for distgit-only repo {dgr.name} ({component_name}) in {self.branch()}. Does branch exist?'
                )

            latest_entry = atom_entries[0]  # Most recent commit's information
            dg_commit = latest_entry.id
            self.logger.debug(f'scan-sources coordinate: dg_commit: {dg_commit}')
            dg_commit_dt = latest_entry.updated
            self.logger.debug(f'scan-sources coordinate: distgit_head_commit_datetime: {dg_commit_dt}')

            if latest_build_creation > dg_commit_dt:
                return RebuildHint(
                    code=RebuildHintCode.DISTGIT_ONLY_COMMIT_OLDER,
                    reason='Distgit only repo commit is older than most recent build',
                )

            # Two possible states here:
            # 1. A user has made a commit to this dist-git only branch and there has been no build attempt
            # 2. We've already tried a build and the build failed.

            # Check whether a build attempt for this assembly has failed.
            last_failed_build = self.get_latest_brew_build(
                default=None, build_state=BuildStates.FAILED, el_target=el_target
            )  # How recent was the last failed build?
            if not last_failed_build:
                return RebuildHint(
                    code=RebuildHintCode.DISTGIT_ONLY_COMMIT_NEWER,
                    reason='Distgit only commit is newer than last successful build',
                )

            last_failed_build_creation = dateutil.parser.parse(last_failed_build['creation_time'])
            last_failed_build_creation = last_failed_build_creation.replace(
                tzinfo=datetime.timezone.utc
            )  # If time lacks timezone info, interpret as UTC
            if last_failed_build_creation + datetime.timedelta(hours=rebuild_interval) > now:
                return RebuildHint(
                    code=RebuildHintCode.DELAYING_NEXT_ATTEMPT,
                    reason=f'Waiting at least {rebuild_interval} hours after last failed build',
                )

            return RebuildHint(
                code=RebuildHintCode.LAST_BUILD_FAILED,
                reason=f'Last build failed > {rebuild_interval} hours ago; making another attempt',
            )

        # Otherwise, we have source. In the case of git source, check the upstream with ls-remote.
        # In the case of alias (only legacy stuff afaik), check the cloned repo directory.

        use_source_fallback_branch = cast(str, self.runtime.group_config.use_source_fallback_branch or "yes")
        if "git" in self.config.content.source:
            _, upstream_commit_hash = SourceResolver.detect_remote_source_branch(
                self.config.content.source.git, self.runtime.stage, use_source_fallback_branch
            )
        elif (
            self.config.content.source.alias
            and self.runtime.group_config.sources
            and self.config.content.source.alias in self.runtime.group_config.sources
        ):
            # This is a new style alias with url information in group config
            source_details = self.runtime.group_config.sources[self.config.content.source.alias]
            _, upstream_commit_hash = SourceResolver.detect_remote_source_branch(
                source_details, self.runtime.stage, use_source_fallback_branch
            )
        else:
            # If it is not git, we will need to punt to the rest of doozer to get the upstream source for us.
            with Dir(dgr.source_path()):
                upstream_commit_hash, _ = exectools.cmd_assert('git rev-parse HEAD', strip=True)

        self.logger.debug(f'scan-sources coordinate: upstream_commit_hash: {upstream_commit_hash}')
        git_component = f'.g*{upstream_commit_hash[:7]}'  # use .g*<commit> so it matches new form ".g0123456" and old ".git.0123456"

        # Scan for any build in this assembly which also includes the git commit.
        upstream_commit_build = self.get_latest_brew_build(
            default=None, extra_pattern=f'*{git_component}*', el_target=el_target
        )  # Latest build for this commit.

        if not upstream_commit_build:
            # There is no build for this upstream commit. Two options to assess:
            # 1. This is a new commit and needs to be built
            # 2. Previous attempts at building this commit have failed

            # Check whether a build attempt with this commit has failed before.
            failed_commit_build = self.get_latest_brew_build(
                default=None, extra_pattern=f'*{git_component}*', build_state=BuildStates.FAILED, el_target=el_target
            )  # Have we tried before and failed?

            # If not, this is a net-new upstream commit. Build it.
            if not failed_commit_build:
                return RebuildHint(
                    code=RebuildHintCode.NEW_UPSTREAM_COMMIT,
                    reason='A new upstream commit exists and needs to be built',
                )

            # Otherwise, there was a failed attempt at this upstream commit on record.
            # Make sure provide at least rebuild_interval hours between such attempts
            last_attempt_time = dateutil.parser.parse(failed_commit_build['creation_time'])
            last_attempt_time = last_attempt_time.replace(
                tzinfo=datetime.timezone.utc
            )  # If time lacks timezone info, interpret as UTC

            if last_attempt_time + datetime.timedelta(hours=rebuild_interval) < now:
                return RebuildHint(
                    code=RebuildHintCode.LAST_BUILD_FAILED,
                    reason=f'It has been {rebuild_interval} hours since last failed build attempt',
                )

            return RebuildHint(
                code=RebuildHintCode.DELAYING_NEXT_ATTEMPT,
                reason=f'Last build of upstream commit {upstream_commit_hash} failed, but holding off for at least {rebuild_interval} hours before next attempt',
            )

        if latest_build['nvr'] != upstream_commit_build['nvr']:
            return RebuildHint(
                code=RebuildHintCode.UPSTREAM_COMMIT_MISMATCH,
                reason=f'Latest build {latest_build["nvr"]} does not match upstream commit build {upstream_commit_build["nvr"]}; commit reverted?',
            )

        return RebuildHint(
            code=RebuildHintCode.BUILD_IS_UP_TO_DATE,
            reason=f'Build already exists for current upstream commit {upstream_commit_hash}: {latest_build}',
        )

    def get_jira_info(self) -> Tuple[str, str]:
        """
        :return: Returns Jira project name and component. These
            are coordinates for where to file bugs.
        """

        # We are trying to discover some team information that indicates which Jira project bugs for this
        # component should be filed against. This information can be stored in the doozer metadata OR
        # in prodsec's component mapping. Metadata overrides, as usual.

        # Maintainer info can be defined in metadata, so try there first.
        maintainer = self.config.jira.copy() or dict()

        product_config = self.runtime.get_product_config()
        issue_project = product_config.bug_mapping.default_issue_project

        component_mapping = product_config.bug_mapping.components
        component_entry = component_mapping[self.get_component_name()]
        if component_entry.issue_project:
            issue_project = component_entry.issue_project

        jira_component = component_entry.issue_component

        if not issue_project:
            issue_project = 'OCPBUGS'

        if not jira_component:
            jira_component = 'Unknown'

        if self.distgit_key == 'openshift-enterprise-base':
            # This is a special case image that is represented by upstream but
            # no one release owns. ART should handle merges here.
            jira_component = 'Release'

        return maintainer.get('project', issue_project), maintainer.get('component', jira_component)

    def extract_kube_env_vars(self) -> Dict[str, str]:
        """
        Analyzes the source_base_dir for the hyperkube Dockerfile in which the release's k8s version
        is defined. Side effect is cloning distgit
        and upstream source if it has not already been done.
        :return: A Dict of environment variables that should be added to the Dockerfile / rpm spec.
                Variables like KUBE_GIT_VERSION, KUBE_GIT_COMMIT, KUBE_GIT_MINOR, ...
                May be empty if there is no kube information in the source dir.
        """
        envs = dict()
        if not self.has_source():  # distgit only. Return empty.
            return envs
        upstream_source_path: pathlib.Path = pathlib.Path(self.runtime.source_resolver.resolve_source(self).source_path)

        with Dir(upstream_source_path):
            out, _ = exectools.cmd_assert(["git", "rev-parse", "HEAD"])
            source_full_sha = out

        use_path = None
        path_4x = upstream_source_path.joinpath(
            'openshift-hack/images/hyperkube/Dockerfile.rhel'
        )  # for >= 4.6: https://github.com/openshift/kubernetes/blob/fcff70a54d3f0bde19e879062e8f1489ba5d0cb0/openshift-hack/images/hyperkube/Dockerfile.rhel#L16
        if path_4x.exists():
            use_path = path_4x

        path_3_11 = upstream_source_path.joinpath(
            'images/hyperkube/Dockerfile'
        )  # for 3.11: https://github.com/openshift/ose/blob/enterprise-3.11/images/hyperkube/Dockerfile
        if not use_path and path_3_11.exists():
            use_path = path_3_11

        kube_version_fields = []
        kube_commit_hash = ''
        if use_path:
            dfp = DockerfileParser(cache_content=True, fileobj=io.BytesIO(use_path.read_bytes()))
            build_versions = dfp.labels.get('io.openshift.build.versions', None)
            if not build_versions:
                raise IOError(f'Unable to find io.openshift.build.versions label in {str(use_path)}')

            # Find something like kubernetes=1.22.1 and extract version as group
            m = re.match(r"^.*[^\w]*kubernetes=([\d.]+).*", build_versions)
            if not m:
                raise IOError(
                    f'Unable to find `kubernetes=...` in io.openshift.build.versions label from {str(use_path)}'
                )

            base_kube_version = m.group(1).lstrip('v')
            kube_version_fields = base_kube_version.split('.')  # 1.17.1 => [ '1', '17', '1']

            # upstream kubernetes creates a tag for each version. Go find its sha.
            rc, out, err = exectools.cmd_gather(
                f'git ls-remote https://github.com/kubernetes/kubernetes v{base_kube_version}'
            )
            out = out.strip()
            if rc == 0 and out:
                # Expecting something like 'a26dc584ac121d68a8684741bce0bcba4e2f2957	refs/tags/v1.19.0-rc.2'
                kube_commit_hash = out.split()[0]
            else:
                # That's strange, but let's not kill the build for it.  Poke in our repo's hash.
                self.logger.warning(
                    f'Unable to find upstream git tag v{base_kube_version} in https://github.com/kubernetes/kubernetes'
                )
                kube_commit_hash = source_full_sha

        if kube_version_fields:
            # For historical consistency with tito's flow, we add +OS_GIT_COMMIT[:7] to the kube version
            envs['KUBE_GIT_VERSION'] = f"v{'.'.join(kube_version_fields)}+{source_full_sha[:7]}"
            envs['KUBE_GIT_MAJOR'] = '0' if len(kube_version_fields) < 1 else kube_version_fields[0]
            godep_kube_minor = '0' if len(kube_version_fields) < 2 else kube_version_fields[1]
            envs['KUBE_GIT_MINOR'] = (
                f'{godep_kube_minor}+'  # For historical reasons, append a '+' since OCP patches its vendored kube.
            )
            envs['KUBE_GIT_COMMIT'] = kube_commit_hash
            envs['KUBE_GIT_TREE_STATE'] = 'clean'
        elif self.name in ('openshift-enterprise-hyperkube', 'openshift', 'atomic-openshift'):
            self.logger.critical(
                f'Unable to acquire KUBE vars for {self.name}. This must be fixed or platform addons can break: https://bugzilla.redhat.com/show_bug.cgi?id=1861097'
            )
            raise IOError(f'Unable to determine KUBE vars for {self.name}')

        return envs

    def is_rpm_exempt(self, rpm_name) -> Tuple[bool, Optional[str]]:
        """Check if the given rpm is exempt from scan_sources
        Pattern matching is done using glob pattern and fnmatch module
        https://docs.python.org/3/library/fnmatch.html
        :param rpm_name: package name to check
        :return: Tuple of (is_exempt, pattern)
        """
        exempt_rpms = self.config.scan_sources.exempt_rpms or []
        if not exempt_rpms:
            return False, None

        for pattern in exempt_rpms:
            if fnmatch.fnmatch(rpm_name, pattern):
                return True, pattern
        return False, None

    def get_konflux_network_mode(self):
        group_config_network_mode = self.runtime.group_config.konflux.get("network_mode")
        image_config_network_mode = self.config.konflux.get("network_mode")

        # Image config supersedes group config, but set to "open" by default, if missing.
        network_mode = image_config_network_mode or group_config_network_mode or "open"

        valid_network_modes = ["hermetic", "internal-only", "open"]
        if network_mode not in valid_network_modes:
            raise ValueError(f"Invalid network mode; {network_mode}. Valid modes: {valid_network_modes}")
        return network_mode
