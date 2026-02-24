import logging
import os
import shutil
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, cast

from artcommonlib import assertion, constants, exectools
from artcommonlib import util as art_util
from artcommonlib.git_helper import git_clone
from artcommonlib.lock import get_named_semaphore
from artcommonlib.model import ListModel, Missing, Model

from doozerlib.record_logger import RecordLogger

if TYPE_CHECKING:
    from doozerlib.metadata import Metadata

LOGGER = logging.getLogger(__name__)


@dataclass
class SourceResolution:
    """A dataclass for caching the result of SourceResolver.resolve_source."""

    source_path: str
    """ The local path to the source code repository. """
    url: str
    """ The URL (usually HTTPS or SSH) of the source code repository (for pushing). """
    branch: str
    """ The branch name (or commit hash) of the source code repository. """
    https_url: str
    """ The HTTPS URL (instead of SSH) of the source code repository (for pushing). """
    commit_hash: str
    """ The commit hash of the current HEAD. """

    @property
    def commit_hash_short(self):
        """The short commit hash of the current HEAD."""
        return self.commit_hash[:7]

    committer_date: datetime
    """ The committer date of the current HEAD. """
    latest_tag: str
    """ The latest tag of the current HEAD. """
    has_public_upstream: bool
    """ True if the source code repository has a public upstream """
    public_upstream_url: str
    """ The public upstream URL of the source code repository. If the source code repository does not have a public upstream, this will be the same as the https_url. """
    public_upstream_branch: str
    """ The public upstream branch name of the source code repository. If the source code repository does not have a public upstream, this will be the same as the branch. """
    pull_url: Optional[str] = None
    """ The URL to pull from. If None, uses url for both pull and push. """

    @property
    def https_pull_url(self) -> str:
        """The HTTPS URL for pulling. If pull_url is None, returns https_url."""
        if self.pull_url:
            return art_util.convert_remote_git_to_https(self.pull_url)
        return self.https_url


class SourceResolver:
    """A class for resolving source code repositories."""

    def __init__(
        self,
        sources_base_dir: str,
        cache_dir: Optional[str],
        group_config: Model,
        local=False,
        upcycle=False,
        stage=False,
        record_logger: Optional[RecordLogger] = None,
        state_holder: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize a new SourceResolver instance.
        :param sources_base_dir: The base directory where source code repositories will be cloned.
        :param cache_dir: The base directory where source code repositories will be cached.
        :param group_config: The group configuration.
        :param local: If True, the source code repositories will be resolved locally.
        :param upcycle: If True, the source code repositories will be refreshed.
        :param stage: If True, the stage branch will be used instead of the target branch.
        :param record_logger: The record logger to use for logging source resolutions.
        :param state_holder: The state holder dict to use for storing source resolutions.
        """
        self.sources_base_dir = Path(sources_base_dir)
        self.local = local
        # Map of source code repo aliases (e.g. "ose") to named tuples representing the source resolution cache.
        # See registry_repo.
        self.source_resolutions: Dict[str, SourceResolution] = {}
        self.upcycle = upcycle
        self.stage = stage
        self.cache_dir = cache_dir
        self._group_config = group_config
        self._record_logger = record_logger
        self._state_holder = state_holder

    def resolve_source(self, meta: 'Metadata', no_clone: bool = False) -> SourceResolution:
        """Resolve the source code repository for the specified metadata.
        :param meta: The metadata to resolve the source code repository for.
        :param no_clone: If True, the source code repository will not be cloned. Only the source resolution object will be returned.
        :return: A SourceResolution instance containing the information of the resolved source code repository.
        """
        LOGGER.debug("Resolving source for {}".format(meta.qualified_key))
        source = cast(Optional[Model], meta.config.content.source)
        if not source:
            raise ValueError(f"No source defined in metadata config for {meta.qualified_key}")

        source_details = None
        # This allows passing `--source <distgit_key> path` to
        # override any source to something local without it
        # having been configured for an alias
        if self.local and meta.distgit_key in self.source_resolutions:
            alias = meta.distgit_key
        elif 'git' in source:
            git_url = urllib.parse.urlparse(art_util.convert_remote_git_to_https(source.git.url))
            name = os.path.splitext(os.path.basename(git_url.path))[0]
            alias = f'{meta.namespace}_{meta.name}_{name}'
            source_details = dict(source.git)
        elif 'alias' in source:
            alias = str(source.alias)
        else:
            raise ValueError(f"No source url or alias defined for {meta.qualified_key}")

        LOGGER.debug("Resolving local source directory for alias {}".format(alias))
        if alias in self.source_resolutions:
            r = self.source_resolutions[alias]
            path = r.source_path
            meta.public_upstream_url = r.public_upstream_url
            meta.public_upstream_branch = r.public_upstream_branch
            LOGGER.debug("returning previously resolved path for alias {}: {}".format(alias, path))
            return self.source_resolutions[alias]

        # Where the source will land, check early so we know if old or new style
        if not source_details:  # old style alias was given
            if self._group_config.sources is Missing or alias not in self._group_config.sources:
                raise IOError("Source alias not found in specified sources or in the current group: %s" % alias)
            source_details = dict(self._group_config.sources[alias])
            source_dir = os.path.join(self.sources_base_dir, f"global_{alias}")
        else:
            source_dir = os.path.join(self.sources_base_dir, alias)

        LOGGER.debug("checking for source directory in source_dir: {}".format(source_dir))

        with get_named_semaphore(source_dir, is_dir=True):
            if alias in self.source_resolutions:  # we checked before, but check again inside the lock
                r = self.source_resolutions[alias]
                path = r.source_path
                meta.public_upstream_url = r.public_upstream_url
                meta.public_upstream_branch = r.public_upstream_branch
                LOGGER.debug("returning previously resolved path for alias {}: {}".format(alias, path))
                return self.source_resolutions[alias]

            # If this source has already been extracted for this working directory
            if os.path.isdir(source_dir):
                if not self.upcycle:
                    # Store so that the next attempt to resolve the source hits the map
                    r = self.register_source_alias(alias, source_dir)
                    path = r.source_path
                    LOGGER.info("Source '{}' already exists in (skipping clone): {}".format(alias, source_dir))
                    meta.public_upstream_url = r.public_upstream_url
                    meta.public_upstream_branch = r.public_upstream_branch
                    return self.source_resolutions[alias]
                if self.local:
                    raise IOError("--upcycle mode doesn't work with --local.")
                LOGGER.info("Refreshing source for '{}' due to --upcycle: {}".format(alias, source_dir))
                shutil.rmtree(source_dir)

            if meta.prevent_cloning:
                raise IOError(
                    f'Attempt to clone upstream {meta.distgit_key} after cloning disabled; a regression has been introduced.'
                )

            use_source_fallback_branch = cast(str, self._group_config.use_source_fallback_branch or "yes")
            clone_branch, _ = self.detect_remote_source_branch(source_details, self.stage, use_source_fallback_branch)

            url = str(source_details["url"])
            pull_url = source_details.get("url_pull")  # Extract optional pull URL
            has_public_upstream = False
            if self._group_config.public_upstreams:
                meta.public_upstream_url, meta.public_upstream_branch, has_public_upstream = self.get_public_upstream(
                    url, self._group_config.public_upstreams
                )

            if no_clone:
                https_url = art_util.convert_remote_git_to_https(url)
                return SourceResolution(
                    source_path=source_dir,
                    url=url,
                    branch=clone_branch,
                    https_url=https_url,
                    commit_hash="",
                    committer_date=datetime.fromtimestamp(0, timezone.utc),
                    latest_tag="",
                    has_public_upstream=has_public_upstream,
                    public_upstream_url=meta.public_upstream_url or https_url,
                    public_upstream_branch=meta.public_upstream_branch or clone_branch,
                    pull_url=pull_url,
                )

            clone_url = pull_url or url  # Use pull URL for cloning if available
            LOGGER.info("Attempting to checkout source '%s' branch %s in: %s" % (clone_url, clone_branch, source_dir))
            try:
                # clone all branches as we must sometimes reference master /OWNERS for maintainer information
                if self.is_branch_commit_hash(branch=clone_branch):
                    gitargs = []
                else:
                    gitargs = ['--branch', clone_branch]

                git_clone(
                    clone_url,
                    source_dir,
                    gitargs=gitargs,
                    set_env=constants.GIT_NO_PROMPTS,
                    git_cache_dir=self.cache_dir,
                )

                # If we used pull_url for cloning, we need to set up the push remote
                if pull_url and pull_url != url:
                    with exectools.Dir(source_dir):
                        exectools.cmd_assert(f'git remote set-url origin {url}')  # Set origin to push URL
                        exectools.cmd_assert(f'git remote add pull {pull_url}')  # Add pull remote

                if self.is_branch_commit_hash(branch=clone_branch):
                    with exectools.Dir(source_dir):
                        exectools.cmd_assert(f'git checkout {clone_branch}')

                if meta.commitish:
                    # With the alias registered, check out the commit we want
                    LOGGER.info(f"Determining if commit-ish {meta.commitish} exists")
                    cmd = ["git", "-C", source_dir, "branch", "--contains", meta.commitish]
                    exectools.cmd_assert(cmd)
                    LOGGER.info(f"Checking out commit-ish {meta.commitish}")
                    exectools.cmd_assert(["git", "-C", source_dir, "reset", "--hard", meta.commitish])

                # fetch public upstream source
                if has_public_upstream:
                    self.setup_and_fetch_public_upstream_source(
                        meta.public_upstream_url, meta.public_upstream_branch or clone_branch, source_dir
                    )

            except IOError as e:
                LOGGER.info("Unable to checkout branch {}: {}".format(clone_branch, str(e)))
                shutil.rmtree(source_dir)
                raise IOError("Error checking out target branch of source '%s' in %s: %s" % (alias, source_dir, str(e)))

            # Store so that the next attempt to resolve the source hits the map
            self.register_source_alias(alias, source_dir)

            return self.source_resolutions[alias]

    @staticmethod
    def detect_remote_source_branch(
        source_details: Dict[str, Any], stage: bool, use_source_fallback_branch: str = "yes"
    ) -> Tuple[str, str]:
        """Find a configured source branch that exists, or raise DoozerFatalError.

        :param source_details: The source details from the metadata config.
        :param stage: If True, the stage branch will be used instead of the target branch.
        :param use_source_fallback_branch: The fallback strategy to use. Must be one of "yes", "always", or "never".
        :returns: Returns branch name and git hash
        """
        if use_source_fallback_branch not in ["yes", "always", "never"]:
            raise ValueError(f"Invalid value for use_source_fallback_branch: {use_source_fallback_branch}")

        git_url = source_details.get("url_pull", source_details["url"])  # Use pull URL if available
        branches = source_details["branch"]

        stage_branch = branches.get("stage", None) if stage else None
        if stage_branch:
            LOGGER.info('Normal branch overridden by --stage option, using "{}"'.format(stage_branch))
            result = SourceResolver._get_remote_branch_ref(git_url, stage_branch)
            if result:
                return stage_branch, result
            raise IOError(
                '--stage option specified and no stage branch named "{}" exists for {}'.format(stage_branch, git_url)
            )

        branch = branches["target"]  # This is a misnomer as it can also be a git commit hash an not just a branch name.
        fallback_branch = branches.get("fallback", None)
        if use_source_fallback_branch == "always" and fallback_branch:
            # only use the fallback (unless none is given)
            branch, fallback_branch = fallback_branch, None
        elif use_source_fallback_branch == "never":
            # ignore the fallback
            fallback_branch = None

        if SourceResolver.is_branch_commit_hash(branch):
            return branch, branch

        result = SourceResolver._get_remote_branch_ref(git_url, branch)
        if result:
            return branch, result

        if not fallback_branch:
            raise IOError('Requested target branch {} does not exist and no fallback provided'.format(branch))

        LOGGER.info('Target branch does not exist in {}, checking fallback branch {}'.format(git_url, fallback_branch))
        result = SourceResolver._get_remote_branch_ref(git_url, fallback_branch)
        if result:
            return fallback_branch, result
        raise IOError('Requested fallback branch {} does not exist'.format(branch))

    @staticmethod
    def is_branch_commit_hash(branch):
        """
        When building custom assemblies, it is sometimes useful to
        pin upstream sources to specific git commits. This cannot
        be done with standard assemblies which should be built from
        branches.
        :param branch: A branch name in rpm or image metadata.
        :returns: Returns True if the specified branch name is actually a commit hash for a custom assembly.
        """
        if len(branch) >= 7:  # The hash must be sufficiently unique
            try:
                int(branch, 16)  # A hash must be a valid hex number
                return True
            except ValueError:
                pass
        return False

    @staticmethod
    def _get_remote_branch_ref(git_url, branch):
        """
        Detect whether a single branch exists on a remote repo; returns git hash if found
        :param git_url: The URL to the git repo to check.
        :param branch: The name of the branch. If the name is not a branch and appears to be a commit
                hash, the hash will be returned without modification.
        """
        LOGGER.info('Checking if target branch {} exists in {}'.format(branch, git_url))

        try:
            out, _ = exectools.cmd_assert('git ls-remote --heads {} refs/heads/{}'.format(git_url, branch), retries=3)
        except Exception as err:
            # We don't expect and exception if the branch does not exist; just an empty string
            LOGGER.error('Error attempting to find target branch {} hash: {}'.format(branch, err))
            return None
        result = out.strip()  # any result means the branch is found; e.g. "7e66b10fbcd6bb4988275ffad0a69f563695901f	refs/heads/some_branch")
        if not result and SourceResolver.is_branch_commit_hash(branch):
            return branch  # It is valid hex; just return it

        return result.split()[0] if result else None

    def register_source_alias(self, alias: str, path: str):
        LOGGER.info("Registering source alias %s: %s" % (alias, path))
        path = os.path.abspath(path)
        assertion.isdir(path, "Error registering source alias %s" % alias)
        with exectools.Dir(path):
            url = None
            origin_url = "?"
            rc1, out_origin, err_origin = exectools.cmd_gather(["git", "config", "--get", "remote.origin.url"])
            if rc1 == 0:
                url = out_origin.strip()
                # Usually something like "git@github.com:openshift/origin.git"
                # But we want an https hyperlink like http://github.com/openshift/origin
                if url.startswith("git@"):
                    origin_url = art_util.convert_remote_git_to_https(url)
                else:
                    origin_url = url
            else:
                LOGGER.error("Failed acquiring origin url for source alias %s: %s" % (alias, err_origin))

            branch = None
            rc2, out_branch, err_branch = exectools.cmd_gather(["git", "rev-parse", "--abbrev-ref", "HEAD"])
            if rc2 == 0:
                branch = out_branch.strip()
            else:
                LOGGER.error("Failed acquiring origin branch for source alias %s: %s" % (alias, err_branch))

            if not url or not branch:
                raise IOError(f"Couldn't detect source URL or branch for local source {path}. Is it a valid Git repo?")

            # Check if pull remote exists
            pull_url = None
            rc_pull, out_pull, _ = exectools.cmd_gather(["git", "config", "--get", "remote.pull.url"])
            if rc_pull == 0:
                pull_url = out_pull.strip()

            out, _ = exectools.cmd_assert(["git", "rev-parse", "HEAD"])
            commit_hash = out.strip()

            out, _ = exectools.cmd_assert("git log -1 --format=%ct")
            comitter_date = datetime.fromtimestamp(float(out.strip()), timezone.utc)
            out, _ = exectools.cmd_assert("git describe --always --tags HEAD")
            latest_tag = out.strip()

            has_public_upstream = False
            public_upstream_url = origin_url
            public_upstream_branch = branch
            if branch != 'HEAD' and self._group_config.public_upstreams:
                # If branch == HEAD, our source is a detached HEAD.
                public_upstream_url, public_upstream_branch_override, has_public_upstream = self.get_public_upstream(
                    url, self._group_config.public_upstreams
                )
                if public_upstream_branch_override:
                    public_upstream_branch = public_upstream_branch_override

            resolution = SourceResolution(
                source_path=path,
                url=url,
                branch=branch,
                https_url=origin_url,
                commit_hash=commit_hash,
                committer_date=comitter_date,
                latest_tag=latest_tag,
                has_public_upstream=has_public_upstream,
                public_upstream_url=public_upstream_url,
                public_upstream_branch=public_upstream_branch,
                pull_url=pull_url,
            )
            self.source_resolutions[alias] = resolution
            if self._record_logger:
                self._record_logger.add_record(
                    "source_alias", alias=alias, origin_url=origin_url, branch=branch or '?', path=path
                )
            if self._state_holder is not None:
                self._state_holder[alias] = {
                    'url': origin_url,
                    'branch': branch or '?',
                    'path': path,
                }
            return resolution

    @staticmethod
    def get_public_upstream(remote_git: str, public_upstreams: ListModel) -> Tuple[str, Optional[str], bool]:
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
        :param public_upstreams: The public upstream configuration from group.yaml.
        :return: tuple (url, branch, has_public_upstream)
            - url: An https normalized remote address with private repo information replaced. If there is no
                   applicable private repo replacement, remote_git will be returned (normalized to https).
            - branch: Optional public branch name if the public upstream source use a different branch name from the private upstream.
            - has_public_upstream: True if the public upstream source is found in the public_upstreams mapping.
        """
        remote_https = art_util.convert_remote_git_to_https(remote_git)

        if public_upstreams:
            # We prefer the longest match in the mapping, so iterate through the entire
            # map and keep track of the longest matching private remote.
            target_priv_prefix = None
            target_pub_prefix = None
            target_pub_branch = None
            for upstream in public_upstreams:
                priv = upstream["private"]
                pub = upstream["public"]
                # priv can be a full repo, or an organization (e.g. git@github.com:openshift)
                # It will be treated as a prefix to be replaced
                https_priv_prefix = art_util.convert_remote_git_to_https(
                    priv
                )  # Normalize whatever is specified in group.yaml
                https_pub_prefix = art_util.convert_remote_git_to_https(pub)
                if remote_https.startswith(f'{https_priv_prefix}/') or remote_https == https_priv_prefix:
                    # If we have not set the prefix yet, or if it is longer than the current contender
                    if not target_priv_prefix or len(https_priv_prefix) > len(target_pub_prefix):
                        target_priv_prefix = https_priv_prefix
                        target_pub_prefix = https_pub_prefix
                        target_pub_branch = upstream.get("public_branch")

            if target_priv_prefix:
                repo_path = remote_https[len(target_priv_prefix) :]  # e.g., "/migtools-mig-operator"

                # Extract public org name
                pub_org = target_pub_prefix.split('/')[-1]  # e.g., "migtools"

                # If public org is not "openshift", try removing org prefix from repo name
                if pub_org != "openshift":
                    repo_name = repo_path.lstrip('/')  # "migtools-mig-operator"
                    if repo_name.startswith(f"{pub_org}-"):
                        # Remove the org prefix: "migtools-mig-operator" → "mig-operator"
                        repo_without_prefix = repo_name.removeprefix(f"{pub_org}-")
                        return f'{target_pub_prefix}/{repo_without_prefix}', target_pub_branch, True

                # Fallback to original behavior
                return f'{target_pub_prefix}{repo_path}', target_pub_branch, True

        return remote_https, None, False

    @staticmethod
    def setup_and_fetch_public_upstream_source(public_source_url: str, public_upstream_branch: str, source_dir: str):
        """
        Fetch public upstream source for specified Git repository. Set up public_upstream remote if needed.

        :param public_source_url: HTTPS Git URL of the public upstream source
        :param public_upstream_branch: Git branch of the public upstream source
        :param source_dir: Path to the local Git repository
        """
        out, _ = exectools.cmd_assert(["git", "-C", source_dir, "remote"])
        if 'public_upstream' not in out.strip().split():
            exectools.cmd_assert(["git", "-C", source_dir, "remote", "add", "--", "public_upstream", public_source_url])
        else:
            exectools.cmd_assert(
                ["git", "-C", source_dir, "remote", "set-url", "--", "public_upstream", public_source_url]
            )
        exectools.cmd_assert(
            ["git", "-C", source_dir, "fetch", "--", "public_upstream", public_upstream_branch],
            retries=3,
            set_env=constants.GIT_NO_PROMPTS,
        )

    @staticmethod
    def get_source_dir(source: SourceResolution, metadata: 'Metadata', check=True) -> Path:
        if not metadata.has_source():
            raise ValueError("Metadata does not have a source")
        source_dir = Path(source.source_path)
        sub_path = metadata.config.content.source.path
        if sub_path is not Missing:
            source_dir = source_dir.joinpath(str(sub_path))
        if check and not source_dir.is_dir():
            raise FileNotFoundError(f"Source directory {source_dir} doesn't exist")
        return source_dir
