import asyncio
import fnmatch
import json
import os
import threading
import time
from string import Template
from typing import Dict, List, Optional, Union, cast

import requests
import yaml
from artcommonlib import logutil
from artcommonlib.config.plashet import PlashetConfig
from artcommonlib.config.repo import Repo as RepoConf
from artcommonlib.config.repo import RepoList
from artcommonlib.exectools import limit_concurrency
from artcommonlib.model import Missing, Model

from doozerlib.constants import KONFLUX_REPO_CA_BUNDLE_FILENAME, KONFLUX_REPO_CA_BUNDLE_TMP_PATH
from doozerlib.repodata import Repodata, RepodataLoader

DEFAULT_REPOTYPES = ['unsigned', 'signed']

# This architecture is handled differently in some cases for legacy reasons
ARCH_X86_64 = "x86_64"

LOGGER = logutil.get_logger(__name__)


class Repo(object):
    """Represents a single yum repository and provides sane ways to
    access each property based on the arch or repo type."""

    @staticmethod
    def from_repo_config(
        repo_config: RepoConf,
        valid_arches: List[str],
        gpgcheck: bool = True,
        plashet_config: Optional[PlashetConfig] = None,
        template_vars: Optional[Dict[str, str]] = None,
    ) -> 'Repo':
        """
        Create a Repo instance from a new-style RepoConf (Pydantic model).

        :param repo_config: New-style RepoConf Pydantic model
        :param valid_arches: List of valid architectures
        :param gpgcheck: Whether to enable GPG signature checking
        :param plashet_config: Global plashet configuration (required for plashet type repos)
        :param template_vars: Additional template variables for URL substitution (e.g., {'slug': 'el9-embargoed'})
        :return: Repo instance
        """
        # Convert new-style RepoConf to old-style dict
        repo_dict = {}
        arches = list(valid_arches)

        # Handle plashet type repos
        if repo_config.type == 'plashet':
            if not plashet_config:
                raise ValueError(f"Repo '{repo_config.name}' is of type 'plashet' but no plashet_config was provided")
            if not repo_config.plashet:
                raise ValueError(f"Repo '{repo_config.name}' doesn't have required key `plashet`.")

            if plashet_arches := repo_config.plashet.arches or plashet_config.arches:
                plashet_arches = set(plashet_arches)
                arches = [arch for arch in arches if arch in plashet_arches]

            vars_dict = {}
            if template_vars:
                vars_dict.update(template_vars)

            # Build conf dict with baseurl for each architecture
            conf = repo_config.conf.copy() if repo_config.conf else {}

            # If baseurl is not provided in repo config, construct it from plashet config
            if 'baseurl' not in conf or not conf['baseurl']:
                vars_dict['slug'] = repo_config.plashet.slug or repo_config.name
                baseurl_dict = {}

                for arch in arches:
                    vars_dict['arch'] = arch

                    # Use string.Template to substitute variables in download_url
                    template = Template(plashet_config.download_url)
                    baseurl_dict[arch] = template.substitute(vars_dict)

                conf['baseurl'] = baseurl_dict
            repo_dict['conf'] = conf

        # Handle external type repos (existing logic)
        elif repo_config.type == 'external':
            if repo_config.conf:
                repo_dict['conf'] = repo_config.conf
        else:
            raise ValueError(f"Unsupported repo type: {repo_config.type}")

        # Add content_set if present
        if repo_config.content_set:
            repo_dict['content_set'] = repo_config.content_set.model_dump(exclude_none=True)

        # Add reposync if present
        if repo_config.reposync:
            repo_dict['reposync'] = repo_config.reposync.model_dump(exclude_none=True)

        return Repo(repo_config.name, repo_dict, list(arches), gpgcheck)

    def __init__(self, name: str, data: Dict, valid_arches: List[str], gpgcheck: bool = True):
        """
        Initialize a Repo instance.

        :param name: Repository name
        :param data: Repository configuration as old-style dict with 'conf' and 'content_set' keys
        :param valid_arches: List of valid architectures
        :param gpgcheck: Whether to enable GPG signature checking
        """
        self.name = name
        self._valid_arches = valid_arches
        self._invalid_cs_arches = set()

        self._data = Model(data)
        for req in ['conf', 'content_set']:
            if req not in self._data:
                raise ValueError('Repo definitions must contain "{}" key!'.format(req))
        if self._data.conf.baseurl is Missing:
            raise ValueError('Repo definitions must include conf.baseurl!')

        # fill out default conf values
        conf = self._data.conf
        conf.name = conf.get('name', name)
        self.gpgcheck = gpgcheck

        def pkgs_to_list(pkgs_str):
            pkgs_str = pkgs_str.strip()
            if "," in pkgs_str and " " in pkgs_str:
                raise ValueError('pkgs string cannot contain both commas and spaces')
            elif "," in pkgs_str:
                return pkgs_str.split(",")
            elif " " in pkgs_str:
                return pkgs_str.split(" ")
            elif pkgs_str:
                return [pkgs_str]
            return []

        # these will be used to filter the packages
        includepkgs_str = conf.get('extra_options', {}).get('includepkgs', "")
        self.includepkgs = pkgs_to_list(includepkgs_str)

        excludepkgs_str = conf.get('extra_options', {}).get('exclude', "") or conf.get('extra_options', {}).get(
            'excludepkgs', ""
        )
        self.excludepkgs = pkgs_to_list(excludepkgs_str)

        self.cs_optional = self._data.content_set.get('optional', False)

        self.repotypes = DEFAULT_REPOTYPES
        self.baseurl(DEFAULT_REPOTYPES[0], self._valid_arches[0])  # run once just to populate self.repotypes
        self.reposync_enabled = (
            True
            if self._data.reposync.enabled is Missing or self._data.reposync.enabled
            else self._data.reposync.enabled
        )
        self.reposync_latest_only = (
            True if self._data.reposync.latest_only is Missing or self._data.reposync.latest_only else False
        )

        # A yum repo's repodata directory (e.g. https://rhsm-pulp.corp.redhat.com/content/eus/rhel8/8.6/x86_64/appstream/os/repodata/)
        # contains repository metadata.
        # This fields holds a cache for the repository metadata.
        self._repodatas: Dict[str, Repodata] = {}  # key is arch, value is Repodata instance
        self._repodata_cache_locks = {arch: threading.Lock() for arch in valid_arches}

    @property
    def enabled(self):
        """Allows access via repo.enabled"""
        val = self._data.conf.enabled
        if isinstance(val, str):
            val = val.lower()
            if val in ('true', 'yes'):
                return True
            if val in ('false', 'no'):
                return False
            raise ValueError(f"Invalid enabled option {val}")
        if val in (Missing, 1, True):
            return True
        if val in (0, False):
            return False
        raise ValueError(f"Invalid enabled option {val}")

    @property
    def arches(self):
        return list(self._valid_arches)

    @enabled.setter
    def enabled(self, val):
        """Set enabled option without digging direct into the underlying data"""
        self._data.conf.enabled = 1 if val else 0

    def set_invalid_cs_arch(self, arch):
        self._invalid_cs_arches.add(arch)

    def is_reposync_enabled(self):
        return self.reposync_enabled

    def is_reposync_latest_only(self):
        return self.reposync_latest_only

    def __repr__(self):
        """For debugging mainly, to display contents as a dict"""
        return str(self._data)

    def baseurl(self, repotype, arch):
        if not repotype:
            repotype = 'unsigned'
        """Get baseurl based on repo type, if one was specified for this repo."""
        bu = self._data.conf.baseurl
        if isinstance(bu, str):
            return bu
        elif isinstance(bu, dict):
            if arch in bu:
                bu_sub = bu
            else:
                if repotype not in bu:
                    raise ValueError('{} is not a valid repotype option in {}'.format(repotype, list(bu.keys())))
                self.repotypes = list(bu.keys())
                bu_sub = bu[repotype]
            if isinstance(bu_sub, str):
                return bu_sub
            elif isinstance(bu_sub, dict):
                if arch not in self._valid_arches:
                    raise ValueError('{} is not a valid arch option!'.format(arch))
                if arch not in bu_sub:
                    raise ValueError('No baseurl available for arch {}'.format(arch))
                return bu_sub[arch]
            return bu[repotype]
        else:
            raise ValueError('baseurl must be str or dict!')

    def content_set(self, arch):
        """Return content set name for given arch with sane fallbacks and error handling."""

        if arch not in self._valid_arches:
            raise ValueError(f'{arch} is not a valid arch!')
        if arch in self._invalid_cs_arches:
            return None
        if self._data.content_set[arch] is Missing:
            if self._data.content_set['default'] is Missing:
                if self._data.content_set['optional'] is not Missing and self._data.content_set['optional']:
                    return ''
                else:
                    raise ValueError(
                        '{} does not contain a content_set for {} and no default was provided.'.format(self.name, arch)
                    )
            return self._data.content_set['default']
        else:
            return self._data.content_set[arch]

    def conf_section(self, repotype, arch=ARCH_X86_64, enabled=None, section_name=None, konflux=False):
        """
        Returns a str that represents a yum repo configuration section corresponding
        to this repo in group.yml.

        :param repotype: Whether to use signed or unsigned repos from group.yml
        :param arch: The architecture this section if being generated for (e.g. ppc64le or x86_64).
        :param enabled: If True|False, explicitly set 'enabled = 1|0' in section. If None, inherit group.yml setting.
        :param section_name: The section name to use if not the repo name in group.yml.
        :param konflux: If True, set custom cert path for Konflux
        :return: Returns a string representing a repo section in a yum configuration file. e.g.
            [rhel-7-server-ansible-2.4-rpms]
            gpgcheck = 0
            enabled = 0
            baseurl = http://pulp.dist.pr.../x86_64/ansible/2.4/os/
            name = rhel-7-server-ansible-2.4-rpms
        """
        if not repotype:
            repotype = 'unsigned'

        if arch not in self._valid_arches:
            raise ValueError('{} does not identify a yum repository for arch: {}'.format(self.name, arch))

        if not section_name:  # If the caller has not specified a name, use group.yml name.
            section_name = self.name

        # Get baseurl once and reuse it
        baseurl_value = self.baseurl(repotype, arch)

        result = '[{}]\n'.format(section_name)

        # Sort keys so they are always in the same order, makes unit
        # testing much easier
        for k in sorted(self._data.conf.keys()):
            v = self._data.conf[k]

            if k == 'ci_alignment':
                # Special keyword that does not translate to yum conf content.
                continue

            line = '{} = {}\n'
            if k == 'baseurl':
                line = line.format(k, baseurl_value)
            elif k == 'name':
                line = line.format(k, section_name)
            elif k == 'extra_options':
                opt_lines = ''
                for opt, val in v.items():
                    opt_lines += "{} = {}\n".format(opt, val)
                line = opt_lines
            else:
                if k == 'enabled' and enabled is not None:
                    v = 1 if enabled else 0
                line = line.format(k, v)

            result += line

        # Usually, gpgcheck will not be specified, in build metadata, but don't override if it is there
        if (
            self._data.conf.get('gpgcheck', None) is None
            and self._data.conf.get('extra_options', {}).get('gpgcheck', None) is None
        ):
            # If we are building a signed repo file, and overall gpgcheck is desired
            if repotype == 'signed' and self.gpgcheck:
                result += 'gpgcheck = 1\n'
            else:
                result += 'gpgcheck = 0\n'

        if (
            self._data.conf.get('gpgkey', None) is None
            and self._data.conf.get('extra_options', {}).get('gpgkey', None) is None
        ):
            # This key will bed used only if gpgcheck=1
            result += 'gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release\n'

        if konflux and 'ocp-artifacts' in baseurl_value or 'download.devel.redhat.com/' in baseurl_value:
            result += f'sslcacert = {KONFLUX_REPO_CA_BUNDLE_TMP_PATH}/{KONFLUX_REPO_CA_BUNDLE_FILENAME}\n'

        result += '\n'

        return result

    @limit_concurrency(limit=8)
    async def get_repodata(self, arch: str):
        repodata = self._repodatas.get(arch)
        if repodata:
            return repodata
        name = f"{self.name}-{arch}"
        repourl = cast(str, self.baseurl("unsigned", arch))
        repodata = self._repodatas[arch] = await RepodataLoader().load(name, repourl)

        if self.excludepkgs:
            LOGGER.info(f"Excluding packages from {name} based on following patterns: {self.excludepkgs}")
            filtered_rpms = []
            for rpm in repodata.primary_rpms:
                # rpm should not match any exclude pattern to be included
                matches_exclude = False
                for exclude_pattern in self.excludepkgs:
                    if fnmatch.fnmatch(rpm.name, exclude_pattern):
                        matches_exclude = True
                        break
                if not matches_exclude:
                    filtered_rpms.append(rpm)
            repodata.primary_rpms = filtered_rpms

        # includepkgs does not override excludepkgs
        # so apply it after excludepkgs
        if self.includepkgs:
            LOGGER.info(
                f"Only including packages from {name} based on following patterns: {self.includepkgs}. "
                "All other packages will be excluded."
            )
            filtered_rpms = []
            for rpm in repodata.primary_rpms:
                # rpm should match at least one include pattern to be included
                for include_pattern in self.includepkgs:
                    if fnmatch.fnmatch(rpm.name, include_pattern):
                        filtered_rpms.append(rpm)
                        break
            repodata.primary_rpms = filtered_rpms

        return repodata

    async def get_repodata_threadsafe(self, arch: str):
        repodata = self._repodatas.get(arch)
        if repodata:
            return repodata
        lock = self._repodata_cache_locks[arch]
        while not lock.acquire(blocking=False):
            await asyncio.sleep(1)
        try:
            return await self.get_repodata(arch)
        finally:
            lock.release()


# Base header for all content_sets.yml output
CONTENT_SETS = """
# This file is managed by the doozer build tool: https://github.com/openshift-eng/doozer,
# by the OpenShift Automated Release Team (#forum-ocp-art on CoreOS Slack).
# Any manual changes will be overwritten by doozer on the next build.
#
# This is a file defining which content sets (yum repositories) are needed to
# update content in this image. Data provided here helps determine which images
# are vulnerable to specific CVEs. Generally you should only need to update this
# file when:
#    1. You start depending on a new product
#    2. You are preparing new product release and your content sets will change
#
# See https://source.redhat.com/groups/public/container-build-system/container_build_system_wiki/guide_to_layered_image_build_service_osbs
# for more information on maintaining this file and the format and examples.
#
# You should have one top level item for each architecture being built. Most
# likely this will be x86_64 and ppc64le initially.
---
"""


class Repos(object):
    """
    Represents the entire collection of repos and provides
    automatic content_set and repo conf file generation.
    """

    def __init__(
        self,
        repos: Union[Dict[str, Dict], RepoList],
        arches: List[str],
        gpgcheck=True,
        plashet_config: Optional[PlashetConfig] = None,
        template_vars: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize Repos collection.

        :param repos: Repository configuration (old-style dict or new-style RepoList)
        :param arches: List of valid architectures
        :param gpgcheck: Whether to enable GPG signature checking
        :param plashet_config: Global plashet configuration (required for plashet type repos)
        :param template_vars: Additional template variables for URL substitution
        """
        self._arches = arches
        self._repos: Dict[str, Repo] = {}
        repotypes = []
        names = []

        # Handle both old-style dict and new-style RepoList
        if isinstance(repos, RepoList):
            # New-style RepoList: process both external and plashet repos
            for repo_config in repos.root:
                if repo_config.disabled:
                    continue
                # Skip repos that are neither external nor plashet
                if repo_config.type not in ['external', 'plashet']:
                    continue

                # Use static method to convert new-style RepoConf to Repo instance
                names.append(repo_config.name)
                self._repos[repo_config.name] = Repo.from_repo_config(
                    repo_config,
                    self._arches,
                    gpgcheck=gpgcheck,
                    plashet_config=plashet_config,
                    template_vars=template_vars,
                )
                repotypes.extend(self._repos[repo_config.name].repotypes)
        else:
            # Old-style dict: pass directly to Repo constructor
            for name, repo in repos.items():
                names.append(name)
                self._repos[name] = Repo(name, repo, self._arches, gpgcheck=gpgcheck)
                repotypes.extend(self._repos[name].repotypes)

        self.names = tuple(names)
        self.repotypes = list(set(repotypes))  # leave only unique values

    def __getitem__(self, item: str) -> Repo:
        """Allows getting a Repo() object simply by name via repos[repo_name]"""
        if item not in self._repos:
            raise ValueError('{} is not a valid repo name!'.format(item))
        return self._repos[item]

    def items(self):
        return self._repos.items()

    def values(self):
        return self._repos.values()

    def __repr__(self):
        """Mainly for debugging to dump a dict representation of the collection"""
        return str(self._repos)

    def repo_file(self, repo_type, enabled_repos=[], arch=None, konflux=False, omit_disabled_repos=True):
        """
        Returns a str defining a list of repo configuration sections for a yum configuration file.
        :param repo_type: Whether to prefer signed or unsigned repos.
        :param enabled_repos: A list of group.yml repo names which should be enabled. A repo is considered
            enabled only if it is both enabled in group.yml AND listed in this parameter. An empty list
            means all repos are disabled.
        :param arch: The architecture for which this repository should be generated. If None, all architectures
            will be included in the returned str.
        :param konflux: Whether to generate Konflux-specific repo configuration.
        :param omit_disabled_repos: If True, disabled repos will be omitted from the generated repo file.
            If False, disabled repos will be included with enabled=0 and a warning will be printed.
        """

        result = ''
        for r in self._repos.values():
            # A repo is enabled only if BOTH conditions are true:
            # 1. enabled in group.yml (r.enabled)
            # 2. listed in enabled_repos
            enabled = r.enabled and r.name in enabled_repos

            if not enabled:
                if omit_disabled_repos:
                    # Skip disabled repos
                    continue
                else:
                    # Include disabled repo with enabled=0 and print warning
                    LOGGER.warning(f"Repository '{r.name}' is disabled but included in repo file")

            if arch:  # Generating a single arch?
                # Just use the configured name for the set. This behavior needs to be preserved to
                # prevent changing mirrored repos by reposync.
                result += r.conf_section(repo_type, enabled=enabled, arch=arch, section_name=r.name, konflux=konflux)
            else:
                # When generating a repo file for multi-arch builds, we need all arches in the same repo file.
                for iarch in r.arches:
                    section_name = '{}-{}'.format(r.name, iarch)
                    result += r.conf_section(
                        repo_type, enabled=enabled, arch=iarch, section_name=section_name, konflux=konflux
                    )

        return result

    def content_sets(self, enabled_repos=[], non_shipping_repos=[]):
        """Generates a valid content_sets.yml file based on the currently
        configured and enabled repos in the collection. Using the correct
        name for each arch."""
        # check for missing repos
        missing_repos = set(enabled_repos) - self._repos.keys()
        if missing_repos:
            raise ValueError(f"enabled_repos references undefined repo(s): {missing_repos}")
        result = {}
        globally_enabled_repos = {r.name for r in self._repos.values() if r.enabled}
        shipping_repos = (set(globally_enabled_repos) | set(enabled_repos)) - set(non_shipping_repos)
        for a in sorted(self._arches):
            content_sets = []
            for r in shipping_repos:
                cs = self._repos[r].content_set(a)
                if cs:  # possible to be forced off by setting to null
                    content_sets.append(cs)
            if content_sets:
                result[a] = sorted(content_sets)
        return CONTENT_SETS + yaml.dump(result, default_flow_style=False)

    def _validate_content_sets(self, arch, names):
        url = "https://rhsm-pulp.corp.redhat.com/pulp/api/v2/repositories/search/"
        payload = {
            "criteria": {
                "fields": [
                    "id",
                    "notes",
                ],
                "filters": {
                    "notes.arch": {
                        "$in": [
                            arch,
                        ],
                    },
                    # per CLOUDWF-4852 content sets may now be specified as pulp repo names.
                    "$or": [
                        {
                            "notes.content_set": {
                                "$in": names,
                            },
                        },
                        {
                            "id": {
                                "$in": names,
                            },
                        },
                    ],
                },
            },
        }

        headers = {
            'Content-Type': "application/json",
            'Authorization': "Basic cWE6cWE=",  # qa:qa
            'Cache-Control': "no-cache",
        }

        # as of 2023-06-09 authentication is required to validate content sets with rhsm-pulp
        cs_auth_key = os.environ.get("RHSM_PULP_KEY")
        cs_auth_cert = os.environ.get("RHSM_PULP_CERT")

        retry_count = 4
        for i in range(retry_count):
            try:
                response = requests.request(
                    "POST",
                    url,
                    data=json.dumps(payload),
                    headers=headers,
                    cert=(cs_auth_cert, cs_auth_key),
                )
                break
            except:
                if i == retry_count - 1:
                    raise
                time.sleep(5)

        resp_dict = response.json()

        result = set()
        for repo in [Model(repo) for repo in resp_dict]:
            # per CLOUDWF-4852 content sets may now be specified as pulp repo names.
            # since we may be searching by either, return both to be compared against the request.
            result.update([repo.id, repo.notes.content_set])

        return result

    def validate_content_sets(self):
        # Determine repos that have no content sets defined at all; we will give these a pass if nothing tries to use them.
        # This is one reason to accept it if no content_set is defined at all: https://github.com/openshift-eng/ocp-build-data/pull/594

        content_set_defined = {}
        for name, repo in self._repos.items():
            content_set_defined[name] = False
            for arch in self._arches + ['default']:
                if repo._data.content_set[arch] is not Missing:
                    content_set_defined[name] = True

        invalid = []
        for arch in self._arches:
            cs_names = {}
            for name, repo in self._repos.items():
                if content_set_defined[name]:
                    cs = repo.content_set(arch)
                    cs_names[name] = cs

            arch_cs_values = list(cs_names.values())
            if arch_cs_values:
                # no point in making empty call
                valid = self._validate_content_sets(arch, arch_cs_values)
                for name, cs in cs_names.items():
                    if cs not in valid:
                        if not self._repos[name].cs_optional:
                            invalid.append('{}/{}'.format(arch, cs))
                        self._repos[name].set_invalid_cs_arch(arch)

        if invalid:
            cs_lst = ', '.join(invalid)
            raise ValueError('The following content set names are given, do not exist, and are not optional: ' + cs_lst)
