import string
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, RootModel

if TYPE_CHECKING:
    from artcommonlib.config.plashet import PlashetConfig

RepoType = Literal['external', 'plashet']
BrewArch = Literal['x86_64', 's390x', 'ppc64le', 'aarch64']


class PlashetRepo(BaseModel):
    disabled: bool = False
    slug: str | None = None
    assembly_aware: bool = False
    embargo_aware: bool = False
    include_embargoed: bool = False
    include_previous_packages: list[str] = []
    arches: list[BrewArch] | None = None
    source: "BrewSource"


class BrewSource(BaseModel):
    type: Literal['brew']
    from_tags: list["BrewTag"]
    embargoed_tags: list[str] = []


class BrewTag(BaseModel):
    name: str
    product_version: str
    release_tag: str | None = None
    inherit: bool = False


class ContentSet(BaseModel):
    optional: bool = False
    default: str | None = None
    x86_64: str | None = None
    s390x: str | None = None
    ppc64le: str | None = None
    aarch64: str | None = None


class RepoSync(BaseModel):
    enabled: bool = True
    latest_only: bool = True


class Repo(BaseModel):
    name: str
    disabled: bool = False
    type: RepoType
    plashet: PlashetRepo | None = None
    conf: dict | None = None
    content_set: ContentSet | None = None
    reposync: RepoSync = RepoSync()

    def construct_download_url(
        self,
        arch: str,
        plashet_config: "PlashetConfig | None" = None,
        replace_vars: dict | None = None,
        plashet_dir: str | None = None,
    ) -> str:
        """
        Construct the download URL for this repo.

        For plashet repos:
            Returns: {base_url}/{base_dir}/{plashet_dir_or_symlink}/{arch}/os/
            Example: https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/rhocp-rhel-4.17/stream/el9/repo-name/x86_64/os/
            Or with symlink: https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/rhocp-rhel-4.17/stream/el9/latest/x86_64/os/

        For external repos:
            Returns the URL from conf.baseurl.$arch or conf.baseurl.default

        Args:
            arch: Architecture (required)
            plashet_config: PlashetConfig instance (required for plashet repos)
            replace_vars: Dictionary of template variables for URL substitution (e.g., {'runtime_assembly': 'stream', 'slug': 'el9'})
            plashet_dir: Specific plashet directory to use in URL. If None, uses symlink_name from plashet_config.

        Returns:
            The constructed download URL

        Raises:
            ValueError: If required parameters are missing
        """
        if self.type == 'plashet':
            if not plashet_config:
                raise ValueError('plashet_config is required for plashet repos')
            if not self.plashet:
                raise ValueError('plashet configuration is missing for plashet repo')

            # Get slug (defaults to repo name if not specified)
            slug = self.plashet.slug or self.name

            # Variables for template substitution
            vars_dict = {
                'slug': slug,
                'arch': arch,
            }
            # Merge in any additional variables provided by caller
            if replace_vars:
                vars_dict.update(replace_vars)

            # Legacy support: Check if deprecated download_url is set
            if plashet_config.download_url:
                import warnings

                warnings.warn(
                    "PlashetConfig.download_url is deprecated and will be removed in a future version. "
                    "The download URL is now constructed dynamically from base_url and other fields.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                # Substitute variables in download_url template
                download_url_template = string.Template(plashet_config.download_url)
                return download_url_template.substitute(vars_dict)

            # Substitute variables in base_dir
            base_dir_template = string.Template(plashet_config.base_dir)
            base_dir = base_dir_template.substitute(vars_dict)

            base_url = plashet_config.base_url.rstrip('/')

            # Use provided plashet_dir or default to symlink_name
            dir_in_url = plashet_dir if plashet_dir is not None else plashet_config.symlink_name

            # Validate that dir_in_url is not empty
            if not dir_in_url:
                raise ValueError('plashet_dir cannot be empty')

            # Construct URL: {base_url}/{base_dir}/{dir}/{arch}/os/
            url_parts = [base_url, base_dir, dir_in_url, arch]

            if plashet_config.create_repo_subdirs:
                url_parts.append(plashet_config.repo_subdir)

            return '/'.join(url_parts) + '/'

        elif self.type == 'external':
            if not self.conf:
                raise ValueError('conf is missing for external repo')

            baseurl = self.conf.get('baseurl', {})
            if not baseurl:
                raise ValueError('conf.baseurl is missing for external repo')

            # Try arch-specific URL first, then default
            if arch in baseurl:
                return baseurl[arch]
            elif 'default' in baseurl:
                return baseurl['default']
            else:
                raise ValueError(f'No baseurl found for arch={arch} or default')

        else:
            raise ValueError(f'Unknown repo type: {self.type}')


RepoList = RootModel[list[Repo]]
