import string
import warnings
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, RootModel, model_validator

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
    include_packages: list[str] = []
    exclude_packages: list[str] = []
    arches: list[BrewArch] | None = None
    source: "BrewSource"
    # Name of the public (non-embargoed) sibling repo. Set on the EMBARGOED repo to
    # symlink its flavor directory to the public sibling's when there is no embargoed content.
    embargoed_sibling: str | None = None
    # Opt-in to the embargoed -> public symlink optimization. Set on the embargoed repo.
    symlink_when_unembargoed: bool = False

    @model_validator(mode="after")
    def _validate_symlink_fields(self) -> "PlashetRepo":
        if self.symlink_when_unembargoed:
            if not self.include_embargoed:
                raise ValueError(
                    "symlink_when_unembargoed=True requires include_embargoed=True "
                    "(the optimization only applies to an embargoed repo symlinking to its public sibling)"
                )
            if not self.embargoed_sibling:
                raise ValueError(
                    "symlink_when_unembargoed=True requires embargoed_sibling to be set "
                    "to the name of the public sibling repo"
                )
        return self


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


class RepoList(RootModel[list[Repo]]):
    @model_validator(mode="after")
    def _validate_embargoed_siblings(self) -> "RepoList":
        # Build a lookup of repo name -> Repo for sibling resolution.
        repos_by_name = {repo.name: repo for repo in self.root}

        # Fields that must be identical between an embargoed repo and its public sibling.
        # The pair may only differ by flavor: include_embargoed / slug /
        # embargoed_sibling / symlink_when_unembargoed.
        shared_fields = (
            'source',
            'include_previous_packages',
            'include_packages',
            'exclude_packages',
            'arches',
        )

        for repo in self.root:
            plashet = repo.plashet
            if plashet is None or not plashet.embargoed_sibling:
                continue

            sibling_name = plashet.embargoed_sibling

            # (a) the named sibling must exist and be a plashet repo.
            sibling = repos_by_name.get(sibling_name)
            if sibling is None:
                raise ValueError(
                    f"Repo '{repo.name}' declares embargoed_sibling '{sibling_name}', "
                    f"but no repo with that name exists in the repo list."
                )
            if sibling.type != 'plashet' or sibling.plashet is None:
                raise ValueError(
                    f"Repo '{repo.name}' declares embargoed_sibling '{sibling_name}', "
                    f"but '{sibling_name}' is not a plashet repo (type={sibling.type!r})."
                )

            # (b) SAFETY: the sibling must be PUBLIC. Refuse an embargoed symlink target.
            if sibling.plashet.include_embargoed:
                raise ValueError(
                    f"Repo '{repo.name}' declares embargoed_sibling '{sibling_name}', "
                    f"but '{sibling_name}' is itself embargoed (include_embargoed=True). "
                    f"The symlink target MUST be a public (non-embargoed) repo; refusing to "
                    f"risk embargoed content appearing in a non-embargoed repo."
                )

            # (c) SAFETY: the pair must use DIFFERENT slugs. The slug determines the flavor's
            # on-disk directory; if the embargoed repo and its public sibling shared a slug they
            # would collapse to a single directory, defeating the two-flavor layout the symlink
            # optimization relies on (and risking an embargoed build overwriting the public one).
            mine_slug = plashet.slug or repo.name
            their_slug = sibling.plashet.slug or sibling.name
            if mine_slug == their_slug:
                raise ValueError(
                    f"Repo '{repo.name}' and its embargoed_sibling '{sibling_name}' must have "
                    f"different slugs, but both resolve to slug {mine_slug!r}. A shared slug "
                    f"collapses both flavors into one directory."
                )

            # (d) the pair must be identical except for the flavor-distinguishing fields.
            for field in shared_fields:
                mine = getattr(plashet, field)
                theirs = getattr(sibling.plashet, field)
                if mine != theirs:
                    raise ValueError(
                        f"Repo '{repo.name}' and its embargoed_sibling '{sibling_name}' "
                        f"must be identical except for the flavor, but field '{field}' differs: "
                        f"{mine!r} != {theirs!r}."
                    )

        # Global uniqueness: the slug is the only per-repo variable in the plashet output
        # path, so two enabled plashet repos sharing a slug would write the same directory
        # (the second clobbering the first, including a public sibling a symlink relies on).
        slug_owner: dict[str, str] = {}
        for repo in self.root:
            if repo.disabled or repo.type != 'plashet' or repo.plashet is None:
                continue
            slug = repo.plashet.slug or repo.name
            if slug in slug_owner:
                raise ValueError(
                    f"Plashet repos '{slug_owner[slug]}' and '{repo.name}' resolve to the same "
                    f"output path (slug {slug!r}); each plashet repo must have a unique slug."
                )
            slug_owner[slug] = repo.name

        return self
