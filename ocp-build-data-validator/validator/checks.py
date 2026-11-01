"""Product-independent checks used by Prow, separate from legacy CLI policy."""

import posixpath
import re
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError
from urllib3.util.retry import Retry


class RemoteError(ValueError):
    """A remote service could not provide an authoritative validation result."""


def load_mapping(path: Path) -> dict:
    """Reject empty documents, duplicate keys, and non-mapping YAML."""
    with path.open() as source:
        data = YAML(typ='safe').load(source)
    if not isinstance(data, dict):
        raise ValueError(f'{path.name} must contain a nonempty YAML mapping')
    return data


def validate_image(data: dict) -> list[str]:
    """Validate ownership, release delivery names and source paths for an enabled image."""
    errors = []
    owners = data.get('owners')
    if (
        not isinstance(owners, list)
        or not owners
        or any(not isinstance(owner, str) or not owner.strip() or '@' not in owner for owner in owners)
    ):
        errors.append('owners must contain at least one nonblank email address')
    if data.get('for_release') is not False:
        repos = data.get('delivery', {}).get('delivery_repo_names', [])
        if not isinstance(repos, list) or not repos or data.get('name') not in repos:
            errors.append('name must match an entry in delivery.delivery_repo_names')
        elif len(repos) != len(set(repos)):
            errors.append('delivery.delivery_repo_names must not contain duplicates')
    errors.extend(validate_source_paths(data))
    return errors


def relative_path(value: str, *, directory: bool = False) -> bool:
    """Accept repository-relative paths whose normalized form stays inside the repository."""
    if not isinstance(value, str) or not value.strip() or value.startswith('/') or '\x00' in value:
        return False
    normalized = posixpath.normpath(value)
    return normalized != '..' and not normalized.startswith('../') and (directory or normalized != '.')


def validate_source_paths(data: dict, *, image: bool = True) -> list[str]:
    """Check all metadata paths before source validation makes authenticated requests."""
    source = data.get('content', {}).get('source', {})
    paths = [('content.source.path', source.get('path'), True)]
    if image:
        paths.append(('content.source.dockerfile', source.get('dockerfile'), False))
    paths.append(('update-csv.manifests-dir', data.get('update-csv', {}).get('manifests-dir'), True))
    return [
        f'{name} must be a nonempty relative path inside the repository'
        for name, value, directory in paths
        if value is not None and not relative_path(value, directory=directory)
    ]


def validate_references(images: dict[str, dict], streams: dict, variables: dict | None = None) -> list[str]:
    """Check member and stream references and detect cycles across the active image graph."""
    errors = []
    edges = {}

    def resolve(value):
        """Expand configured group variables in a metadata reference."""
        return value.format_map(variables) if variables is not None else value

    stream_names = set()
    for name, stream in streams.items():
        try:
            if not isinstance(name, str):
                errors.append('streams: names must be strings')
                continue
            stream_names.add(resolve(name))
            if isinstance(stream, dict):
                aliases = stream.get('aliases', [])
                if not isinstance(aliases, list) or any(not isinstance(alias, str) for alias in aliases):
                    errors.append(f'{name}: stream aliases must be a list of strings')
                else:
                    stream_names.update(resolve(alias) for alias in aliases)
        except KeyError as error:
            errors.append(f'streams: undefined group variable {error.args[0]}')
    for name, data in images.items():
        if data.get('mode') in {'wip', 'disabled'}:
            continue
        parent = data.get('from') or {}
        if not isinstance(parent, dict) or not isinstance(parent.get('builder', []), list):
            errors.append(f'{name}: from must be a mapping with a list of builders')
            continue
        references = [parent, *parent.get('builder', [])]
        edges[name] = []
        for reference in references:
            if not isinstance(reference, dict):
                errors.append(f'{name}: from.builder entries must be mappings')
                continue
            if member := reference.get('member'):
                if member not in images or images[member].get('mode') == 'disabled':
                    errors.append(f'{name}: from.member references absent or disabled image {member}')
                else:
                    edges[name].append(member)
            if stream := reference.get('stream'):
                if stream not in stream_names:
                    errors.append(f'{name}: from.stream references unknown stream {stream}')
        for dependent in data.get('dependents', []):
            if dependent not in images:
                errors.append(f'{name}: dependents references absent image {dependent}')
    try:
        tuple(TopologicalSorter(edges).static_order())
    except CycleError as error:
        errors.append('image build dependency cycle: ' + ' -> '.join(error.args[1]))
    return errors


def repository_url(url: str) -> str:
    """Normalize supported SSH and HTTPS repository URLs for mapping comparisons."""
    url = url.removesuffix('.git').rstrip('/')
    return url.replace('git@github.com:', 'https://github.com/')


def repository_name(url: str) -> str:
    """Extract a GitHub owner/repository pair without credentials or ambiguous URL segments."""
    match = re.fullmatch(r'https://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)', repository_url(url))
    if not match or any(part in {'.', '..'} for part in match[1].split('/')):
        raise ValueError('source repositories must use a GitHub URL without credentials or query parameters')
    return match[1]


def source_repositories(data: dict, group: dict) -> list[tuple[str, str | None]]:
    """Return private and public sources using the most specific configured upstream mapping."""
    source = data.get('content', {}).get('source', {}).get('git', {})
    if not source.get('url'):
        return []
    private = repository_url(source['url'])
    repositories = [(repository_name(private), None)]
    # Match complete path boundaries; repo-level overrides take priority over org mappings.
    mappings = sorted(group.get('public_upstreams', []), key=lambda m: len(m['private']), reverse=True)
    for mapping in mappings:
        prefix = repository_url(mapping['private'])
        if private == prefix or private.startswith(prefix + '/'):
            public = repository_url(mapping['public'])
            suffix = private[len(prefix) :]
            org = public.rsplit('/', 1)[-1]
            if org != 'openshift' and suffix.startswith(f'/{org}-'):
                suffix = '/' + suffix.removeprefix(f'/{org}-')
            counterpart = (repository_name(public + suffix), mapping.get('public_branch'))
            if counterpart[0] != repositories[0][0]:
                repositories.append(counterpart)
            break
    return repositories


class GitHubClient:
    """Read-only GitHub API client. Never send credentials to metadata-supplied hosts."""

    def __init__(self, session=None, token: str | None = None):
        """Configure bounded retries and optional authentication for GitHub API requests."""
        self.session = session or requests.Session()
        self.session.headers.update({'Accept': 'application/vnd.github+json'})
        if token:
            self.session.headers.update({'Authorization': f'Bearer {token}'})
        retries = Retry(total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
        self.session.mount('https://api.github.com/', HTTPAdapter(max_retries=retries))
        self.cache = {}

    def exists(self, repository: str, path: str | None, ref: str, *, kind: str | None = None) -> bool:
        """Check a branch or typed contents entry, caching successful authoritative responses."""
        repository = repository_name(f'https://github.com/{repository}')
        if path is not None:
            if not relative_path(path, directory=kind == 'dir'):
                raise ValueError('GitHub contents path must stay inside the repository')
            path = posixpath.normpath(path)
        key = (repository, path, ref, kind)
        if key not in self.cache:
            endpoint = f'https://api.github.com/repos/{repository}/'
            endpoint += (
                'contents/' + quote(path, safe='/') if path is not None else 'git/ref/heads/' + quote(ref, safe='')
            )
            try:
                response = self.session.get(
                    endpoint, params={'ref': ref} if path is not None else {}, timeout=(10, 30), allow_redirects=False
                )
            except requests.RequestException:
                raise RemoteError(f'GitHub unavailable while checking {repository}; retry the job') from None
            if response.status_code not in (200, 404):
                raise RemoteError(
                    f'GitHub returned HTTP {response.status_code} for {repository}; check access/rate limits'
                )
            found = response.status_code == 200
            if found and kind is not None:
                try:
                    content = response.json()
                except requests.exceptions.JSONDecodeError:
                    raise RemoteError(f'GitHub returned an invalid response for {repository}; retry the job') from None
                found = (kind == 'dir' and isinstance(content, list)) or (
                    isinstance(content, dict) and content.get('type') == kind
                )
            self.cache[key] = found
        return self.cache[key]


def validate_sources(data: dict, group: dict, client: GitHubClient, image: bool = True) -> list[str]:
    """Check source refs, Dockerfiles and manifests in private and configured public repositories."""
    if errors := validate_source_paths(data, image=image):
        return errors
    source = data.get('content', {}).get('source', {})
    git = source.get('git', {})
    if not git.get('url'):
        return []
    branches = git.get('branch', {})
    variables = group.get('vars', {})

    def resolve(value):
        """Expand configured group variables in a metadata reference."""
        if not isinstance(value, str) or not value.strip():
            return None
        return value.format_map(variables)

    try:
        target, fallback = resolve(branches.get('target')), resolve(branches.get('fallback'))
        if not target and not fallback:
            return ['content.source.git.branch must specify target or fallback']
        errors = []
        for repository, public_branch in source_repositories(data, group):
            candidates = [resolve(public_branch)] if public_branch else [target, fallback]
            branch = next((ref for ref in candidates if ref and client.exists(repository, None, ref)), None)
            if not branch:
                errors.append(
                    f'{repository}: no configured branch exists ({", ".join(filter(None, candidates))}); '
                    'private repositories require a read-only GitHub token'
                )
                continue
            paths = []
            if image:
                paths.append((source.get('dockerfile', 'Dockerfile'), 'file'))
            if manifests := data.get('update-csv', {}).get('manifests-dir'):
                paths.append((manifests, 'dir'))
            for path, kind in paths:
                path = posixpath.join(source.get('path', ''), path)
                if not client.exists(repository, path, branch, kind=kind):
                    errors.append(f'{repository}: {path} not found on branch {branch}')
        return errors
    except (KeyError, ValueError) as error:
        return [f'source validation: {error}']


def component_name(application: str, image: str, data: dict, *, bundle: bool = False) -> str:
    """Match Doozer component naming, including explicit image and bundle overrides."""
    override_key = 'bundle_name_override' if bundle else 'name_override'
    override = data.get('konflux', {}).get('component', {}).get(override_key)
    if override:
        return override
    if bundle:
        image = data.get('bundle_name_override') or f'{image}-bundle'
    name = f'{application}-{image}'.replace('.', '-').replace('_', '-')
    return f'ose-{name[10:]}' if name.startswith('openshift-') else name


def validate_admissions(images: dict[str, dict], root: Path, application: str) -> list[str]:
    """Reuse OCP stage/prod repo, bundle and component presence checks for any application."""
    images = {
        name: data
        for name, data in images.items()
        if data.get('mode') not in {'disabled', 'wip'} and data.get('for_release') is not False
    }
    if not images:
        return []
    admissions = {'stage': [], 'prod': []}
    errors = []
    for path in sorted(root.rglob('*.yaml')):
        try:
            rpa = load_mapping(path)
            if rpa.get('kind') != 'ReleasePlanAdmission' or application not in rpa.get('spec', {}).get(
                'applications', []
            ):
                continue
            data = rpa['spec'].get('data', {})
            intention = data.get('intention')
            environment = {'staging': 'stage', 'stage': 'stage', 'production': 'prod', 'prod': 'prod'}.get(intention)
            # Older OCP admissions use conventional names rather than intention.
            if not environment:
                name = rpa.get('metadata', {}).get('name', '')
                environment = (
                    'stage'
                    if re.search(r'-(?:stage|staging)(?:-|$)', name)
                    else ('prod' if re.search(r'-prod(?:-|$)', name) else None)
                )
            if environment:
                components = data.get('mapping', {}).get('components')
                if not isinstance(components, list) or not components:
                    errors.append(f'{path.name}: missing RPA mapping.components')
                elif any(
                    not isinstance(component, dict)
                    or not isinstance(component.get('name'), str)
                    or not isinstance(component.get('repositories'), list)
                    or any(
                        not isinstance(repo, dict) or not isinstance(repo.get('url'), str)
                        for repo in component.get('repositories', [])
                    )
                    for component in components
                ):
                    errors.append(f'{path.name}: invalid RPA component/repository mapping')
                else:
                    admissions[environment].extend(components)
        except (ValueError, TypeError, KeyError, AttributeError, YAMLError) as error:
            errors.append(f'{path.name}: invalid admission: {error}')
    for environment, components in admissions.items():
        if not components:
            errors.append(f'{environment}: no usable RPA mapping for application {application}')
            continue
        repos = {
            repo.get('url', '').partition('/')[2]
            for component in components
            for repo in component.get('repositories', [])
        }
        names = {component.get('name') for component in components}
        for name, data in images.items():
            if data.get('mode') == 'disabled' or data.get('for_release') is False:
                continue
            delivery = data.get('delivery', {})
            component = component_name(application, name, data)
            component_repos = {
                repo['url'].partition('/')[2]
                for entry in components
                if entry.get('name') == component
                for repo in entry.get('repositories', [])
            }
            for repository in delivery.get('delivery_repo_names', []):
                if component in names and repository not in component_repos:
                    errors.append(f'{name}: {environment} component {component} does not deliver to {repository}')
            expected = list(delivery.get('delivery_repo_names', []))
            if bundle := delivery.get('bundle_delivery_repo_name'):
                expected.append(bundle)
                bundle_component = component_name(application, name, data, bundle=True)
                bundle_repos = {
                    repo['url'].partition('/')[2]
                    for entry in components
                    if entry.get('name') == bundle_component
                    for repo in entry.get('repositories', [])
                }
                if bundle_component not in names:
                    errors.append(f'{name}: {environment} RPA missing bundle component {bundle_component}')
                elif bundle not in bundle_repos:
                    errors.append(
                        f'{name}: {environment} bundle component {bundle_component} does not deliver to {bundle}'
                    )
            for repository in expected:
                if repository not in repos:
                    errors.append(f'{name}: {environment} RPA missing delivery repository {repository}')
            component = component_name(application, name, data)
            if component not in names:
                errors.append(f'{name}: {environment} RPA missing component {component}')
    return errors
