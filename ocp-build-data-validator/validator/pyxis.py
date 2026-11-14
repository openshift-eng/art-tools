"""Check product content streams against a local pyxis-repo-configs checkout."""

from pathlib import Path

from artcommonlib.util import product_version_from_group_name

from .checks import load_mapping


def expected_stream(group: dict) -> str:
    """Use the product group, not an upstream operator's independent build version."""
    try:
        name = group['name'].format(**group.get('vars', {}))
        version = product_version_from_group_name(name)
    except (KeyError, IndexError, AttributeError, TypeError, ValueError) as error:
        raise ValueError('Cannot resolve group.yml.name for the Pyxis product stream') from error
    if version is None:
        raise ValueError('group.yml.name must end in MAJOR.MINOR for the Pyxis product stream')
    return f'v{version[0]}.{version[1]}'


def validate_content_streams(images: dict[str, dict], group: dict, root: Path) -> list[str]:
    """Require the product stream in every image and bundle delivery repository entry."""
    repositories = set()
    for data in images.values():
        if data.get('mode') in {'disabled', 'wip'} or data.get('for_release') is False:
            continue
        delivery = data.get('delivery', {})
        repositories.update(delivery.get('delivery_repo_names', []))
        if bundle := delivery.get('bundle_delivery_repo_name'):
            repositories.add(bundle)
    if not repositories:
        return []
    stream = expected_stream(group)
    products = root / 'products'
    if not products.is_dir():
        raise ValueError(f'Pyxis products directory does not exist: {products}')
    found = set()
    errors = []
    paths = sorted(set(products.rglob('*.yaml')) | set(products.rglob('*.yml')))
    for path in paths:
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError(f'Pyxis configuration symlink escapes the checkout: {path}')
        entries = load_mapping(path).get('repositories', [])
        if not isinstance(entries, list):
            raise ValueError(f'{path}: Pyxis repositories must be a list')
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get('repository'), dict):
                raise ValueError(f'{path}: invalid Pyxis repository mapping')
            config = entry['repository']
            repository = config.get('repository')
            if not isinstance(repository, str):
                raise ValueError(f'{path}: Pyxis repository name must be a string')
            if repository not in repositories:
                continue
            found.add(repository)
            tags = config.get('content_stream_tags')
            if (
                not isinstance(tags, list)
                or any(not isinstance(tag, str) or not tag.strip() for tag in tags)
                or stream not in tags
            ):
                errors.append(
                    f'Pyxis repository {repository}: expected content_stream_tags to include {stream}; update {path}'
                )
    for repository in sorted(repositories - found):
        errors.append(
            f'Pyxis repository {repository}: no configuration found for expected stream {stream}; '
            f'add its repository configuration under {products}'
        )
    return errors
