"""Compare the Go toolchains selected for image builds and upstream CI."""

import re
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError


def _read_yaml(path):
    if not path.exists():
        return {}
    with path.open() as source:
        data = YAML(typ='safe').load(source) or {}
    if not isinstance(data, dict):
        raise ValueError(f'{path} must contain a YAML mapping')
    return data


class _GoVersionResolver:
    def __init__(self, images_dir):
        self.images_dir = Path(images_dir)
        self.variables = _read_yaml(self.images_dir.parent / 'group.yml').get('vars', {})
        self.streams = _read_yaml(self.images_dir.parent / 'streams.yml')
        self.members = {path.stem: path for path in self.images_dir.glob('*.yml')}

    def _expand(self, value):
        return re.sub(r'\{(\w+)\}', lambda match: str(self.variables.get(match[1], match[0])), value)

    def _version(self, value):
        if not value:
            return None
        value = self._expand(value)
        if re.search(r'\{GO_\w+\}', value):
            raise ValueError(f'Cannot resolve Go version in {value!r}: missing group variable')
        match = re.search(r'golang(?:-builder(?:-container)?)?[-:]v?(\d+)\.(\d+)', value)
        return (int(match[1]), int(match[2])) if match else None

    def resolve(self, reference, upstream=False, seen=frozenset()):
        if 'stream' in reference:
            name = self._expand(reference['stream'])
            matches = [
                (key, config)
                for key, config in self.streams.items()
                if name == self._expand(key) or name in [self._expand(alias) for alias in config.get('aliases', [])]
            ]
            if len(matches) != 1:
                raise ValueError(f'Cannot resolve stream {name!r}: expected one definition, found {len(matches)}')
            key, config = matches[0]
            # CI uses upstream_image; downstream builds use the pinned image.
            image = config.get('upstream_image' if upstream else 'image')
            return self._version(image) or self._version(key) or self._version(config.get('upstream_image'))

        if 'image' in reference:
            return self._version(reference['image'])

        if 'member' not in reference:
            return None

        member = self._expand(reference['member'])
        if member in seen:
            raise ValueError(f'Cannot resolve member {member!r}: circular from.member reference')
        if member not in self.members:
            raise ValueError(f'Cannot resolve member {member!r}: image not found in {self.images_dir}')

        config = _read_yaml(self.members[member])
        alignment = config.get('content', {}).get('source', {}).get('ci_alignment', {})
        if upstream:
            upstream_version = self._version(alignment.get('upstream_image'))
            if upstream_version:
                return upstream_version

        # Follow the final base image, not build stages that may only produce binaries.
        return self.resolve(config.get('from', {}), upstream, seen | {member}) or self._version(
            alignment.get('upstream_image')
        )


def validate_go_versions(file, data, images_dir):
    alignment = data.get('content', {}).get('source', {}).get('ci_alignment', {})
    build_root = alignment.get('streams_prs', {}).get('ci_build_root')
    builders = data.get('from', {}).get('builder', [])
    if not build_root or not builders:
        return []

    image = Path(file).stem if file else data.get('name', '<unknown>')
    try:
        resolver = _GoVersionResolver(images_dir)
        builder_versions = {version for builder in builders if (version := resolver.resolve(builder))}
        if not builder_versions:
            return []
        root_version = resolver.resolve(build_root, upstream=True)
    except (ValueError, OSError, YAMLError) as error:
        return [f'{image}: cannot validate Go version consistency: {error}']

    # Metadata without a versioned Go image cannot establish its toolchain offline.
    if root_version is None:
        return []

    mismatches = sorted(builder_versions - {root_version})
    if not mismatches:
        return []

    versions = ', '.join(f'{major}.{minor}' for major, minor in mismatches)
    return [
        f'{image}: Go version mismatch: from.builder uses {versions}, '
        f'but content.source.ci_alignment.streams_prs.ci_build_root uses {root_version[0]}.{root_version[1]}'
    ]
