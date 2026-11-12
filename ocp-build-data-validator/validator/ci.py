"""Shared Prow entry point: python -m validator.ci --repo <PR checkout>."""

import argparse
import json
import os
import re
import subprocess
from pathlib import Path

from artcommonlib.util import product_version_from_group_name
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError
from semver import Version

from . import checks, releases, schema

ROOT_FILES = {'group.yml', 'streams.yml', 'releases.yml', 'erratatool.yml', 'bug.yml', 'bugzilla.yml'}


def metadata_file(path: Path) -> bool:
    """Recognize supported root and component metadata files relative to the checkout."""
    return str(path) in ROOT_FILES or (
        path.parts[0] in {'images', 'rpms', 'repos'} and path.suffix in {'.yml', '.yaml'}
    )


def all_files(root: Path) -> list[Path]:
    """List all supported metadata files in deterministic order."""
    candidates = [root / name for name in ROOT_FILES]
    for directory in ('images', 'rpms', 'repos'):
        candidates.extend((root / directory).rglob('*'))
    return sorted(
        path.relative_to(root) for path in candidates if path.is_file() and metadata_file(path.relative_to(root))
    )


def resolve_commit(root: Path, base: str) -> str:
    """Resolve a single revision before passing it to other Git commands."""
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._/~^@{}-]*', base):
        raise ValueError('--base-ref must be a single git commit revision')
    return (
        subprocess.run(
            ['git', '-C', str(root), 'rev-parse', '--verify', '--end-of-options', f'{base}^{{commit}}'],
            capture_output=True,
            check=True,
        )
        .stdout.decode()
        .strip()
    )


def select_files(root: Path, base: str | None = None) -> list[Path]:
    """Select changed metadata against a single commit, expanding shared edits to a full pass."""
    if not base:
        return all_files(root)
    revision = resolve_commit(root, base)
    result = subprocess.run(
        ['git', '-C', str(root), 'diff', '--name-only', '-z', revision, 'HEAD', '--'], capture_output=True, check=True
    )
    changed = [Path(path) for path in result.stdout.decode().split('\0') if path and metadata_file(Path(path))]
    if any(
        str(path) in {'group.yml', 'streams.yml'} or path.parts[0] == 'repos' or not (root / path).is_file()
        for path in changed
    ):
        return all_files(root)
    return sorted(changed)


def read_metadata(root: Path, relative: Path) -> dict:
    """Load a metadata mapping while preventing symlinks from escaping the checkout."""
    path = root / relative
    if not path.resolve().is_relative_to(root):
        raise ValueError('metadata symlink escapes the checkout')
    if relative == Path('releases.yml'):
        # A placeholder is common before an LP branch has any named assemblies.
        with path.open() as source:
            if YAML(typ='safe').load(source) is None:
                return {'releases': {}}
    return checks.load_mapping(path)


def prow_base_ref() -> str | None:
    """Find the metadata PR target, ignoring the release PR used for rehearsals."""
    job = json.loads(os.environ.get('JOB_SPEC') or '{}')
    for refs in [job.get('refs'), *job.get('extra_refs', [])]:
        if refs and refs.get('org') == 'openshift-eng' and refs.get('repo') == 'ocp-build-data' and refs.get('pulls'):
            base = refs.get('base_sha')
            if not isinstance(base, str) or not re.fullmatch(r'[0-9a-fA-F]{40,64}', base):
                raise ValueError('Metadata PR JOB_SPEC must contain a valid base_sha for version comparison')
            return base
    return None


def version_fields(group: dict) -> tuple[object, object]:
    """Resolve the version and auto group name using this revision's variables."""
    variables = group.get('vars') or {}
    if not isinstance(variables, dict):
        raise ValueError('group.yml.vars must be a mapping for version comparison')

    def resolve(value):
        if not variables or not isinstance(value, str):
            return value
        try:
            return value.format(**variables)
        except (KeyError, IndexError, AttributeError, ValueError) as error:
            raise ValueError(f'Could not resolve group.yml version variables: {error}') from error

    version = resolve(group.get('version'))
    return version, resolve(group.get('name')) if version == 'auto' else None


def effective_version(version: object, group_name: object) -> Version:
    """Interpret auto as MAJOR.MINOR.0, as the product build pipeline does."""
    value = version
    if version == 'auto':
        if not isinstance(group_name, str):
            raise ValueError("Cannot resolve version 'auto': group name must be a string")
        product_version = product_version_from_group_name(group_name)
        if product_version is None:
            raise ValueError("Cannot resolve version 'auto': group name must end in MAJOR.MINOR")
        value = f'{product_version[0]}.{product_version[1]}.0'
    if not isinstance(value, str):
        raise ValueError('group.yml.version must be a version string when comparing against an existing version')
    if re.fullmatch(r'[0-9]+\.[0-9]+', value):
        value += '.0'
    return Version.parse(value)


def validate_version_change(root: Path, base: str) -> list[str]:
    """Reject decreases against the target branch without consulting release trackers."""
    revision = resolve_commit(root, base)
    exists = subprocess.run(
        ['git', '-C', str(root), 'ls-tree', '--name-only', revision, '--', 'group.yml'],
        capture_output=True,
        check=True,
    )
    if not exists.stdout:
        return []  # Initial metadata on a previously empty branch.
    previous = YAML(typ='safe').load(
        subprocess.run(
            ['git', '-C', str(root), 'show', f'{revision}:group.yml'], capture_output=True, check=True
        ).stdout.decode()
    )
    if not isinstance(previous, dict):
        raise ValueError('Target branch group.yml must contain a mapping for version comparison')
    current = read_metadata(root, Path('group.yml'))
    if 'version' not in previous:
        return []
    previous_fields = version_fields(previous)
    current_fields = version_fields(current)
    if previous_fields == current_fields:
        return []
    if effective_version(*current_fields) < effective_version(*previous_fields):
        return [f'group.yml: version must not decrease ({previous_fields[0]} -> {current_fields[0]})']
    return []


def validate(
    root: Path,
    selected: list[Path],
    schema_only: bool,
    token: str | None,
    rpa_root: Path | None,
    application: str | None,
    registry_auth_file: Path | None = None,
) -> list[str]:
    """Collect file and repository errors while keeping WIP findings nonblocking."""
    errors = []
    if not selected:
        return errors
    group = read_metadata(root, Path('group.yml'))
    streams = read_metadata(root, Path('streams.yml')) if (root / 'streams.yml').exists() else {}
    images = {}
    # A whole-repository reference graph catches references broken by edits/deletions.
    for relative in all_files(root):
        if relative.parts[0] != 'images':
            continue
        try:
            image = read_metadata(root, relative)
            if image.get('mode') != 'disabled':
                if relative.stem in images:
                    errors.append(f'{relative}: duplicate image metadata key {relative.stem}')
                images[relative.stem] = image
        except (ValueError, OSError, YAMLError) as error:
            errors.append(f'{relative}: {error}')
    client = checks.GitHubClient(token=token)
    valid_images = {}
    for relative in selected:
        data = None
        file_errors = []
        try:
            if not (root / relative).resolve().is_relative_to(root):
                raise ValueError('metadata symlink escapes the checkout')
            if relative.parts[0] == 'repos':
                # Repo files have a list schema; image_schema checks enabled_repos against them.
                with (root / relative).open() as source:
                    repos = YAML(typ='safe').load(source)
                if (
                    not isinstance(repos, list)
                    or not repos
                    or any(
                        not isinstance(repo, dict) or not isinstance(repo.get('name'), str) or not repo['name'].strip()
                        for repo in repos
                    )
                ):
                    raise ValueError('repo metadata must be a nonempty list of named repositories')
                continue
            data = read_metadata(root, relative)
            if data.get('mode') == 'disabled':
                continue
            error = schema.validate(str(relative), data, str(root / 'images'))
            if error:
                raise ValueError(error)
            if relative == Path('releases.yml'):
                if semantic_error := releases.validate(data, group_config=group):
                    file_errors.append(semantic_error)
            if relative.parts[0] == 'images':
                semantic_errors = checks.validate_image(data)
                file_errors.extend(semantic_errors)
                if not semantic_errors:
                    valid_images[relative.stem] = data
            if not schema_only and relative.parts[0] in {'images', 'rpms'}:
                file_errors.extend(checks.validate_sources(data, group, client, image=relative.parts[0] == 'images'))
        except (ValueError, KeyError, TypeError, OSError, YAMLError) as error:
            file_errors.append(str(error))
        if isinstance(data, dict) and data.get('mode') == 'wip':
            for error in file_errors:
                print(f'WARNING: {relative} (wip): {error}')
        else:
            errors.extend(f'{relative}: {error}' for error in file_errors)
    try:
        errors.extend(checks.validate_references(images, streams, variables=group.get('vars', {})))
        if not schema_only:
            errors.extend(checks.validate_fbc_catalogs(group, registry_auth_file))
        if rpa_root:
            if not application:
                raise ValueError('--application is required with --rpa-root')
            if not rpa_root.is_dir():
                raise ValueError('RPA directory does not exist')
            errors.extend(checks.validate_admissions(valid_images, rpa_root, application))
    except (ValueError, TypeError, KeyError, YAMLError) as error:
        errors.append(f'repository validation: {error}')
    return errors


def main(argv=None) -> int:
    """Run validation and write the JSON artifact for both success and handled failures."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument(
        '--base-ref', help='Base commit to compare with the Prow merge checkout; omit to validate all files'
    )
    parser.add_argument(
        '--schema-only', action='store_true', help='Run local checks without querying source repositories'
    )
    parser.add_argument(
        '--github-token-file', type=Path, help='File containing a read-only token for private GitHub sources'
    )
    parser.add_argument('--rpa-root', type=Path, help='Local konflux-release-data ReleasePlanAdmission directory')
    parser.add_argument('--application', help='Exact Konflux application whose stage/prod RPAs should be checked')
    parser.add_argument(
        '--registry-auth-file', type=Path, help='Docker/Podman pull-auth file for production FBC catalogs'
    )
    parser.add_argument('--report', type=Path, help='Write a machine-readable JSON validation artifact')
    args = parser.parse_args(argv)
    root = args.repo.resolve()
    selected = []
    try:
        if not root.is_dir() or not (root / 'group.yml').is_file():
            raise ValueError('Expected a build-data release branch containing group.yml; main is not a release branch')
        token = args.github_token_file.read_text().strip() if args.github_token_file else None
        if args.github_token_file and not token:
            raise ValueError('GitHub token file is empty')
        if args.registry_auth_file and not args.registry_auth_file.is_file():
            raise ValueError('Registry auth file does not exist')
        selected = select_files(root, args.base_ref)
        errors = validate(
            root, selected, args.schema_only, token, args.rpa_root, args.application, args.registry_auth_file
        )
        base = args.base_ref or prow_base_ref()
        if base:
            errors.extend(validate_version_change(root, base))
    except subprocess.CalledProcessError:
        errors = ['Could not compare the requested base revision; ensure the Prow checkout contains its history']
    except (ValueError, OSError, YAMLError) as error:
        errors = [str(error)]
    errors = list(dict.fromkeys(errors))
    for error in errors:
        print(f'ERROR: {error}')
    print(f'Validated {len(selected)} metadata files; {len(errors)} error(s)')
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps({'files': [str(path) for path in selected], 'errors': errors}, indent=2) + '\n'
        )
    return 1 if errors else 0


if __name__ == '__main__':
    raise SystemExit(main())
