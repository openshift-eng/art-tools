"""Shared Prow entry point: python -m validator.ci --repo <PR checkout>."""

import argparse
import json
import subprocess
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from . import checks, schema

ROOT_FILES = {'group.yml', 'streams.yml', 'releases.yml', 'erratatool.yml', 'bug.yml', 'bugzilla.yml'}


def metadata_file(path: Path) -> bool:
    return str(path) in ROOT_FILES or (
        path.parts[0] in {'images', 'rpms', 'repos'} and path.suffix in {'.yml', '.yaml'}
    )


def all_files(root: Path) -> list[Path]:
    candidates = [root / name for name in ROOT_FILES]
    for directory in ('images', 'rpms', 'repos'):
        candidates.extend((root / directory).rglob('*'))
    return sorted(
        path.relative_to(root) for path in candidates if path.is_file() and metadata_file(path.relative_to(root))
    )


def select_files(root: Path, base: str | None = None) -> list[Path]:
    if not base:
        return all_files(root)
    result = subprocess.run(
        ['git', '-C', str(root), 'diff', '--name-only', '-z', base, 'HEAD', '--'], capture_output=True, check=True
    )
    changed = [Path(path) for path in result.stdout.decode().split('\0') if path and metadata_file(Path(path))]
    if any(
        str(path) in {'group.yml', 'streams.yml'} or path.parts[0] == 'repos' or not (root / path).is_file()
        for path in changed
    ):
        return all_files(root)
    return sorted(changed)


def read_metadata(root: Path, relative: Path) -> dict:
    path = root / relative
    if not path.resolve().is_relative_to(root):
        raise ValueError('metadata symlink escapes the checkout')
    return checks.load_mapping(path)


def validate(
    root: Path,
    selected: list[Path],
    schema_only: bool,
    token: str | None,
    rpa_root: Path | None,
    application: str | None,
) -> list[str]:
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
        selected = select_files(root, args.base_ref)
        errors = validate(root, selected, args.schema_only, token, args.rpa_root, args.application)
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
