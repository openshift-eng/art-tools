"""Constraints for build stages that preserve their inherited RPM versions."""

import re
import shlex
from pathlib import Path

from doozerlib.image_rpms import InstalledRpm
from doozerlib.lockfile_prototype.models import RepoEntry
from doozerlib.repodata import Rpm

RpmInventory = dict[str, tuple[InstalledRpm, ...]]


def stage_references(entries: list[dict]) -> dict[int, int]:
    """Map FROM aliases (including numeric references) to earlier stages."""
    aliases: dict[str, int] = {}
    references = {}
    stage = -1
    for entry in entries:
        if entry['instruction'] != 'FROM':
            continue
        stage += 1
        tokens = [t for t in shlex.split(entry['value']) if not t.startswith('--')]
        if tokens[0].lower() in aliases:
            references[stage] = aliases[tokens[0].lower()]
        aliases[str(stage)] = stage
        if len(tokens) >= 3 and tokens[-2].lower() == 'as':
            aliases[tokens[-1].lower()] = stage
    return references


def stage_commands(source_dir: Path, entries: list[dict]) -> dict[int, str]:
    """Read RUN commands and referenced local scripts, including COPY renames.

    This recognizes literal wildcard versionlocking, not arbitrary shell
    execution. Build context scripts are only read, never executed.
    """
    result: dict[int, list[str]] = {}
    copies: dict[str, Path] = {}
    stage = -1
    root = source_dir.resolve()
    script_pattern = re.compile(r'(?<![\w.-])(/?[\w./-]+\.sh)\b')
    for entry in entries:
        instruction, value = entry['instruction'], entry['value']
        if instruction == 'FROM':
            stage += 1
            copies = {}
            result[stage] = []
        elif instruction == 'COPY' and '--from=' not in value:
            tokens = shlex.split(value)
            if len(tokens) == 2 and tokens[0].endswith('.sh'):
                copies[tokens[1]] = root / tokens[0]
        elif instruction == 'RUN':
            result[stage].append(value)
            mounts = {}
            for mount in re.findall(r'--mount=([^\s]+)', value):
                options = dict(part.split('=', 1) for part in mount.split(',') if '=' in part)
                target = options.get('target') or options.get('dst') or options.get('destination')
                if options.get('type', 'bind') == 'bind' and 'from' not in options and target:
                    mounts[target.rstrip('/')] = root / options.get('source', options.get('src', '.'))
            for script in script_pattern.findall(value):
                candidates = [copies.get(script)]
                if not script.startswith('/'):
                    candidates.append(root / script)
                for target, source in mounts.items():
                    if script.startswith(target + '/'):
                        candidates.append(source / script[len(target) + 1 :])
                for candidate in candidates:
                    if candidate is None:
                        continue
                    candidate = candidate.resolve()
                    if candidate.is_relative_to(root) and candidate.is_file():
                        result[stage].append(candidate.read_text())
                        break
    return {stage: '\n'.join(commands) for stage, commands in result.items()}


def locks_inherited_rpms(commands: str) -> bool:
    """Recognize a literal dnf/yum versionlock add '*' command."""
    commands = commands.replace('\\\n', ' ')
    return bool(
        re.search(
            r'''(?:^|[;\n]|&&)\s*(?:dnf|yum)\s+[^\n;]*?\bversionlock\s+add\s+(?:'\*'|"\*"|\*)(?=\s|$|;)''',
            commands,
        )
    )


def changes_rpms(commands: str) -> bool:
    """Detect package mutations for which inherited versions cannot be assumed."""
    return bool(
        re.search(
            r'\b(?:dnf|yum|rpm-ostree)\b[^\n;]*\b(?:install|update|upgrade|distro-sync|remove|erase|swap)\b', commands
        )
    )


def preserved_versions(inventory: RpmInventory) -> dict[str, str]:
    """Include versions inherited on other arches, rejecting conflicting anchors."""
    versions: dict[str, dict[str, set[str]]] = {}
    for arch, packages in inventory.items():
        for package in packages:
            versions.setdefault(package.name, {}).setdefault(arch, set()).add(package.evr)
    result = {}
    for name, per_arch in versions.items():
        evrs = set().union(*per_arch.values())
        if len(evrs) != 1:
            raise ValueError(f'Inherited RPM versions disagree for {name}: {per_arch}')
        result[name] = evrs.pop()
    return result


def constrain_repo(repo: RepoEntry, packages: list[Rpm], versions: dict[str, str]) -> RepoEntry:
    """Exclude incompatible NEVRAs without installing absent inherited packages.

    The existing Repo repodata cache supplies the candidates. Exact NEVRAs keep
    epoch and architecture distinctions and leave unrelated packages untouched.
    """
    excluded = sorted(
        {
            package.nevra
            for package in packages
            if package.name in versions
            and f'{package.epoch}:{package.version}-{package.release}' != versions[package.name]
        }
    )
    constrained = repo.model_copy(deep=True)
    if excluded:
        previous = [
            str(constrained.options.pop(key)) for key in ('exclude', 'excludepkgs') if key in constrained.options
        ]
        constrained.options['excludepkgs'] = ' '.join(previous + excluded)
    return constrained
