"""Persist and read assembly-pinned inputs for unified layered-product catalogs."""

import re
from io import StringIO

from artcommonlib.assembly import assembly_image_pins
from artcommonlib.model import Model
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import extract_ocp_version_from_fbc_nvr, new_roundtrip_yaml_handler

from pyartcd.fbc_util import validate_layered_product_fbc_nvrs
from pyartcd.git import GitRepository


def assembly_fbc_records(assembly: str, nvrs: list[str], pullspecs: list[str], operand_nvrs: list[str]) -> dict:
    """Index a complete, validated build result by OCP target and operator."""
    if not nvrs or len(nvrs) != len(pullspecs) or not operand_nvrs:
        raise ValueError('Recording FBCs requires complete NVRs, pullspecs, and operand pins')
    validate_layered_product_fbc_nvrs(assembly, nvrs)
    records = {}
    for nvr, pullspec in zip(nvrs, pullspecs):
        target = extract_ocp_version_from_fbc_nvr(nvr)
        name = parse_nvr(nvr)['name']
        if not target or not name.endswith('-fbc'):
            raise ValueError(f'Cannot identify FBC target and operator for {nvr}')
        if not re.fullmatch(r'[^\s@]+@sha256:[0-9a-f]{64}', pullspec):
            raise ValueError(f'FBC {nvr} requires an immutable pullspec')
        operator = name.removesuffix('-fbc')
        entries = records.setdefault(target, {})
        if operator in entries:
            raise ValueError(f'Duplicate FBC for {operator} on OCP {target}')
        entries[operator] = {'nvr': nvr, 'pullspec': pullspec, 'operand_nvrs': sorted(set(operand_nvrs))}
    if len({tuple(sorted(entries)) for entries in records.values()}) != 1:
        raise ValueError('FBC targets contain different operator sets')
    return records


def recorded_assembly_fbcs(releases: dict, assembly: str, target: str) -> dict:
    """Read only this assembly's records and reject records predating its current pins."""
    definition = releases.get('releases', {}).get(assembly)
    if not isinstance(definition, dict):
        raise ValueError(f'Assembly {assembly} is missing from releases.yml')
    entries = definition.get('assembly', {}).get('group', {}).get('fbc_builds', {}).get(target)
    if not isinstance(entries, dict) or not entries:
        raise ValueError(
            f'Assembly {assembly} has no recorded FBCs for OCP {target}; run prepare-release-lp --unified-fbc'
        )
    pins = sorted(set(assembly_image_pins(Model(releases), assembly).values()))
    nvrs, pullspecs = [], []
    for operator, record in entries.items():
        if not isinstance(record, dict) or record.get('operand_nvrs') != pins:
            raise ValueError(f'Assembly {assembly} has stale or incomplete FBC pins for {operator}')
        nvr = record.get('nvr')
        pullspec = record.get('pullspec')
        if not isinstance(nvr, str) or not isinstance(pullspec, str):
            raise ValueError(f'Assembly {assembly} has an invalid FBC record for {operator}')
        if parse_nvr(nvr)['name'] != f'{operator}-fbc' or extract_ocp_version_from_fbc_nvr(nvr) != target:
            raise ValueError(f'Assembly {assembly} has an FBC for the wrong operator or target: {nvr}')
        nvrs.append(nvr)
        pullspecs.append(pullspec)
    return assembly_fbc_records(assembly, nvrs, pullspecs, pins)[target]


async def update_assembly_fbcs(repo: GitRepository, group: str, assembly: str, records: dict, expected: dict) -> bool:
    """Refetch before editing and preserve unrelated changes; fail on concurrent pin or FBC changes."""
    await repo.fetch_switch_branch(group, remote='origin')
    yaml = new_roundtrip_yaml_handler()
    releases = yaml.load(await repo.read_file('releases.yml')) or {}
    current = releases.get('releases', {}).get(assembly)
    previous = expected.get('releases', {}).get(assembly)
    if current is None or previous is None:
        raise ValueError(f'Assembly {assembly} must already exist before recording FBCs')
    if assembly_image_pins(Model(releases), assembly) != assembly_image_pins(Model(expected), assembly):
        raise RuntimeError(f'Assembly {assembly} operand pins changed while building FBCs; rerun preparation')
    old_records = previous.get('assembly', {}).get('group', {}).get('fbc_builds')
    config = current.setdefault('assembly', {}).setdefault('group', {})
    if config.get('fbc_builds') == records:
        return False
    if config.get('fbc_builds') != old_records:
        raise RuntimeError(f'Assembly {assembly} FBC records changed concurrently; rerun preparation')
    config['fbc_builds'] = records
    output = StringIO()
    yaml.dump(releases, output)
    await repo.write_file('releases.yml', output.getvalue())
    return await repo.commit_push(f'Record FBC builds for {group} assembly {assembly}', safe=True)
