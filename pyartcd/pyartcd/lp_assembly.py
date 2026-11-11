"""Generate and persist assemblies for direct layered-product releases."""

from copy import deepcopy

from artcommonlib.assembly import assembly_image_pins
from artcommonlib.model import Model
from artcommonlib.util import new_roundtrip_yaml_handler

from pyartcd.git import GitRepository


def image_pin_members(pins: dict[str, str], parent: dict[str, str] | None = None) -> list[dict]:
    """Generate canonical image members, overriding only changed inherited pins.

    Args:
        pins: Selected image keys and exact NVRs.
        parent: Effective parent pins, or None for a standalone assembly.

    Returns:
        Sorted image members using the same inheritance rules in both LP paths.
    """
    return [
        {
            'distgit_key': key,
            'why': 'Pin this image build for the assembly',
            'metadata': {'is': {'nvr!' if parent is not None else 'nvr': nvr}},
        }
        for key, nvr in sorted(pins.items())
        if parent is None or parent.get(key) != nvr
    ]


def direct_release_assembly(releases: dict, assembly: str, pins: dict[str, str], fbcs: dict) -> dict:
    """Reconcile generated fields while retaining inheritance and explicit exclusions.

    Args:
        releases: Original releases.yml mapping.
        assembly: Release assembly to populate.
        pins: Product operator and operand pins from validated release inputs.
        fbcs: Source pullspecs grouped by OCP target.

    Returns:
        A complete assembly definition preserving unrelated configuration.

    Raises:
        ValueError: If the parent assembly chain is invalid.
    """
    definition = deepcopy(releases.get('releases', {}).get(assembly, {}).get('assembly', {}))
    basis = definition.setdefault('basis', {})
    parent_name = basis.get('assembly')
    parent = assembly_image_pins(Model(releases), parent_name) if parent_name else None
    old_members = definition.setdefault('members', {}).get('images', [])
    exclusions = [deepcopy(member) for member in old_members if 'exclude' in member]
    for member in exclusions:
        # An explicit re-inclusion must not retain an old child NVR when the
        # current input now matches the parent and needs no generated override.
        if member.get('exclude') is False and member['distgit_key'] in pins:
            member.get('metadata', {}).pop('is', None)
    excluded_keys = {member['distgit_key'] for member in exclusions if member.get('exclude') is True}
    generated = image_pin_members({key: nvr for key, nvr in pins.items() if key not in excluded_keys}, parent)
    old_by_key = {member['distgit_key']: member for member in old_members}
    for member in generated:
        old = deepcopy(old_by_key.get(member['distgit_key'], {}))
        metadata = old.setdefault('metadata', {})
        metadata['is'] = member['metadata']['is']
        member.update(old)
    definition['type'] = 'standard'
    basis['fbc_pullspecs'] = fbcs
    generated_keys = {member['distgit_key'] for member in generated}
    definition['members']['images'] = generated + [m for m in exclusions if m['distgit_key'] not in generated_keys]
    shipment = definition.setdefault('group', {}).setdefault('shipment', {})
    shipment.setdefault('advisories', [{'kind': 'image'}, {'kind': 'fbc'}])
    return definition


def _release_inputs(releases: dict, assembly: str) -> tuple:
    """Capture fields owned by release preparation and effective inherited pins."""
    entry = releases.get('releases', {}).get(assembly)
    if entry is None:
        return (None,)
    definition = entry.get('assembly', {})
    group = definition.get('group', {})
    return (
        definition.get('type'),
        definition.get('basis'),
        definition.get('members'),
        group.get('fbc_builds'),
        group.get('shipment'),
        assembly_image_pins(Model(releases), assembly),
    )


async def persist_direct_assembly(
    repo: GitRepository, group: str, assembly: str, definition: dict, expected: dict, mr_url: str
) -> bool:
    """Publish assembly content and pointer together without overwriting competing inputs.

    Args:
        repo: Build-data push checkout.
        group: Group branch to update.
        assembly: Assembly name.
        definition: Desired complete assembly definition.
        expected: Releases mapping read before preparing shipment content.
        mr_url: Authoritative shipment MR for the generated content.

    Returns:
        Whether a commit was pushed; identical reruns are successful no-ops.

    Raises:
        RuntimeError: If release inputs or the pointer changed concurrently.
    """
    await repo.fetch_switch_branch(group, remote='origin')
    yaml = new_roundtrip_yaml_handler()
    path = repo._directory / 'releases.yml'
    current = yaml.load(path) or {}
    if _release_inputs(current, assembly) != _release_inputs(expected, assembly):
        raise RuntimeError(f'Assembly {assembly} changed concurrently; rerun release preparation')
    entry = current.setdefault('releases', {}).setdefault(assembly, {})
    desired = deepcopy(entry.get('assembly', {}))
    for key in ('type', 'basis', 'members'):
        desired[key] = deepcopy(definition[key])
    desired_group = desired.setdefault('group', {})
    desired_group['shipment'] = deepcopy(definition['group']['shipment'])
    desired_group['shipment']['mr'] = mr_url
    if 'fbc_builds' in definition['group']:
        desired_group['fbc_builds'] = deepcopy(definition['group']['fbc_builds'])
    else:
        desired_group.pop('fbc_builds', None)
    if entry.get('assembly') == desired:
        return False
    entry['assembly'] = desired
    yaml.dump(current, path)
    return await repo.commit_push(f'Update assembly {assembly} and shipment MR', safe=True)
