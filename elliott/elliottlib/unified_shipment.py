"""Shared identity and input validation for assembly-based unified shipments."""

import re

from artcommonlib.assembly import assembly_image_pins
from artcommonlib.model import Model


def effective_inputs(releases: dict, assembly: str, target: str) -> dict:
    """Capture release content without mutable delivery bookkeeping.

    Args:
        releases: Complete releases.yml document, including inherited assemblies.
        assembly: Contributing named assembly.
        target: Catalog OCP major.minor version.

    Returns:
        Effective image pins and immutable operator FBC selections.

    Raises:
        ValueError: If the contributing assembly or target records are absent.
    """
    definition = releases.get('releases', {}).get(assembly, {}).get('assembly', {})
    records = definition.get('group', {}).get('fbc_builds', {}).get(target)
    if not records:
        raise ValueError(f'{assembly} has no FBC records for OCP {target}')
    pins = sorted(set(assembly_image_pins(Model(releases), assembly).values()))
    if not pins:
        raise ValueError(f'{assembly} has no effective image pins')
    selected = {}
    for operator, record in sorted(records.items()):
        if sorted(record.get('operand_nvrs', [])) != pins:
            raise ValueError(f'{assembly}/{operator} has stale FBC pins')
        pullspec = record.get('pullspec', '')
        if not re.fullmatch(r'[^@\s]+@sha256:[0-9a-f]{64}', pullspec):
            raise ValueError(f'{assembly}/{operator} has no immutable FBC digest')
        selected[operator] = {'nvr': record['nvr'], 'pullspec': pullspec}
    return {'image_nvrs': pins, 'fbcs': selected}


def validate_contributor_inputs(provenance: dict, contributors: list, load_releases) -> None:
    """Compare current assembly content with the pinned catalog input selection.

    Args:
        provenance: Catalog provenance read at its immutable source commit.
        contributors: Validated shipment contributor models.
        load_releases: Callable accepting (group, revision); None means current branch.

    Raises:
        ValueError: If provenance associations differ or relevant input content changed.
    """
    target = provenance.get('ocp_target_version')
    if not isinstance(target, str) or not re.fullmatch(r'[0-9]+\.[0-9]+', target):
        raise ValueError('Provenance has no valid OCP target')
    recorded = provenance.get('inputs')
    if not isinstance(recorded, list) or not recorded:
        raise ValueError('Assembly-based unified provenance has no inputs')
    identities = {(a.group, a.assembly, a.revision) for a in contributors}
    actual = {(r['group'], r.get('assembly'), r.get('assembly_group_revision')) for r in recorded}
    if identities != actual:
        raise ValueError('Shipment contributors do not match pinned catalog provenance')
    for a in contributors:
        pinned = load_releases(a.group, a.revision)
        expected = effective_inputs(pinned, a.assembly, target)
        selections = {
            r['nvr']: r['pullspec'] for r in recorded if r['group'] == a.group and r['assembly'] == a.assembly
        }
        if selections != {r['nvr']: r['pullspec'] for r in expected['fbcs'].values()}:
            raise ValueError(f'{a.group}/{a.assembly} FBC selection differs from pinned provenance')
        current = load_releases(a.group, None)
        if effective_inputs(current, a.assembly, target) != expected:
            raise ValueError(f'{a.group}/{a.assembly} changed since unified preparation; regenerate and restage')
        pointer = current['releases'][a.assembly]['assembly'].get('group', {}).get('shipment', {}).get('mr')
        if pointer != a.imageShipmentMR:
            raise ValueError(f'{a.group}/{a.assembly} image shipment association changed; reconcile before release')
