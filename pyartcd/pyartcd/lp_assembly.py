"""Generate layered-product assemblies and publish build-data updates."""

import asyncio
import logging
from collections.abc import Callable
from copy import deepcopy
from hashlib import sha256

from artcommonlib.assembly import assembly_image_pins
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.model import Model
from artcommonlib.util import new_roundtrip_yaml_handler, split_git_url
from github import GithubException

from pyartcd.git import GitRepository

LOGGER = logging.getLogger(__name__)
_IMAGE_PIN_WHY = 'Pin this image build for the assembly'
_MERGE_TIMEOUT = 30 * 60
_MERGE_POLL_INTERVAL = 30


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
            'why': _IMAGE_PIN_WHY,
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
    excluded_keys = {member['distgit_key'] for member in old_members if member.get('exclude') is True}
    generated = image_pin_members({key: nvr for key, nvr in pins.items() if key not in excluded_keys}, parent)
    old_by_key = {member['distgit_key']: member for member in old_members}
    for member in generated:
        old = deepcopy(old_by_key.get(member['distgit_key'], {}))
        metadata = old.setdefault('metadata', {})
        # Current inputs own the pin, regardless of its previous merge operator.
        for field in ('is!', 'is?', 'is-'):
            metadata.pop(field, None)
        metadata['is'] = member['metadata']['is']
        member.update(old)
    definition['type'] = 'standard'
    basis['fbc_pullspecs'] = fbcs
    generated_keys = {member['distgit_key'] for member in generated}
    for old in old_members:
        if old['distgit_key'] in generated_keys:
            continue
        member = deepcopy(old)
        metadata = member.get('metadata', {})
        for field in ('is', 'is!', 'is?'):
            image_pin = metadata.get(field, {})
            for nvr_field in ('nvr', 'nvr!', 'nvr?'):
                image_pin.pop(nvr_field, None)
            if not image_pin:
                metadata.pop(field, None)
        if member['distgit_key'] in pins and member['distgit_key'] not in excluded_keys:
            # A selection matching the parent needs no child pin, but must
            # remove a previous suppression so the parent pin takes effect.
            for field in ('is', 'is!', 'is?', 'is-'):
                metadata.pop(field, None)
        if not metadata:
            member.pop('metadata', None)
        # A redundant or removed pin must not discard unrelated image settings
        # or explicit exclusions. A pin-only member can disappear entirely.
        if set(member) - {'distgit_key', 'why'}:
            generated.append(member)
    definition['members']['images'] = generated
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
    repo: GitRepository, group: str, assembly: str, definition: dict, expected: dict, mr_url: str, *, repo_url: str
) -> bool:
    """Merge assembly content and pointer together through a build-data PR.

    Args:
        repo: Build-data push checkout.
        group: Group branch to update.
        assembly: Assembly name.
        definition: Desired complete assembly definition.
        expected: Releases mapping read before preparing shipment content.
        mr_url: Authoritative shipment MR for the generated content.
        repo_url: GitHub URL of the build-data push repository.

    Returns:
        Whether a PR merged; identical reruns are successful no-ops.

    Raises:
        RuntimeError: If release inputs or the pointer changed concurrently.
    """
    await repo.fetch_switch_branch(group, remote='origin')
    yaml = new_roundtrip_yaml_handler()
    path = repo._directory / 'releases.yml'
    current = yaml.load(path) or {}
    expected_inputs = _release_inputs(expected, assembly)

    def validate_base(releases: dict) -> None:
        if _release_inputs(releases, assembly) != expected_inputs:
            raise RuntimeError(f'Assembly {assembly} changed concurrently; rerun release preparation')

    validate_base(current)
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
    return await merge_assembly_pr(
        repo,
        group,
        assembly,
        repo_url=repo_url,
        title=f'Update assembly {assembly} and shipment MR',
        details=f'Shipment MR: {mr_url}',
        validate_base=validate_base,
    )


def _backfill_image_pin_reasons(releases: dict) -> bool:
    """Add missing reasons to explicit image pins in every assembly."""
    changed = False
    for entry in releases.get('releases', {}).values():
        for member in entry.get('assembly', {}).get('members', {}).get('images', []):
            if member.get('exclude') is True or any(key in member for key in ('why', 'why!', 'why?', 'why-')):
                continue
            metadata = member.get('metadata', {})
            for pin_key in ('is', 'is!', 'is?'):
                pin = metadata.get(pin_key)
                if isinstance(pin, dict) and any(pin.get(nvr_key) for nvr_key in ('nvr', 'nvr!', 'nvr?')):
                    member['why'] = _IMAGE_PIN_WHY
                    changed = True
                    break
    return changed


async def merge_assembly_pr(
    repo: GitRepository,
    group: str,
    assembly: str,
    *,
    repo_url: str,
    title: str,
    details: str,
    validate_base: Callable[[dict], None],
) -> bool:
    """Publish a prepared releases.yml change and wait for GitHub to merge it.

    The caller prepares releases.yml on the group branch and supplies the
    checks to repeat against the target branch before merging.
    """
    path = repo._directory / 'releases.yml'
    yaml = new_roundtrip_yaml_handler()
    releases = yaml.load(path) or {}
    # Presubmit validates historical assemblies in this file as well as the update.
    if _backfill_image_pin_reasons(releases):
        yaml.dump(releases, path)
    content = path.read_bytes()
    # Reuse the branch and its CI results when retrying the same update.
    digest = sha256(content).hexdigest()[:16]
    branch = f'update-assembly-{group}-{assembly}-{digest}'
    _, owner, name = split_git_url(repo_url)
    gh_repo = get_github_client_for_org(owner).get_repo(f'{owner}/{name}')
    head = f'{owner}:{branch}'

    if not await repo.does_branch_exist_on_remote(branch, remote='origin'):
        await repo.create_branch(branch, reset=True)
        if not await repo.commit_push(title, safe=True):
            return False
    head_sha = gh_repo.get_branch(branch).commit.sha
    if gh_repo.get_contents('releases.yml', ref=head_sha).decoded_content != content:
        raise RuntimeError(f'Build-data PR branch {branch} changed concurrently')

    pr = next(iter(gh_repo.get_pulls(state='open', head=head, base=group)), None)
    if pr is None:
        pr = gh_repo.create_pull(
            title=title,
            body=f'Update `releases.yml` for {group} assembly {assembly}.\n\n{details}',
            head=head,
            base=group,
        )
    LOGGER.info('Waiting for build-data PR to merge: %s', pr.html_url)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _MERGE_TIMEOUT
    while True:
        pr = gh_repo.get_pull(pr.number)
        if pr.head.sha != head_sha or pr.base.ref != group:
            raise RuntimeError(f'Build-data PR head or target branch changed concurrently: {pr.html_url}')
        if pr.merged:
            break
        if pr.state == 'closed':
            raise RuntimeError(f'Build-data PR was closed without merging: {pr.html_url}')

        latest = yaml.load(gh_repo.get_contents('releases.yml', ref=group).decoded_content) or {}
        try:
            validate_base(latest)
        except (RuntimeError, ValueError) as exc:
            # A manual merge may have changed the base since we fetched the PR.
            if gh_repo.get_pull(pr.number).merged:
                continue
            raise RuntimeError(f'{exc} (build-data PR {pr.html_url})') from exc

        try:
            result = pr.merge(sha=head_sha)
            if result.merged:
                break
            blocked_by = result.message
        except GithubException as exc:
            if exc.status != 405:
                raise
            blocked_by = str(exc)
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError(f'Timed out merging build-data PR {pr.html_url}: {blocked_by}')
        LOGGER.info('Build-data PR %s cannot merge yet: %s', pr.html_url, blocked_by)
        await asyncio.sleep(min(_MERGE_POLL_INTERVAL, remaining))
    LOGGER.info('Merged build-data PR: %s', pr.html_url)
    return True
