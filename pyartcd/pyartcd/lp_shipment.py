import copy
import re
from collections import Counter
from io import StringIO
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

from artcommonlib import exectools
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from elliottlib.shipment_model import ShipmentConfig

from pyartcd.fbc_util import extract_ocp_version_from_nvr
from pyartcd.git import GitRepository

YAML = new_roundtrip_yaml_handler()
_TIMESTAMP_RE = re.compile(r"(\d{14})$")
_COUNTER_RE = re.compile(r"(\d{14})(\d{2})\.ya?ml$")


def _project_path(url: str) -> str:
    return urlparse(url).path.strip('/').removesuffix('.git')


def get_shipment_mr_url(releases_config: dict, assembly: str) -> str | None:
    """Return the LP shipment MR pointer for an assembly, if one exists."""
    return (
        (releases_config or {})
        .get('releases', {})
        .get(assembly, {})
        .get('assembly', {})
        .get('group', {})
        .get('shipment', {})
        .get('mr')
    )


async def update_shipment_mr_url(
    repo: GitRepository,
    group: str,
    assembly: str,
    mr_url: str,
    expected_mr_url: str | None,
    *,
    create_as_stream: bool,
) -> bool:
    """Persist an LP shipment pointer with optimistic concurrency protection."""
    await repo.fetch_switch_branch(group, remote="origin")
    releases_path = repo._directory / "releases.yml"
    releases_config = YAML.load(releases_path) if releases_path.exists() else None
    releases_config = releases_config or {}

    current_mr_url = get_shipment_mr_url(releases_config, assembly)
    if current_mr_url != expected_mr_url:
        raise RuntimeError(
            f"Shipment MR pointer changed concurrently from {expected_mr_url!r} to {current_mr_url!r}; "
            "refusing to overwrite it"
        )

    releases = releases_config.setdefault('releases', {})
    if not create_as_stream and assembly not in releases:
        raise RuntimeError(f"Assembly {assembly} disappeared before the shipment MR pointer could be stored")
    assembly_entry = releases.setdefault(assembly, {})
    assembly_def = assembly_entry.setdefault('assembly', {})
    if create_as_stream and not assembly_def:
        assembly_def['type'] = 'stream'
    shipment = assembly_def.setdefault('group', {}).setdefault('shipment', {})
    shipment['mr'] = mr_url
    YAML.dump(releases_config, releases_path)
    return await repo.commit_push(f"Update assembly {assembly}: add shipment MR URL", safe=True)


async def verify_shipment_mr_url(repo: GitRepository, group: str, assembly: str, expected_mr_url: str) -> None:
    """Confirm releases.yml still points at the MR selected earlier in the run."""
    await repo.fetch_switch_branch(group, remote="origin")
    releases_path = repo._directory / "releases.yml"
    releases_config = YAML.load(releases_path) if releases_path.exists() else None
    current_mr_url = get_shipment_mr_url(releases_config or {}, assembly)
    if current_mr_url != expected_mr_url:
        raise RuntimeError(
            f"Shipment MR pointer changed concurrently from {expected_mr_url!r} to {current_mr_url!r}; "
            "refusing to update the old MR"
        )


def validate_shipment_mr(gitlab_client, mr_url: str, pull_url: str, push_url: str):
    """Validate an MR referenced by releases.yml and return the MR object."""
    if urlparse(mr_url).netloc != urlparse(pull_url).netloc:
        raise ValueError(
            f"Shipment MR host {urlparse(mr_url).netloc} does not match {urlparse(pull_url).netloc}. "
            "Use --force to create a replacement MR."
        )
    target_project_path, _ = gitlab_client._parse_mr_url(mr_url)
    mr = gitlab_client.get_mr_from_url(mr_url)
    if not mr:
        raise ValueError(f"Shipment MR {mr_url} was not found. Use --force to create a replacement MR.")
    if mr.state != "opened":
        raise ValueError(f"Shipment MR state is {mr.state}, not opened. Use --force to create a replacement MR.")
    if target_project_path != _project_path(pull_url):
        raise ValueError(
            f"Shipment MR target project {target_project_path} does not match {_project_path(pull_url)}. "
            "Use --force to create a replacement MR."
        )
    source_project_path = gitlab_client.get_project(mr.source_project_id).path_with_namespace
    if source_project_path != _project_path(push_url):
        raise ValueError(
            f"Shipment MR source project {source_project_path} does not match {_project_path(push_url)}. "
            "Use --force to create a replacement MR."
        )
    if mr.target_branch != "main":
        raise ValueError(
            f"Shipment MR target branch is {mr.target_branch}, not main. Use --force to create a replacement MR."
        )
    return mr


def set_shipment_mr_draft(mr, dry_run: bool) -> None:
    if mr.title.startswith("Draft:"):
        return
    mr.title = f"Draft: {mr.title}"
    if not dry_run:
        mr.save()


def _to_dict(config: ShipmentConfig) -> dict:
    return config.model_dump(exclude_unset=True, exclude_none=True)


def _identity(config: dict) -> tuple:
    shipment = config.get('shipment', {})
    metadata = shipment.get('metadata', {})
    base = (
        metadata.get('product'),
        metadata.get('group'),
        metadata.get('assembly'),
        metadata.get('application'),
        bool(metadata.get('fbc', False)),
    )
    if not base[-1]:
        return base

    nvrs = shipment.get('snapshot', {}).get('nvrs', [])
    if len(nvrs) != 1:
        raise ValueError(f"Expected one NVR in an FBC shipment, found {len(nvrs)}")
    nvr = nvrs[0]
    component = parse_nvr(nvr)['name']
    return (*base, component, extract_ocp_version_from_nvr(nvr))


def _identity_sort_key(item: tuple) -> tuple[str, ...]:
    return tuple("" if value is None else str(value) for value in item[0])


def _reconcile_config(existing: dict, desired: dict) -> dict:
    """Apply generator-owned data while preserving downstream environment results."""
    result = copy.deepcopy(existing)
    existing_shipment = result.setdefault('shipment', {})
    desired_shipment = desired['shipment']
    existing_shipment['metadata'] = copy.deepcopy(desired_shipment['metadata'])
    existing_shipment['snapshot'] = copy.deepcopy(desired_shipment['snapshot'])

    if 'data' in desired_shipment:
        existing_shipment['data'] = copy.deepcopy(desired_shipment['data'])
    else:
        existing_shipment.pop('data', None)

    existing_environments = existing_shipment.setdefault('environments', {})
    for env_name, desired_env in desired_shipment.get('environments', {}).items():
        existing_env = existing_environments.setdefault(env_name, {})
        if 'releasePlan' in desired_env:
            existing_env['releasePlan'] = desired_env['releasePlan']
        else:
            existing_env.pop('releasePlan', None)
    return result


async def _restore_from_main(repo: GitRepository, path: str) -> None:
    rc, content, error = await exectools.cmd_gather_async(
        ["git", "-C", str(repo._directory), "show", f"main:{path}"], env=repo._local_env()
    )
    if rc:
        raise RuntimeError(f"Unable to restore {path} from main: {error}")
    await repo.write_file(path, content)


async def reconcile_shipment_mr(
    repo: GitRepository,
    mr,
    shipments_by_kind: Dict[str, ShipmentConfig],
    *,
    include_fbc_ocp_version: bool,
    dry_run: bool,
) -> bool:
    """Reconcile generated LP shipments into an existing MR branch."""
    timestamp_match = _TIMESTAMP_RE.search(mr.source_branch)
    if not timestamp_match:
        raise ValueError(f"Cannot determine shipment timestamp from MR branch {mr.source_branch}")
    timestamp = timestamp_match.group(1)

    desired_items = [(kind, _to_dict(config)) for kind, config in shipments_by_kind.items()]
    desired_identities = [_identity(config) for _, config in desired_items]
    duplicates = [identity for identity, count in Counter(desired_identities).items() if count > 1]
    if duplicates:
        raise ValueError(f"Generated shipment configurations contain duplicate identities: {duplicates}")
    desired_by_identity = {identity: item for identity, item in zip(desired_identities, desired_items)}

    await repo.fetch_switch_branch(mr.source_branch, remote="origin")
    change_data = mr.changes()
    if change_data.get('overflow'):
        raise RuntimeError("GitLab truncated the shipment MR change list; refusing an incomplete reconciliation")
    changes = change_data.get('changes', [])
    existing_by_identity: dict[tuple, tuple[str, dict, bool]] = {}
    for change in changes:
        path = change['new_path']
        if not path.startswith('shipment/') or not path.endswith(('.yaml', '.yml')):
            continue
        absolute_path = repo._directory / path
        if not absolute_path.exists():
            continue
        config = YAML.load(absolute_path)
        if not isinstance(config, dict) or 'shipment' not in config:
            continue
        identity = _identity(config)
        if identity[:3] != desired_identities[0][:3]:
            continue
        if identity in existing_by_identity:
            raise ValueError(f"Existing MR contains duplicate shipment identity {identity}")
        is_new = change.get('new_file') is True or change.get('new_file') == 'true'
        existing_by_identity[identity] = (path, config, is_new)

    used_counters = {
        int(match.group(2))
        for path, _, _ in existing_by_identity.values()
        if (match := _COUNTER_RE.search(path)) and match.group(1) == timestamp
    }
    changed = False

    for identity, (path, existing, _) in existing_by_identity.items():
        desired_item = desired_by_identity.pop(identity, None)
        if desired_item:
            _, desired = desired_item
            reconciled = _reconcile_config(existing, desired)
            if reconciled != existing:
                out = StringIO()
                YAML.dump(reconciled, out)
                await repo.write_file(path, out.getvalue())
                changed = True

    for identity, (path, _, is_new) in existing_by_identity.items():
        if identity in desired_identities:
            continue
        if is_new:
            (repo._directory / path).unlink()
        else:
            await _restore_from_main(repo, path)
        changed = True

    next_counter = 1
    for identity, (kind, desired) in sorted(desired_by_identity.items(), key=_identity_sort_key):
        while next_counter in used_counters:
            next_counter += 1
        metadata = desired['shipment']['metadata']
        target_dir = Path('shipment') / metadata['product'] / metadata['group'] / metadata['application'] / 'prod'
        if metadata.get('fbc'):
            ocp_part = f".ocp{identity[-1]}" if include_fbc_ocp_version and identity[-1] else ""
            filename = f"{metadata['assembly']}.fbc{ocp_part}.{timestamp}{next_counter:02d}.yaml"
            used_counters.add(next_counter)
            next_counter += 1
        else:
            filename = f"{metadata['assembly']}.{kind.rstrip('0123456789')}.{timestamp}.yaml"
        out = StringIO()
        YAML.dump(desired, out)
        path = target_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        await repo.write_file(path, out.getvalue())
        changed = True

    if not changed:
        return False
    await repo.log_diff()
    if dry_run:
        return True
    return await repo.commit_push(f"Update shipment configurations for {desired_identities[0][2]}", safe=True)
