"""Check RPM identities against embargo markers and release evidence in Brew."""

from pathlib import Path
from urllib.parse import unquote, urlsplit

import yaml
from artcommonlib.build_visibility import VISIBILITY_SUFFIX, BuildVisibility
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from artcommonlib.rpm_utils import parse_nvr

from doozerlib import brew

_PRIVATE_SUFFIXES = {suffixes[BuildVisibility.PRIVATE] for suffixes in VISIBILITY_SUFFIX.values()}


def is_release_marked_private(release: str) -> bool:
    return bool(_PRIVATE_SUFFIXES.intersection(release.split('.')))


class EmbargoedRpmError(RuntimeError):
    """Public use of an image with embargoed RPM inputs is prohibited."""


def rpm_nvras_from_lockfile(path: Path) -> set[str]:
    """Read pinned binary and source RPMs across all architectures and stages."""
    if not path.is_file():
        return set()

    lockfile = yaml.safe_load(path.read_text())
    return {
        unquote(urlsplit(package['url']).path.rsplit('/', 1)[-1]).removesuffix('.rpm')
        for arch in lockfile['arches']
        for field in ('packages', 'source')
        for package in arch.get(field, ())
    }


def rpm_inventory_from_build(build: KonfluxBuildRecord) -> tuple[set[str], set[str]]:
    # Historical records store binary NVRs without per-package architecture.
    arches = set(build.arches or ()) | {'noarch'}
    nvras = {f'{nvr}.{arch}' for nvr in build.installed_rpms or () for arch in arches}
    return nvras, set(build.installed_packages or ())


def find_embargoed_rpms(session, *, rpm_nvras=(), source_nvrs=()) -> set[str]:
    """Return input RPM identities whose exact build has an active embargo.

    Build IDs tie binary subpackages to their source builds, where Brew records
    tags. RPMs from outside Brew can still be classified by their own suffix.
    """
    rpms = sorted({(rpm, False) for rpm in rpm_nvras} | {(rpm, True) for rpm in source_nvrs})
    if not rpms:
        return set()

    with session.multicall(strict=True) as multicall:
        tasks = [
            multicall.getBuild(_normalize_nvr(identity)) if source else multicall.getRPM(_normalize_nvr(identity))
            for identity, source in rpms
        ]

    builds_by_rpm = {}
    builds_by_nvr = {}
    for (identity, source), task in zip(rpms, tasks, strict=True):
        obj = task.result
        if obj and obj.get('build_id'):
            builds_by_rpm[identity] = obj['build_id']
            builds_by_nvr.setdefault(_rpm_nvr(identity, source), set()).add(obj['build_id'])

    build_ids = sorted(set(builds_by_rpm.values()))
    tag_results = brew.get_builds_tags(build_ids, session) if build_ids else ()
    build_tags = {
        build_id: {tag['name'] for tag in tags} for build_id, tags in zip(build_ids, tag_results, strict=True)
    }
    shipped = {build_id for build_id, tags in build_tags.items() if any(tag.endswith('-released') for tag in tags)}
    embargo_tags = {
        build_id for build_id, tags in build_tags.items() if any(tag.endswith('-embargoed') for tag in tags)
    }

    embargoed = set()
    for identity, source in rpms:
        exact_id = builds_by_rpm.get(identity)
        # Old inventories lack per-RPM arches. An unsuccessful alternative-arch
        # lookup must not obscure release evidence for the same RPM NVR.
        resolved_ids = {exact_id} if exact_id else builds_by_nvr.get(_rpm_nvr(identity, source), set())
        if resolved_ids and resolved_ids <= shipped:
            continue
        if is_release_marked_private(parse_nvr(identity)['release']) or (resolved_ids - shipped) & embargo_tags:
            embargoed.add(identity)
    return embargoed


def _rpm_nvr(identity: str, source: bool) -> str:
    normalized = _normalize_nvr(identity)
    return normalized if source else normalized.rsplit('.', 1)[0]


def _normalize_nvr(identity: str) -> str:
    parts = parse_nvr(identity)
    return f"{parts['name']}-{parts['version']}-{parts['release']}"
