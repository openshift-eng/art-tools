"""
Shared OLM operator manifest image reference handling.

Used by doozer KonfluxOlmBundleRebaser and by elliott find-builds validation so behavior stays aligned.
"""

from __future__ import annotations

import asyncio
import glob
import logging
import os
import re
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml
from artcommonlib.constants import REGISTRY_PROXY_BASE_URL
from artcommonlib.konflux.konflux_build_record import Engine
from artcommonlib.model import Missing
from artcommonlib.util import oc_image_info_for_arch_async

LOGGER = logging.getLogger(__name__)


@lru_cache
def get_image_reference_pattern(registry: str) -> re.Pattern:
    """Match `registry/namespace/image:tag` style references for the given registry host."""
    pattern = r'{}\/([^:]+):([^\'"\\\s]+)'.format(re.escape(registry))
    return re.compile(pattern)


def get_digest_image_pattern() -> re.Pattern:
    """Match digest-pinned references: registry/path@sha256:hexdigest."""
    pattern = r'([a-zA-Z0-9][-a-zA-Z0-9.]*(?::[0-9]+)?/[^@\s]+)@(sha256:[a-fA-F0-9]{64})'
    return re.compile(pattern)


def load_delivery_repo_override_map(data_dir: str, logger: Optional[logging.Logger] = None) -> Dict[str, str]:
    """Short-name map for delivery_repo_name_override (same logic as KonfluxOlmBundleRebaser)."""
    log = logger or LOGGER
    delivery_override_map: Dict[str, str] = {}
    for _yml_path in glob.glob(f"{data_dir}/images/*.yml") + glob.glob(f"{data_dir}/images/*.yaml"):
        try:
            with open(_yml_path) as _yf:
                _img_data = yaml.safe_load(_yf)
            if not isinstance(_img_data, dict):
                continue
            _delivery = _img_data.get('delivery', {}) or {}
            if not _delivery.get('delivery_repo_name_override'):
                continue
            _repo_names = _delivery.get('delivery_repo_names') or []
            if len(_repo_names) != 1:
                raise IOError(
                    f"delivery_repo_name_override is set in {_yml_path} but delivery_repo_names has "
                    f"{len(_repo_names)} entries (expected exactly 1)"
                )
            _img_short = str(_img_data.get('name', '')).rsplit('/', 1)[-1]
            _override_short = str(_repo_names[0]).rsplit('/', 1)[-1]
            delivery_override_map[_img_short] = _override_short
        except (yaml.YAMLError, OSError) as e:
            log.debug("Failed to parse image YAML %s: %s", _yml_path, e)
    return delivery_override_map


def _operator_image_ref_mode(group_config: Any) -> str:
    # Prefer Model.get (matches KonfluxOlmBundleRebaser / MagicMock tests) over bare attribute access.
    if hasattr(group_config, 'get'):
        mode = group_config.get('operator_image_ref_mode', Missing)
        if mode not in (Missing, None, ''):
            return str(mode)
    mode = getattr(group_config, 'operator_image_ref_mode', Missing)
    if mode not in (Missing, None, ''):
        return str(mode)
    return 'manifest-list'


def _csv_namespace(group_config: Any) -> str:
    if hasattr(group_config, 'get'):
        ns = group_config.get('csv_namespace', 'openshift')
        if ns not in (Missing, None, ''):
            return str(ns)
    ns = getattr(group_config, 'csv_namespace', Missing)
    if ns not in (Missing, None, ''):
        return str(ns)
    return 'openshift'


async def replace_olm_manifest_image_references(
    old_registry: str,
    content: str,
    engine: Engine,
    metadata: Any,
    group_config: Any,
    image_repo: str,
    logger: Optional[logging.Logger] = None,
) -> Tuple[str, Dict[str, Tuple[str, str, str]]]:
    """
    Replace image references in YAML-ish content the same way KonfluxOlmBundleRebaser did historically.

    Returns (new_content, found_images) where found_images maps short name to
    (old_pullspec, new_pullspec, nvr_or_external).
    """
    log = logger or LOGGER
    new_content = content
    found_images: Dict[str, Tuple[str, str, str]] = {}

    pattern = get_image_reference_pattern(old_registry)
    matches = pattern.finditer(content)
    art_references: Dict[str, Tuple[str, str, str]] = {}
    for match in matches:
        pullspec = match.group(0)
        namespace, image_short_name = match.group(1).rsplit('/', maxsplit=1)
        image_tag = match.group(2)
        art_references[pullspec] = (namespace, image_short_name, image_tag)

    image_info_tasks: List[asyncio.Task] = []
    for pullspec, (namespace, image_short_name, image_tag) in art_references.items():
        if engine is Engine.KONFLUX:
            build_pullspec = f"{image_repo}:{image_short_name}-{image_tag}"
            image_info_tasks.append(
                asyncio.create_task(
                    oc_image_info_for_arch_async(
                        build_pullspec,
                        registry_config=os.getenv("QUAY_AUTH_FILE"),
                    )
                )
            )
        elif engine is Engine.BREW:
            build_pullspec = f"{REGISTRY_PROXY_BASE_URL}/rh-osbs/{namespace}-{image_short_name}:{image_tag}"
            image_info_tasks.append(
                asyncio.create_task(
                    oc_image_info_for_arch_async(
                        build_pullspec,
                    )
                )
            )
        else:
            raise ValueError(f"Unsupported engine {engine!r} for OLM image reference replacement")

    image_infos = await asyncio.gather(*image_info_tasks)

    csv_namespace = _csv_namespace(group_config)
    ref_mode = _operator_image_ref_mode(group_config)
    data_dir = metadata.runtime.data_dir
    delivery_override_map = load_delivery_repo_override_map(data_dir, logger=log)

    for pullspec, image_info in zip(art_references, image_infos):
        image_labels = image_info['config']['config']['Labels']
        image_version = image_labels['version']
        image_release = image_labels['release']
        image_component_name = image_labels['com.redhat.component']
        image_nvr = f"{image_component_name}-{image_version}-{image_release}"
        namespace, image_short_name, image_tag = art_references[pullspec]
        image_sha = image_info['contentDigest'] if ref_mode == 'by-arch' else image_info['listDigest']
        ocp_group = metadata.runtime.group
        if not ocp_group.startswith("openshift-"):
            new_namespace = namespace
        else:
            new_namespace = 'openshift4' if namespace == csv_namespace else namespace
        delivery_image_short_name = delivery_override_map.get(image_short_name, image_short_name)
        new_pullspec = '{}/{}@{}'.format(
            'registry.redhat.io',
            f'{new_namespace}/{delivery_image_short_name}',
            image_sha,
        )
        new_content = new_content.replace(pullspec, new_pullspec)
        found_images[delivery_image_short_name] = (pullspec, new_pullspec, image_nvr)

    digest_pattern = get_digest_image_pattern()
    for match in digest_pattern.finditer(new_content):
        image_path = match.group(1)
        digest = match.group(2)
        pullspec = f"{image_path}@{digest}"
        image_short_name = image_path.rsplit('/', 1)[-1]
        if image_short_name in found_images:
            continue
        found_images[image_short_name] = (pullspec, pullspec, "external")
        log.debug("Found digest-pinned external image: %s", pullspec)

    return new_content, found_images


async def validate_olm_manifest_image_references(
    old_registry: str,
    content: str,
    engine: Engine,
    metadata: Any,
    group_config: Any,
    image_repo: str,
    logger: Optional[logging.Logger] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Return (True, None) if replace_olm_manifest_image_references would succeed for this content.

    On failure return (False, human-readable reason).
    """
    try:
        await replace_olm_manifest_image_references(
            old_registry, content, engine, metadata, group_config, image_repo, logger=logger
        )
        return True, None
    except (KeyError, TypeError) as e:
        return False, f"missing image metadata field: {e}"
    except Exception as e:  # noqa: BLE001 — surface oc failures and YAML edge cases
        return False, str(e)


async def validate_olm_bundle_dir_yaml_files(
    files: Iterable[Tuple[str, str]],
    old_registry: str,
    engine: Engine,
    metadata: Any,
    group_config: Any,
    image_repo: str,
    logger: Optional[logging.Logger] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Validate each (relative_path, file_content) tuple; fail fast on first invalid file.
    """
    for rel_path, content in files:
        ok, err = await validate_olm_manifest_image_references(
            old_registry, content, engine, metadata, group_config, image_repo, logger=logger
        )
        if not ok:
            return False, f"{rel_path}: {err}"
    return True, None
