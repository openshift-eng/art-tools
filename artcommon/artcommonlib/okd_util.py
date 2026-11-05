"""Shared helpers for merging per-image OKD configuration.

The OKD rebase CLI and the Konflux build CLI must apply the same per-image ``okd:``
merge so that overrides such as ``okd.konflux.network_mode`` and ``okd.konflux.cachi2``
are honored identically on both the rebase and build paths. Keeping a single
implementation here avoids the two call sites diverging over time.
"""

import logging
from copy import copy

from artcommonlib.model import Missing, Model
from artcommonlib.util import deep_merge

LOGGER = logging.getLogger(__name__)

# Fields that should be completely replaced by the okd stanza rather than deep-merged.
_REPLACE_FIELDS = {'from'}


def merge_okd_image_config(image_meta, logger: logging.Logger = None) -> Model:
    """Merge an image's ``okd:`` stanza into its base config, okd taking precedence.

    The returned :class:`Model` has the image's ``okd.*`` values merged over the base
    config (e.g. ``okd.konflux.network_mode`` surfaces as ``konflux.network_mode``), so
    downstream resolution like ``metadata.get_konflux_network_mode()`` and cachi2/lockfile
    handling reads the image-level OKD overrides. ``enabled_repos`` is cleared, matching
    OKD build expectations. ``image_meta.config`` is not mutated.

    If the image has no ``okd`` stanza, a copy of the config (with ``enabled_repos``
    cleared) is returned unchanged.
    """
    logger = logger or LOGGER
    image_config = copy(image_meta.config)
    image_config.enabled_repos = []

    if image_config.okd is not Missing:
        logger.info('Merging OKD configuration into image configuration for %s', image_meta.distgit_key)
        base_config = image_config.primitive()
        okd_config = image_meta.config.okd.primitive()

        # First, deep merge for fields that should be merged.
        merged_config = deep_merge(base_config, okd_config)

        # Then, replace fields that must be completely replaced rather than merged.
        for field in _REPLACE_FIELDS:
            if field in okd_config:
                merged_config[field] = okd_config[field]

        image_config = Model(merged_config)

    return image_config
