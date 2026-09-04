import asyncio
import logging
import sys
from copy import copy
from pathlib import Path

import click
from artcommonlib.model import Missing, Model
from artcommonlib.util import (
    deep_merge,
)
from artcommonlib.variants import BuildVariant

from doozerlib import Runtime, constants
from doozerlib.backend.rebaser import KonfluxRebaser
from doozerlib.cli import (
    cli,
    click_coroutine,
    option_commit_message,
    option_push,
    pass_runtime,
    validate_semver_major_minor_patch,
)
from doozerlib.image import ImageMetadata
from doozerlib.state import STATE_FAIL, STATE_PASS

OKD_DEFAULT_IMAGE_REPO = 'quay.io/redhat-user-workloads/ocp-art-tenant/art-okd-images'


@cli.group("images:okd", short_help="Manage OKD builds in prow.")
def images_okd():
    """
    Sub-verbs to managing the content of OKD builds.
    """
    pass


class OkdRebaseCli:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        release: str,
        image_repo: str,
        message: str,
        push: bool,
    ):
        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.version = version
        self.release = release
        self.image_repo = image_repo
        self.message = message
        self.push = push
        self.upcycle = runtime.upcycle
        self.state = {}

    async def run(self):
        # OKD configuration is automatically merged in get_group_config() when variant=okd
        # Initialize with disabled=True to load all images (including those with mode: disabled)
        # This is necessary because some parent images may be disabled for OCP but enabled for OKD
        self.runtime.initialize(mode='images', clone_distgits=False, build_system='konflux', disabled=True)

        # Apply OKD config to ALL loaded images
        # This ensures that parent images also get the OKD branch overrides
        for image_meta in self.runtime.image_metas():
            if image_meta.config.okd is not Missing:
                # Apply the OKD configuration to this image
                image_meta.config = self.get_okd_image_config(image_meta)
                self.logger.debug(f'Applied OKD config to {image_meta.distgit_key}')

        # Wrap late_resolve_image to apply OKD config to any images loaded during rebase
        # This handles parent images that are loaded on-demand when not explicitly included via --images
        original_late_resolve_image = self.runtime.late_resolve_image

        def okd_late_resolve_image(distgit_name, add=False, required=True):
            # Call the original method to load the image
            meta = original_late_resolve_image(distgit_name, add=add, required=required)
            # Apply OKD config if this image has one
            if meta and meta.config.okd is not Missing:
                meta.config = self.get_okd_image_config(meta)
                self.logger.debug(f'Applied OKD config to late-resolved image {meta.distgit_key}')
            return meta

        # Replace the method on the runtime instance
        self.runtime.late_resolve_image = okd_late_resolve_image

        # For OKD, we need to use the OKD group variant (e.g., okd-4.20 instead of openshift-4.20)
        # This ensures the Konflux DB cache is loaded for the correct group
        major, minor = self.runtime.get_major_minor_fields()
        self.runtime.group = f'okd-{major}.{minor}'
        self.logger.info(f'Changed runtime group to OKD variant: {self.runtime.group}')

        base_dir = Path(self.runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_OKD_SOURCES)
        rebaser = KonfluxRebaser(
            runtime=self.runtime,
            base_dir=base_dir,
            source_resolver=self.runtime.source_resolver,
            repo_type='unsigned',
            upcycle=self.upcycle,
            variant=BuildVariant.OKD,
            image_repo=self.image_repo,
        )

        # Rebase all loaded images (filtered by --images flag if specified)
        metas = self.runtime.ordered_image_metas()
        tasks = [self.rebase_image(image_meta, rebaser) for image_meta in metas]
        await asyncio.gather(*tasks, return_exceptions=False)

        # Check rebase results, log errors if any in state.yml
        self.runtime.state.setdefault('images:okd:rebase', {})['images'] = self.state

        if any((state == 'failure' for state in self.state.values())):
            self.runtime.state['status'] = STATE_FAIL
            sys.exit(1)

        else:
            self.runtime.state['status'] = STATE_PASS

    async def rebase_image(self, image_meta: ImageMetadata, rebaser: KonfluxRebaser):
        image_meta.config = self.get_okd_image_config(image_meta)
        image_name = image_meta.distgit_key

        if image_meta.config.mode == 'disabled':
            # Raise an exception to be caught in okd4 pipeline; image will be removed from the building list.
            self.logger.warning('Image %s is disabled for OKD: skipping rebase', image_name)
            image_meta.rebase_status = True
            image_meta.rebase_event.set()
            self.state[image_name] = {
                'status': 'skipped',
                'private_fix': False,
            }
            return

        try:
            await rebaser.rebase_to(
                image_meta,
                self.version,
                self.release,
                force_yum_updates=False,
                commit_message=self.message,
                push=self.push,
            )
            self.state[image_name] = {
                'status': 'success',
                'private_fix': image_meta.private_fix if image_meta.private_fix else False,
            }

        except Exception as e:
            self.logger.warning('Failed rebasing %s: %s', image_name, e)
            self.state[image_name] = {
                'status': 'failure',
                'private_fix': False,
            }

    def get_okd_image_config(self, image_meta: ImageMetadata):
        image_config = copy(image_meta.config)
        image_config.enabled_repos = []

        if image_config.okd is not Missing:
            # Merge the rest of the config, with okd taking precedence
            # Certain fields like 'from' should be completely replaced, not merged
            self.logger.info('Merging OKD configuration into image configuration for %s', image_meta.distgit_key)
            base_config = image_config.primitive()
            okd_config = image_meta.config.okd.primitive()

            # Fields that should be completely replaced rather than merged
            replace_fields = {'from'}

            # First, do a deep merge for fields that should be merged
            merged_config = deep_merge(base_config, okd_config)

            # Then, replace fields that should be completely replaced
            for field in replace_fields:
                if field in okd_config:
                    merged_config[field] = okd_config[field]

            image_config = Model(merged_config)

        return image_config


@images_okd.command("rebase", short_help="Refresh a group's OKD source content.")
@click.option(
    "--version",
    metavar='VERSION',
    required=True,
    callback=validate_semver_major_minor_patch,
    help="Version string to populate in Dockerfiles.",
)
@click.option("--release", metavar='RELEASE', required=True, help="Release string to populate in Dockerfiles.")
@click.option('--image-repo', default=OKD_DEFAULT_IMAGE_REPO, help='Image repo for base images')
@option_commit_message
@option_push
@pass_runtime
@click_coroutine
async def images_okd_rebase(
    runtime: Runtime,
    version: str,
    release: str,
    image_repo: str,
    message: str,
    push: bool,
):
    """
    Refresh a group's konflux content from source content.
    """

    runtime.variant = BuildVariant.OKD  # Set variant so get_group_config() merges okd config
    runtime.network_mode_override = 'open'  # OKD builds must be done in open mode.

    await OkdRebaseCli(
        runtime=runtime,
        version=version,
        release=release,
        image_repo=image_repo,
        message=message,
        push=push,
    ).run()
