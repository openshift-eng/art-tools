import asyncio
import logging
from pathlib import Path
from typing import Optional

import click

from doozerlib import constants, opm
from doozerlib.backend.konflux_fbc import KonfluxFbcImporter
from doozerlib.cli import (cli, click_coroutine, option_commit_message,
                           pass_runtime)
from doozerlib.exceptions import DoozerFatalError
from doozerlib.runtime import Runtime

LOGGER = logging.getLogger(__name__)
yaml = opm.yaml


class FbcImportCli:
    def __init__(self, runtime: Runtime, index_image: str | None, keep_templates: bool, push: bool,
                 fbc_repo: str, message: str, dest_dir: str | None):
        self.runtime = runtime
        self.index_image = index_image
        self.keep_templates = keep_templates
        self.push = push
        self.fbc_repo = fbc_repo or constants.ART_FBC_GIT_REPO
        self.message = message
        self.dest_dir = Path(dest_dir) if dest_dir else Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_FBC_SOURCES)

    async def run(self):
        """ Run the FBC import process
        This function implements the main logic of the FBC import process
        following https://github.com/konflux-ci/olm-operator-konflux-sample/blob/main/docs/konflux-onboarding.md#create-the-fbc-in-the-git-repository.
        """
        # Ensure opm is installed
        try:
            await opm.verify_opm()
        except (IOError, FileNotFoundError):
            LOGGER.error("Please install the latest opm cli binary from https://github.com/operator-framework/operator-registry/releases")
            raise

        # Initialize runtime
        runtime = self.runtime
        runtime.initialize(mode="images", clone_distgits=False)
        assert runtime.group_config is not None, "group_config is not loaded; Doozer bug?"
        if not runtime.assembly:
            raise ValueError("Assemblies feature is disabled for this group. This is no longer supported.")
        if not runtime.group_config.vars or "MAJOR" not in runtime.group_config.vars or "MINOR" not in runtime.group_config.vars:
            raise ValueError("MAJOR and MINOR must be set in group vars.")
        major, minor = int(runtime.group_config.vars.MAJOR), int(runtime.group_config.vars.MINOR)

        operator_metadatas = [operator_meta for operator_meta in runtime.ordered_image_metas() if operator_meta.is_olm_operator]
        if not operator_metadatas:
            raise ValueError("No operator images loaded in group")
        importer = KonfluxFbcImporter(
            base_dir=self.dest_dir,
            group=runtime.group,
            assembly=str(runtime.assembly),
            ocp_version=(major, minor),
            keep_templates=self.keep_templates,
            upcycle=runtime.upcycle,
            push=self.push,
            commit_message=self.message,
            fbc_repo=self.fbc_repo,
        )

        LOGGER.info("Importing FBC from index image...")
        tasks = []
        for metadata in operator_metadatas:
            tasks.append(importer.import_from_index_image(metadata, self.index_image))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed_tasks = []
        for metadata, result in zip(operator_metadatas, results):
            if isinstance(result, Exception):
                failed_tasks.append(metadata.distgit_key)
                LOGGER.error(f"Failed to import FBC for {metadata.distgit_key}: {result}")
        if failed_tasks:
            raise DoozerFatalError(f"Failed to import FBC for bundles: {', '.join(failed_tasks)}")
        LOGGER.info("FBC import complete")


@cli.command("beta:fbc:import", short_help="Create FBC by importing from the provided index image")
@click.option("--from-index", metavar='INDEX_IMAGE', help="The index image to import from. If not set, the production index image will be used.")
@click.option("--keep-templates", is_flag=True, help="Keep the generated templates. If not set, the templates will be deleted after rendering the final catalogs.")
@click.option("--push", is_flag=True, help="Push the generated FBC to the git repository.")
@click.option("--fbc-repo", metavar='FBC_REPO', help="The git repository to push the FBC to.", default=constants.ART_FBC_GIT_REPO)
@option_commit_message
@click.argument("dest_dir", metavar='DEST_DIR', required=False, default=None)
@pass_runtime
@click_coroutine
async def fbc_import(runtime: Runtime, from_index: Optional[str], keep_templates: bool, push: bool, fbc_repo: str, message: str, dest_dir: Optional[str]):
    """
    Create an FBC repository by importing from the provided index image

    Example usage:

    doozer --group=openshift-4.17 beta:fbc:import registry.redhat.io/redhat/redhat-operator-index:v4.17 ./fbc-4.17
    """
    cli = FbcImportCli(runtime=runtime, index_image=from_index, keep_templates=keep_templates, push=push, fbc_repo=fbc_repo, message=message, dest_dir=dest_dir)
    await cli.run()
