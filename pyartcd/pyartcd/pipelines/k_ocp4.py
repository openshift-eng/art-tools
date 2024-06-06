import json
import click
import os
import yaml
from artcommonlib import exectools
from artcommonlib.util import TreeAnalyzer
from pyartcd import constants
from pyartcd import util
from pyartcd.cli import cli, pass_runtime, click_coroutine
from pyartcd.runtime import Runtime


class Version:
    def __init__(self):
        self.stream = ""  # "X.Y" e.g. "4.0"
        self.branch = ""  # e.g. "rhaos-4.0-rhel-7"
        self.release = ""  # e.g. "201901011200.?"
        self.major = 0  # X in X.Y, e.g. 4
        self.minor = 0  # Y in X.Y, e.g. 0

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)


class KonfluxPipeline:
    def __init__(self, runtime, assembly, data_path, image_list, version, data_gitref):
        self.runtime = runtime
        self.image_list = image_list.split(",")
        self._doozer_working = os.path.abspath(f"{self.runtime.working_dir / 'doozer_working'}")
        self.version = Version()
        self.version.stream = version
        self.version.release = util.default_release_suffix()

        group_param = f"--group=openshift-{version}"
        if data_gitref:
            group_param += f"@{data_gitref}"

        self._doozer_base_command = [
            "doozer",
            f"--assembly={assembly}",
            f"--working-dir={self._doozer_working}",
            f"--data-path={data_path}",
            group_param
        ]

        self.tree_analyzer = TreeAnalyzer(self._get_tree())

    async def _rebase(self, images):
        cmd = self._doozer_base_command.copy()
        cmd.extend([
            "--latest-parent-version", "k:images:rebase", f"--images={','.join(images)}",
            f"--version=v{self.version.stream}", f"--release='{self.version.release}'",
            f"--message='Updating Dockerfile version and release v{self.version.stream}-{self.version.release}'",
        ])

        if self.runtime.dry_run:
            cmd.append("--dry-run")

        await exectools.cmd_assert_async(cmd)

    def _get_tree(self):
        """
        Get the parent-tree graph from ocp-build-data, of all images
        """
        cmd = self._doozer_base_command.copy()
        cmd.extend([
            "images:show-tree",
            "--yml"
        ])
        out, _ = exectools.cmd_assert(cmd)
        self.runtime.logger.info("images:show-tree output:\n%s", out)
        return yaml.safe_load(out)

    def _get_parents_children(self, images):
        """
        Get the list of parents and children of a node, including the node as well.
        """
        unique_images = set()

        for image in images:
            unique_images.add(image)
            parents, children = self.tree_analyzer.get_parents_and_children(image)
            for node in parents + children:
                unique_images.add(node)

        return unique_images

    async def run(self):
        # Get all the parents and children that we need. Combine them so that there are no duplicates.
        # This will be the new set of images to rebase
        parents_children = self._get_parents_children(self.image_list)

        await self._rebase(images=parents_children)


@cli.command("k:ocp4", help=" Konflux pipeline")
@click.option("--image-list", required=True, default="",
              help="Comma/space-separated list to include/exclude per BUILD_IMAGES "
                   "(e.g. logging-kibana5,openshift-jenkins-2)")
@click.option("--assembly", required=True, help="The name of an assembly to rebase & build for")
@click.option("--data-path", required=False, default=constants.OCP_BUILD_DATA_URL,
              help="ocp-build-data fork to use (e.g. assembly definition in your own fork)")
@click.option("--version", required=True, help="OCP version to scan, e.g. 4.14")
@click.option("--data-gitref", required=False, default="",
              help="Doozer data path git [branch / tag / sha] to use")
@pass_runtime
@click_coroutine
async def ocp4(runtime: Runtime, assembly, data_path, image_list, version, data_gitref):
    await KonfluxPipeline(runtime, assembly, data_path, image_list, version, data_gitref).run()
