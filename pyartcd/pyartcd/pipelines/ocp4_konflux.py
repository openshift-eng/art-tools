import json
import logging
import os
import shutil
from enum import Enum
from typing import Optional, Tuple

import click
import yaml

from artcommonlib import exectools

from pyartcd import constants, util, jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)


class BuildStrategy(Enum):
    ALL = 'all'
    ONLY = 'only'
    EXCEPT = 'except'

    def __str__(self):
        return self.value


class BuildPlan:
    def __init__(self, build_strategy=BuildStrategy.ALL):
        self.build_strategy = build_strategy  # build all images or a subset
        self.images_included = []  # include list for images to build
        self.images_excluded = []  # exclude list for images to build
        self.active_image_count = 0  # number of images active in this version

    def __str__(self):
        return json.dumps(self.__dict__, indent=4, cls=EnumEncoder)


class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


class KonfluxOcp4Pipeline:
    def __init__(self, runtime: Runtime, assembly: str, data_path: Optional[str], image_build_strategy: Optional[str],
                 image_list: Optional[str], version: str, data_gitref: Optional[str],
                 kubeconfig: Optional[str], skip_rebase: bool, arches: Tuple[str, ...]):
        self.runtime = runtime
        self.assembly = assembly
        self._doozer_working = os.path.abspath(f'{self.runtime.working_dir / "doozer_working"}')
        self.version = version
        self.kubeconfig = kubeconfig
        self.arches = arches
        self.skip_rebase = skip_rebase

        group_param = f'--group=openshift-{version}'
        if data_gitref:
            group_param += f'@{data_gitref}'

        self._doozer_base_command = [
            'doozer',
            f'--assembly={assembly}',
            f'--working-dir={self._doozer_working}',
            f'--data-path={data_path}',
            group_param
        ]

        self.build_plan = BuildPlan(BuildStrategy(image_build_strategy))
        self.image_list = [image.strip() for image in image_list.split(',')] if image_list else []

    def image_param_from_build_plan(self):
        if self.build_plan.build_strategy == BuildStrategy.ALL:
            image_param = ''  # Doozer runtime will consider all images
        elif self.build_plan.build_strategy == BuildStrategy.ONLY:
            image_param = f'--images={",".join(self.build_plan.images_included)}'
        else:
            image_param = f'--exclude={",".join(self.build_plan.images_excluded)}'
        return image_param

    async def rebase(self, version: str, input_release: str):
        cmd = self._doozer_base_command.copy()
        if self.arches:
            cmd.append("--arches")
            cmd.append(",".join(self.arches))

        cmd.extend([
            '--latest-parent-version',
            self.image_param_from_build_plan(),
            'beta:images:konflux:rebase',
            f'--version={version}',
            f'--release={input_release}',
            f"--message='Updating Dockerfile version and release {version}-{input_release}'",
        ])
        if not self.runtime.dry_run:
            cmd.append('--push')

        try:
            await exectools.cmd_assert_async(cmd)

        except ChildProcessError:
            with open(f'{self._doozer_working}/state.yaml') as state_yaml:
                state = yaml.safe_load(state_yaml)
            failed_images = state['images:konflux:rebase'].get('failed-images', [])
            if not failed_images:
                raise  # Something else went wrong
            LOGGER.warning('Following images failed to rebase and won\'t be built: %s', ','.join(failed_images))

            # Exclude images that failed to rebase from the build step
            if self.build_plan.build_strategy == BuildStrategy.ALL:
                # Move from building all to excluding failed images
                self.build_plan.build_strategy = BuildStrategy.EXCEPT
                self.build_plan.images_excluded = failed_images

            elif self.build_plan.build_strategy == BuildStrategy.ONLY:
                # Remove failed images from included ones
                self.build_plan.images_included = [i for i in self.build_plan.images_included if i not in failed_images]

            else:  # strategy = EXCLUDE
                # Append failed images to excluded ones
                self.build_plan.images_excluded.extend(failed_images)

    async def build(self):
        cmd = self._doozer_base_command.copy()
        if self.arches:
            cmd.append("--arches")
            cmd.append(",".join(self.arches))

        if self.build_plan.build_strategy == BuildStrategy.ONLY and not self.build_plan.images_included:
            LOGGER.warning('No images will be built')
            return

        cmd.extend([
            '--latest-parent-version',
            self.image_param_from_build_plan(),
            'beta:images:konflux:build',
            "--konflux-namespace=ocp-art-tenant",
        ])
        if self.kubeconfig:
            cmd.extend(['--konflux-kubeconfig', self.kubeconfig])
        if self.runtime.dry_run:
            cmd.append('--dry-run')
        await exectools.cmd_assert_async(cmd)

        LOGGER.info("All builds completed successfully")

    async def init_build_plan(self):
        # Get number of images in current group
        shutil.rmtree(self._doozer_working, ignore_errors=True)
        _, out, _ = await exectools.cmd_gather_async([*self._doozer_base_command.copy(), 'images:list'])
        # Last line looks like this: "219 images"
        self.build_plan.active_image_count = int(out.splitlines()[-1].split(' ')[0].strip())

        if self.build_plan.build_strategy == BuildStrategy.ALL:
            self.build_plan.images_included = []
            self.build_plan.images_excluded = []
            jenkins.update_title('[mass rebuild]')
            jenkins.update_description(f'Images: building {self.build_plan.active_image_count} images.<br/>')

        elif self.build_plan.build_strategy == BuildStrategy.ONLY:
            self.build_plan.images_included = self.image_list
            self.build_plan.images_excluded = []

            n_images = len(self.build_plan.images_included)
            if n_images == 1:
                jenkins.update_title(f'[{self.build_plan.images_included[0]}]')
            else:
                jenkins.update_title(f'[{n_images} images]')

            if n_images <= 10:
                jenkins.update_description(f'Images: building {", ".join(self.build_plan.images_included)}.<br/>')
            else:
                jenkins.update_description(f'Images: building {n_images} images.<br/>')

        else:  # build_plan.build_strategy == BuildStrategy.EXCEPT
            self.build_plan.images_included = []
            self.build_plan.images_excluded = self.image_list

            n_images = self.build_plan.active_image_count - len(self.build_plan.images_excluded)
            jenkins.update_title(f'[{n_images} images]')
            if len(self.build_plan.images_excluded) <= 10:
                jenkins.update_description(f'Images: building all images except '
                                           f'{",".join(self.build_plan.images_excluded)}.<br/>')
            else:
                jenkins.update_description(f'Images: building {n_images} images.<br/>')

        self.runtime.logger.info('Initial build plan:\n%s', self.build_plan)

    async def initialize(self):
        jenkins.init_jenkins()
        jenkins.update_title(f' - {self.version} ')
        await self.init_build_plan()

        if self.assembly.lower() == "test":
            jenkins.update_title(" [TEST]")

    async def run(self):
        await self.initialize()

        version = f"v{self.version}.0"
        input_release = util.default_release_suffix()

        if self.skip_rebase:
            LOGGER.warning("Skipping rebase step because --skip-rebase flag is set")
        else:
            LOGGER.info(f"Rebasing images for OCP {self.version} with release {input_release}")
            await self.rebase(version, input_release)

        LOGGER.info(f"Building images for OCP {self.version} with release {input_release}")
        await self.build()


@cli.command("beta:ocp4-konflux", help="A pipeline to build images with Konflux for OCP 4")
@click.option('--image-build-strategy', required=True,
              type=click.Choice(['all', 'only', 'except'], case_sensitive=False),
              help='Which images are candidates for building? "only/except" refer to the --image-list param')
@click.option('--image-list', required=True,
              help='Comma/space-separated list to include/exclude per BUILD_IMAGES '
                   '(e.g. logging-kibana5,openshift-jenkins-2)')
@click.option('--assembly', required=True, help='The name of an assembly to rebase & build for')
@click.option('--data-path', required=False, default=constants.OCP_BUILD_DATA_URL,
              help='ocp-build-data fork to use (e.g. assembly definition in your own fork)')
@click.option('--version', required=True, help='OCP version to scan, e.g. 4.14')
@click.option('--data-gitref', required=False, default='',
              help='Doozer data path git [branch / tag / sha] to use')
@click.option("--kubeconfig", required=False, help="Path to kubeconfig file to use for Konflux cluster connections")
@click.option('--ignore-locks', is_flag=True, default=False,
              help='Do not wait for other builds in this version to complete (use only if you know they will not conflict)')
@click.option("--skip-rebase", is_flag=True, help="(For testing) Skip the rebase step")
@click.option("--arch", "arches", metavar="TAG", multiple=True,
              help="(Optional) [MULTIPLE] Limit included arches to this list")
@pass_runtime
@click_coroutine
async def ocp4(runtime: Runtime, image_build_strategy: str, image_list: Optional[str], assembly: str,
               data_path: Optional[str], version: str, data_gitref: Optional[str], kubeconfig: Optional[str],
               ignore_locks: bool, skip_rebase: bool, arches: Tuple[str, ...]):
    if not kubeconfig:
        kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    lock_identifier = jenkins.get_build_path()
    if not lock_identifier:
        runtime.logger.warning('Env var BUILD_URL has not been defined: a random identifier will be used for the locks')

    pipeline = KonfluxOcp4Pipeline(
        runtime=runtime,
        assembly=assembly,
        data_path=data_path,
        image_build_strategy=image_build_strategy,
        image_list=image_list,
        version=version,
        data_gitref=data_gitref,
        kubeconfig=kubeconfig,
        skip_rebase=skip_rebase,
        arches=arches)

    if ignore_locks:
        await pipeline.run()
    else:
        await locks.run_with_lock(
            coro=pipeline.run(),
            lock=Lock.BUILD_KONFLUX,
            lock_name=Lock.BUILD_KONFLUX.value.format(version=version),
            lock_id=lock_identifier
        )
