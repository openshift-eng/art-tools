import json
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Optional

import click
import yaml
from artcommonlib import exectools
from artcommonlib.variants import BuildVariant
from doozerlib.cli.images_okd import OKD_DEFAULT_IMAGE_REPO
from doozerlib.state import STATE_PASS

from pyartcd import jenkins, locks
from pyartcd import record as record_util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import KONFLUX_IMAGE_BUILD_PLR_TEMPLATE_URL_FORMAT, OCP_BUILD_DATA_URL
from pyartcd.locks import Lock
from pyartcd.pipelines.ocp4_konflux import BuildStrategy, EnumEncoder
from pyartcd.runtime import Runtime
from pyartcd.util import default_release_suffix, get_group_images

LOGGER = logging.getLogger(__name__)

OKD_ARCHES = ['x86_64']


class BuildPlan:
    def __init__(self, image_build_strategy=BuildStrategy.ALL):
        self.image_build_strategy = image_build_strategy  # build all images or a subset
        self.images_included = []  # include list for images to build
        self.images_excluded = []  # exclude list for images to build
        self.active_image_count = 0  # number of images active in this version

    def __str__(self):
        return json.dumps(self.__dict__, indent=4, cls=EnumEncoder)


class KonfluxOkd4Pipeline:
    def __init__(
        self,
        runtime: Runtime,
        image_build_strategy: str,
        image_list: Optional[str],
        assembly: str,
        data_path: Optional[str],
        data_gitref: Optional[str],
        version: str,
        ignore_locks: bool,
        plr_template: str,
        lock_identifier: Optional[str],
        build_priority: Optional[str],
        imagestream_namespace: str,
    ):
        self.runtime = runtime
        self.image_build_strategy = image_build_strategy
        self.image_list = [image.strip() for image in image_list.split(',')] if image_list else []
        self.assembly = assembly
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.version = version
        self.release = default_release_suffix()
        self.konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')
        self.ignore_locks = ignore_locks
        self.plr_template = plr_template
        self.lock_identifier = lock_identifier
        self.build_priority = build_priority
        self.imagestream_namespace = imagestream_namespace

        self.group_images = []
        self.build_plan = BuildPlan(BuildStrategy(image_build_strategy))
        self.slack_client = runtime.new_slack_client()

        self.built_images = []

        group_param = f'--group=openshift-{version}'
        if data_gitref:
            group_param += f'@{data_gitref}'
        self._doozer_base_command = [
            'doozer',
            f'--assembly={assembly}',
            f'--working-dir={self.runtime.doozer_working}',
            f'--data-path={data_path}',
            '--build-system=konflux',
            f'--arches={",".join(OKD_ARCHES)}',
            group_param,
        ]

    async def run(self):
        await self.initialize()
        await self.rebase_and_build_images()
        await self.update_imagestreams()
        self.finalize()

    async def initialize(self):
        jenkins.init_jenkins()
        jenkins.update_title(f' - {self.version} ')
        if self.assembly.lower() == "test":
            jenkins.update_title(" [TEST]")

        await self.init_build_plan()
        self.slack_client.bind_channel(f'openshift-{self.version}')

    async def init_build_plan(self):
        # Get number of images in current group
        shutil.rmtree(self.runtime.doozer_working, ignore_errors=True)
        self.group_images = await get_group_images(
            group=f'openshift-{self.version}',
            assembly=self.assembly,
            build_system='konflux',
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )
        self.build_plan.active_image_count = len(self.group_images)

        # Set the image list based on the build strategies
        self.check_building_images()

        # Log the initial build plan
        LOGGER.info('Initial build plan:\n%s', self.build_plan)

    def check_building_images(self):
        if self.build_plan.image_build_strategy == BuildStrategy.NONE:
            jenkins.update_title('[NO IMAGES]')

        elif self.build_plan.image_build_strategy == BuildStrategy.ALL:
            self.build_plan.images_included = []
            self.build_plan.images_excluded = []
            jenkins.update_description(f'Building {self.build_plan.active_image_count} images.<br>')
            jenkins.update_title(f'[{self.build_plan.active_image_count}] images')

        elif self.build_plan.image_build_strategy == BuildStrategy.ONLY:
            self.build_plan.images_included = self.image_list
            self.build_plan.images_excluded = []

            n_images = len(self.build_plan.images_included)
            if n_images == 1:
                jenkins.update_title(f'[{self.build_plan.images_included[0]}]')
            else:
                jenkins.update_title(f'[{n_images} images]')

            if n_images <= 10:
                jenkins.update_description(f'Building images {", ".join(self.build_plan.images_included)}.<br>')
            else:
                jenkins.update_description(f'Building {n_images} images.<br>')

        else:  # build_plan.build_strategy == BuildStrategy.EXCEPT
            self.build_plan.images_included = []
            self.build_plan.images_excluded = self.image_list

            n_images = self.build_plan.active_image_count - len(self.build_plan.images_excluded)
            jenkins.update_title(f'[{n_images} images]')
            if len(self.build_plan.images_excluded) <= 10:
                jenkins.update_description(
                    f'Building all images except {",".join(self.build_plan.images_excluded)}.<br>'
                )
            else:
                jenkins.update_description(f'Building {n_images} images.<br>')

    async def rebase_and_build_images(self):
        await self.rebase_images(f"v{self.version}.0", self.release)
        await self.build_images()

    async def rebase_images(self, version: str, input_release: str):
        # If no images are being built, skip the rebase step
        if not self.building_images():
            LOGGER.warning('No images will be rebased')
            return

        LOGGER.info(f"Rebasing images for OCP {self.version} with release {self.release}")

        cmd = self._doozer_base_command.copy()
        cmd.append('--latest-parent-version')
        cmd.extend(self.include_exclude_param())
        cmd.extend(
            [
                'images:okd',
                'rebase',
                f'--version={version}',
                f'--release={input_release}',
                f"--message='Updating Dockerfile version and release {version}-{input_release}'",
                f'--image-repo={OKD_DEFAULT_IMAGE_REPO}',
            ]
        )

        if not self.runtime.dry_run:
            cmd.append('--push')

        try:
            await exectools.cmd_assert_async(cmd)

        except ChildProcessError:
            pass

        finally:
            self.handle_rebase_failures()

    def handle_rebase_failures(self):
        state = self.load_state_yaml()

        # Some images failed to rebase: log them, and track them in Redis
        rebase_failures = [image for image, state in state['images:okd:rebase']['images'].items() if state == 'failure']

        if rebase_failures:
            LOGGER.warning(f'Following images failed to rebase and won\'t be built: {",".join(rebase_failures)}')
            jenkins.update_description(f'Rebase failures: {", ".join(rebase_failures)}<br>')

        # OKD disabled images have not been rebased and must not be built
        skipped_images = [image for image, state in state['images:okd:rebase']['images'].items() if state == 'skipped']
        if skipped_images:
            LOGGER.warning(
                f'Following images are disabled in OKD and have not been rebased: {",".join(skipped_images)}'
            )
            jenkins.update_description(f'Skipped images: {", ".join(skipped_images)}<br>')

        # Exclude images that were skipped or failed during rebase from the build step
        if self.build_plan.image_build_strategy == BuildStrategy.ALL:
            # Move from building all to excluding failed images
            self.build_plan.image_build_strategy = BuildStrategy.EXCEPT
            self.build_plan.images_excluded = rebase_failures + skipped_images

        elif self.build_plan.image_build_strategy == BuildStrategy.ONLY:
            # Remove failed images from included ones
            self.build_plan.images_included = [
                i for i in self.build_plan.images_included if i not in rebase_failures + skipped_images
            ]

        else:  # strategy = EXCLUDE
            # Append failed images to excluded ones
            self.build_plan.images_excluded.extend(rebase_failures + skipped_images)

    async def build_images(self):
        if not self.building_images():
            LOGGER.warning('No images will be built')
            return

        LOGGER.info(f'Building images for OCP {self.version} with release {self.release}')

        cmd = self._doozer_base_command.copy()

        cmd.append('--latest-parent-version')
        cmd.extend(self.include_exclude_param())
        cmd.extend(
            [
                'beta:images:konflux:build',
                f'--variant={BuildVariant.OKD.value}',
                '--network-mode=open',
                '--konflux-namespace=ocp-art-tenant',
                f'--image-repo={OKD_DEFAULT_IMAGE_REPO}',
            ]
        )

        if self.konflux_kubeconfig:
            cmd.extend(['--konflux-kubeconfig', self.konflux_kubeconfig])

        if self.plr_template:
            plr_template_owner, plr_template_branch = (
                self.plr_template.split('@') if self.plr_template else ['openshift-priv', 'main']
            )
            plr_template_url = KONFLUX_IMAGE_BUILD_PLR_TEMPLATE_URL_FORMAT.format(
                owner=plr_template_owner, branch_name=plr_template_branch
            )
            cmd.extend(['--plr-template', plr_template_url])

        if self.runtime.dry_run:
            cmd.append('--dry-run')

        # Add build priority. Can be a str between "1" (highest priority) - "10" or "auto"
        LOGGER.info(f"Using build priority: {self.build_priority}")
        cmd.extend(['--build-priority', self.build_priority])

        LOGGER.info('Running command: %s', ' '.join(cmd))

        try:
            await exectools.cmd_assert_async(cmd)

        except ChildProcessError:
            pass

        finally:
            self.handle_built_images()

    def handle_built_images(self):
        record_log = self.parse_record_log()
        if not record_log:
            LOGGER.error('record.log not found!')
            return

        self.built_images = [
            {
                'name': entry['name'],
                'nvr': entry['nvrs'],
                'image_pullspec': entry.get('image_pullspec'),
                'image_tag': entry.get('image_tag'),
            }
            for entry in record_log.get('image_build_okd', [])
            if not int(entry['status'])
        ]
        if self.built_images:
            LOGGER.info('Built images: %s', self.built_images)
            jenkins.update_description(f'Built images: {",".join([image["name"] for image in self.built_images])}<br>')

        failed_images = [entry['name'] for entry in record_log.get('image_build_okd', []) if int(entry['status'])]
        if failed_images:
            jenkins.update_description(f'Build failures: {", ".join(failed_images)}<br>')

    async def update_imagestreams(self):
        """
        Update static OKD imagestream with successfully built images:
        scos-{version}-art - accumulates all successfully built images
        """

        if self.assembly != 'stream':
            LOGGER.info('Assembly is not "stream"; skipping imagestream updates')
            return

        if not self.built_images:
            LOGGER.warning('No images were successfully built; skipping imagestream updates')
            return

        if self.runtime.dry_run:
            LOGGER.info('[DRY RUN] Would update imagestreams in namespace %s', self.imagestream_namespace)
            LOGGER.info('[DRY RUN] Would tag %d images', len(self.built_images))
            return

        is_name = f'scos-{self.version}-art'
        env = os.environ.copy()
        successful_tags = []
        failed_tags = []

        # Tag each newly built image into the imagestream
        LOGGER.info('Updating imagestream: %s in namespace %s', is_name, self.imagestream_namespace)
        for image in self.built_images:
            image_name = image['name']
            image_pullspec = image.get('image_pullspec')
            image_tag = image.get('image_tag')

            if not image_pullspec or not image_tag:
                LOGGER.warning('Image %s missing pullspec or tag; skipping', image_name)
                failed_tags.append(image_name)
                continue

            # Tag into imagestream
            target = f'{self.imagestream_namespace}/{is_name}:{image_name}'
            try:
                await self._tag_image_to_stream(source_pullspec=image_pullspec, target_tag=target, env=env)
                LOGGER.info('Tagged %s into %s', image_name, target)
                successful_tags.append(image_name)
            except Exception as e:
                LOGGER.warning('Failed to tag %s into imagestream: %s', image_name, e)
                failed_tags.append(image_name)

        # Update Jenkins description with results
        if successful_tags:
            success_msg = f'Updated {is_name} with {len(successful_tags)} images'
            jenkins.update_description(f'{success_msg}<br>')
            LOGGER.info(success_msg)

        if failed_tags:
            failure_msg = f'Imagestream update failures: {", ".join(failed_tags)}'
            jenkins.update_description(f'{failure_msg}<br>')
            LOGGER.warning(failure_msg)

    async def _tag_image_to_stream(self, source_pullspec: str, target_tag: str, env: dict):
        """
        Helper method to tag an image into an imagestream.

        Arg(s):
            source_pullspec (str): Full pullspec of source image
            target_tag (str): Target in format 'namespace/imagestream:tag'
            env (dict): Environment variables for oc command
        """
        cmd = [
            'oc',
            'tag',
            '--import-mode=PreserveOriginal',
            '--',
            source_pullspec,
            target_tag,
        ]

        LOGGER.debug('Running: %s', ' '.join(cmd))
        await exectools.cmd_assert_async(cmd, env=env, stdout=sys.stderr)

    def parse_record_log(self) -> Optional[dict]:
        record_log_path = Path(self.runtime.doozer_working, 'record.log')
        if not record_log_path.exists():
            return None

        with record_log_path.open('r') as file:
            record_log: dict = record_util.parse_record_log(file)
            return record_log

    def building_images(self):
        """
        Returns True if images are being built, False otherwise.
        """

        # If the build strategy is NONE, no images will be built
        if self.build_plan.image_build_strategy == BuildStrategy.NONE:
            return False

        # If the build strategy is ONLY but no images are included, no images will be built
        if self.build_plan.image_build_strategy == BuildStrategy.ONLY and not self.build_plan.images_included:
            return False

        # If the build strategy is EXCEPT but no images are excluded,
        # or if the build strategy is ALL, all images will be built
        return True

    def include_exclude_param(self):
        """
        Returns the include/exclude parameters for the Doozer command based on the image build strategy.
        """

        build_strategy = self.build_plan.image_build_strategy
        includes = self.build_plan.images_included
        excludes = self.build_plan.images_excluded

        if build_strategy == BuildStrategy.ALL:
            return []

        elif build_strategy == BuildStrategy.ONLY:
            return [f'--images={",".join(includes)}']

        elif build_strategy == BuildStrategy.EXCEPT:
            return ['--images=', f'--exclude={",".join(excludes)}']

        else:  # BuildStrategy.NONE
            raise ValueError(f'Invalid build strategy: {build_strategy}')

    def finalize(self):
        state = self.load_state_yaml()
        if state.get('status') != STATE_PASS:
            sys.exit(1)

    def load_state_yaml(self) -> dict:
        with open(f'{self.runtime.doozer_working}/state.yaml') as state_yaml:
            return yaml.safe_load(state_yaml)


@cli.command("okd4", help="A pipeline to build images with Konflux for OCP 4")
@click.option(
    '--image-build-strategy',
    required=True,
    type=click.Choice(['all', 'none', 'only', 'except'], case_sensitive=False),
    help='Which images are candidates for building? "only/except" refer to the --image-list param',
)
@click.option(
    '--image-list',
    required=True,
    help='Comma/space-separated list to include/exclude per --image-build-strategy (e.g. logging-kibana5,openshift-jenkins-2)',
)
@click.option('--assembly', required=True, help='The name of an assembly to rebase & build for')
@click.option(
    '--data-path',
    required=False,
    default=OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option('--version', required=True, help='OCP version to build, e.g. 4.21')
@click.option(
    '--ignore-locks',
    is_flag=True,
    default=False,
    help='Do not wait for other builds in this version to complete (use only if you know they will not conflict)',
)
@click.option(
    '--plr-template',
    required=False,
    default='',
    help='Override the Pipeline Run template commit from openshift-priv/art-konflux-template; format: <owner>@<branch>',
)
@click.option(
    '--build-priority',
    type=str,
    metavar='PRIORITY',
    default='10',
    required=False,
    help='Kueue build priority. Use "auto" for automatic resolution from image/group config, or specify a number 1-10 (where 1 is highest priority). Takes precedence over group and image config settings.',
)
@click.option(
    '--imagestream-namespace',
    required=False,
    default='ocp',
    help='Namespace for OKD imagestream updates (default: ocp)',
)
@pass_runtime
@click_coroutine
async def okd4(
    runtime: Runtime,
    image_build_strategy: str,
    image_list: Optional[str],
    assembly: str,
    data_path: Optional[str],
    data_gitref: Optional[str],
    version: str,
    ignore_locks: bool,
    plr_template: str,
    build_priority: Optional[str],
    imagestream_namespace: str,
):
    lock_identifier = jenkins.get_build_path()
    if not lock_identifier:
        runtime.logger.warning('Env var BUILD_URL has not been defined: a random identifier will be used for the locks')

    pipeline = KonfluxOkd4Pipeline(
        runtime=runtime,
        image_build_strategy=image_build_strategy,
        image_list=image_list,
        assembly=assembly,
        data_path=data_path,
        data_gitref=data_gitref,
        version=version,
        ignore_locks=ignore_locks,
        plr_template=plr_template,
        lock_identifier=lock_identifier,
        build_priority=build_priority,
        imagestream_namespace=imagestream_namespace,
    )

    if ignore_locks:
        await pipeline.run()
    else:
        await locks.run_with_lock(
            coro=pipeline.run(),
            lock=Lock.BUILD_OKD4,
            lock_name=Lock.BUILD_OKD4.value.format(version=version),
            lock_id=lock_identifier,
        )
