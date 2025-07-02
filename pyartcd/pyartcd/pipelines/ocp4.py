import asyncio
import json
import os.path
import shutil

import click
import yaml
from artcommonlib import exectools, redis
from artcommonlib.constants import KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS

from pyartcd import constants, jenkins, locks, oc, util
from pyartcd import record as record_util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime
from pyartcd.util import get_group_images, increment_rebase_fail_counter, mass_rebuild_score, reset_rebase_fail_counter


class BuildPlan:
    def __init__(self):
        self.active_image_count = 1  # number of images active in this version
        self.dry_run = False  # report build plan without performing it
        self.build_rpms = False  # should we build rpms
        self.rpms_included = []  # include list for rpms to build
        self.rpms_excluded = []  # exclude list for rpms to build
        self.build_images = False  # should we build images
        self.images_included = []  # include list for images to build
        self.images_excluded = []  # exclude list for images to build

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)


class Ocp4Pipeline:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        assembly: str,
        data_path: str,
        data_gitref: str,
        build_rpms: str,
        rpm_list: str,
        build_images: str,
        image_list: str,
        skip_plashets: bool,
        mail_list_failure: str,
        comment_on_pr: bool,
        lock_identifier: str = None,
    ):
        self.runtime = runtime
        self.assembly = assembly
        self.build_rpms = build_rpms
        self.rpm_list = rpm_list
        self.build_images = build_images
        self.image_list = image_list
        self.skip_plashets = skip_plashets
        self.mail_list_failure = mail_list_failure

        self.build_plan = BuildPlan()
        self.mass_rebuild = False
        self.group_images = []

        self.version = version
        self.release = util.default_release_suffix()

        self.all_image_build_failed = False
        self.comment_on_pr = comment_on_pr

        self.data_path = data_path
        self.data_gitref = data_gitref
        self.success_nvrs = []

        # If BUILD_URL env var is not set, the lock identifier will be None, and we'll get an auto-generated value
        self.lock_identifier = lock_identifier

        group_param = f'--group=openshift-{version}'
        if data_gitref:
            group_param += f'@{data_gitref}'

        self._doozer_base_command = [
            'doozer',
            f'--assembly={assembly}',
            f'--working-dir={self.runtime.doozer_working}',
            f'--data-path={data_path}',
            group_param,
        ]
        self._slack_client = runtime.new_slack_client()
        self._mail_client = self.runtime.new_mail_client()

    async def _check_assembly(self):
        """
        If assembly != 'stream' and assemblies not enabled for <version>, raise an error
        """

        shutil.rmtree(self.runtime.doozer_working, ignore_errors=True)
        cmd = self._doozer_base_command.copy()
        cmd.extend(['config:read-group', '--default=False', 'assemblies.enabled'])
        _, out, err = await exectools.cmd_gather_async(cmd)
        assemblies_enabled = out.strip() == 'True'
        self.runtime.logger.info('Assemblies %s enabled for %s', 'NOT' if not assemblies_enabled else '', self.version)

        if self.assembly != 'stream' and not assemblies_enabled:
            raise RuntimeError(
                f"ASSEMBLY cannot be set to '{self.assembly}' because assemblies are not enabled in ocp-build-data."
            )

        if self.assembly.lower() == "test":
            jenkins.update_title(" [TEST]")

    async def _initialize_build_plan(self):
        """
        Initialize the "build_plan" data structure based on job parameters
        """

        # build_plan.active_image_count
        self.group_images = await get_group_images(
            group=f'openshift-{self.version}',
            assembly=self.assembly,
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )
        self.build_plan.active_image_count = len(self.group_images)

        # build_plan.dry_run
        self.build_plan.dry_run = self.runtime.dry_run

        # build_plan.build_rpms, build_plan.rpms_included, build_plan.rpms_excluded
        self.build_plan.build_rpms = True

        if self.build_rpms.lower() == 'none':
            self.build_plan.build_rpms = False

        elif self.build_rpms.lower() == 'all':
            assert not self.rpm_list, (
                'Aborting because a list of RPMs was specified; you probably want to specify only/except.'
            )

        elif self.build_rpms.lower() == 'only':
            assert self.rpm_list, 'A list of RPMs must be specified when "only" is selected'
            self.build_plan.rpms_included = [rpm.strip() for rpm in self.rpm_list.split(',')]

        elif self.build_rpms.lower() == 'except':
            assert self.rpm_list, 'A list of RPMs must be specified when "except" is selected'
            self.build_plan.rpms_excluded = [rpm.strip() for rpm in self.rpm_list.split(',')]

        # build_plan.build_images, build_plan.images_included, build_plan.images_excluded
        self.build_plan.build_images = True

        if self.build_images.lower() == 'none':
            self.build_plan.build_images = False

        elif self.build_images.lower() == 'all':
            assert not self.image_list, (
                'Aborting because a list of RPMs was specified; you probably want to specify only/except.'
            )

        elif self.build_images.lower() == 'only':
            assert self.image_list, 'A list of images must be specified when "only" is selected'
            self.build_plan.images_included = [image.strip() for image in self.image_list.split(',')]

        elif self.build_images.lower() == 'except':
            assert self.image_list, 'A list of images must be specified when "except" is selected'
            self.build_plan.images_excluded = [image.strip() for image in self.image_list.split(',')]

        self.check_mass_rebuild()

        self.runtime.logger.info('Initial build plan:\n%s', self.build_plan)

    def check_mass_rebuild(self):
        # Account for mass rebuilds:
        # If build plan includes more than half or excludes less than half or rebuilds everything, it's a mass rebuild
        include_count = len(self.build_plan.images_included)
        exclude_count = len(self.build_plan.images_excluded)
        self.mass_rebuild = (
            (include_count and self.build_plan.active_image_count < include_count * 2)
            or (exclude_count and self.build_plan.active_image_count > exclude_count * 2)
            or (not include_count and not exclude_count)
        )

    async def _is_build_permitted(self):
        # For assembly != 'stream' always permit ocp4 builds
        if self.assembly != 'stream':
            self.runtime.logger.info('Permitting build for assembly %s', self.assembly)
            return True

        # For assembly = 'stream', we need to check automation freeze state
        return await util.is_build_permitted(
            version=self.version,
            data_path=self.data_path,
            doozer_working=self.runtime.doozer_working,
            doozer_data_gitref=self.data_gitref,
        )

    async def _initialize(self):
        self.runtime.logger.info('Initializing build:\n%s', str(self.version))

        jenkins.init_jenkins()

        if not await self._is_build_permitted():
            jenkins.update_description('Builds not permitted', append=False)
            raise RuntimeError(
                'This build is being terminated because it is not permitted according to current group.yml'
            )

        await self._check_assembly()
        await self._initialize_build_plan()
        self._update_title_and_description()

    def _update_title_and_description(self):
        """
        Update Jenkins build title and description according to the build content.
        """

        title_update = " [dry-run]" if self.runtime.dry_run else ""
        title_update += f' - {self.version}-{self.release} '
        jenkins.update_title(title_update)

        jenkins.update_description('Pinned builds (whether source changed or not).<br/>')
        self.runtime.logger.info('Pinned builds (whether source changed or not)')

        if not self.build_plan.build_rpms:
            jenkins.update_description('RPMs: not building.<br/>')

        elif self.build_plan.rpms_included:
            jenkins.update_description(f'RPMs: building {self.build_plan.rpms_included}.<br/>')

        elif self.build_plan.rpms_excluded:
            jenkins.update_description(f'RPMs: building all except {self.build_plan.rpms_excluded}.<br/>')

        else:
            jenkins.update_description('RPMs: building all.<br/>')

        if self.build_plan.rpms_included:
            jenkins.update_title(self._display_tag_for(self.build_plan.rpms_included, 'RPM'))

        elif self.build_plan.rpms_excluded:
            jenkins.update_title(self._display_tag_for(self.build_plan.rpms_excluded, 'RPM', is_excluded=True))

        elif self.build_plan.build_rpms:
            jenkins.update_title(' [all RPMs]')

        jenkins.update_description('Will create RPM compose.<br/>')

        if not self.build_plan.build_images:
            jenkins.update_description('Images: not building.<br/>')

        elif self.mass_rebuild:
            jenkins.update_title(' [mass rebuild]')
            jenkins.update_description('Mass image rebuild (more than half) - invoking serializing semaphore<br/>')

        elif self.build_plan.images_included:
            images_to_build = len(self.build_plan.images_included)
            if images_to_build <= 10:
                jenkins.update_description(f'Images: building {self.build_plan.images_included}.<br/>')
            else:
                jenkins.update_description(f'Images: building {images_to_build} images.<br/>')

        elif self.build_plan.images_excluded:
            jenkins.update_description(f'Images: building all except {self.build_plan.images_excluded}.<br/>')

        else:
            jenkins.update_description('Images: building all.<br/>')

        if self.build_plan.images_included:
            jenkins.update_title(self._display_tag_for(self.build_plan.images_included, 'image'))

        elif self.build_plan.images_excluded:
            jenkins.update_title(self._display_tag_for(self.build_plan.images_excluded, 'image', is_excluded=True))

        elif self.build_plan.build_images:
            jenkins.update_title(' [all images]')

    def _report(self, msg: str):
        """
        Logs the message and appends it to current job description
        """
        self.runtime.logger.info(msg)
        jenkins.update_description(f'{msg}<br/>')

    @staticmethod
    def _include_exclude(kind: str, includes: list, excludes: list) -> list:
        """
        Determine what doozer parameter (if any) to use for includes/excludes
        --latest-parent-version only applies for images but won't hurt for RPMs
        """

        if kind not in ['rpms', 'images']:
            raise ValueError('Kind must be one in ["rpms", "images"]')

        if includes:
            return ['--latest-parent-version', f'--{kind}', ','.join(includes)]

        if excludes:
            return ['--latest-parent-version', f"--{kind}=", '--exclude', ','.join(excludes)]

        return [f'--{kind}=']

    @staticmethod
    def _display_tag_for(items: list, kind: str, is_excluded: bool = False):
        desc = items[0] if len(items) == 1 else len(items)
        plurality = kind if len(items) == 1 else f'{kind}s'

        if is_excluded:
            return f' [{kind}s except {desc}]'
        return f' [{desc} {plurality}]'

    async def _rebase_and_build_rpms(self):
        if not self.build_plan.build_rpms:
            self.runtime.logger.info('Not building RPMs.')
            return

        cmd = self._doozer_base_command.copy()
        cmd.extend(self._include_exclude('rpms', self.build_plan.rpms_included, self.build_plan.rpms_excluded))
        cmd.extend(
            [
                'rpms:rebase-and-build',
                f'--version={self.version}',
                f'--release={self.release}',
            ]
        )

        if self.runtime.dry_run:
            self.runtime.logger.info('Would have run: %s', ' '.join(cmd))
            return

        # Build RPMs
        self.runtime.logger.info('Building RPMs')
        try:
            await exectools.cmd_assert_async(cmd)
        except ChildProcessError:
            self._handle_rpm_build_failures()

        try:
            with open(f'{self.runtime.doozer_working}/record.log', 'r') as file:
                record_log: dict = record_util.parse_record_log(file)

            success_map = record_util.get_successful_rpms(record_log, full_record=True)

            # Kick off SAST scans for builds that succeeded using the NVR
            # Gives a comma separated list of NVRs, the filter-lambda function will handle the case of nvrs filed not found
            successful_rpm_nvrs = list(filter(lambda x: x, [build.get("nvrs") for build in success_map.values()]))

            if successful_rpm_nvrs:
                self.success_nvrs += successful_rpm_nvrs
        except Exception as e:
            self.runtime.logger.error(f"Failed to get successfully build RPM NVRs: {e}")

    def _handle_rpm_build_failures(self):
        with open(f'{self.runtime.doozer_working}/record.log', 'r') as file:
            record_log: dict = record_util.parse_record_log(file)

        failed_map = record_util.get_failed_rpms(record_log)
        if not failed_map:
            # failed so badly we don't know what failed; give up
            raise
        failed_rpms = list(failed_map.keys())
        jenkins.update_description(f'Failed rpms: {", ".join(failed_rpms)}<br/>')

        self.runtime.logger.warning('Failed rpms: %s', ', '.join(failed_rpms))

    async def _is_compose_build_permitted(self) -> bool:
        """
        If automation is not frozen, go ahead
        If automation is "scheduled", job was triggered by hand and there were no RPMs in the build plan: return False
        If automation is "scheduled", job was triggered by hand and there were RPMs in the build plan: return True
        """

        automation_state: str = await util.get_freeze_automation(
            version=self.version,
            doozer_data_path=self.data_path,
            doozer_working=self.runtime.doozer_working,
            doozer_data_gitref=self.data_gitref,
        )
        self.runtime.logger.info('Automation freeze for %s: %s', self.version, automation_state)

        if automation_state not in ['scheduled', 'yes', 'True']:
            return True

        if automation_state == 'scheduled' and self.build_plan.build_rpms and util.is_manual_build():
            # Send a Slack notification since we're running compose build during automation freeze
            self._slack_client.bind_channel(f'openshift-{self.version}')
            await self._slack_client.say(
                "*:alert: ocp4 build compose running during automation freeze*\n"
                "There were RPMs in the build plan that forced build compose during automation freeze.",
            )

            return True

        return False

    async def _rebase_images(self):
        self.runtime.logger.info('Rebasing images')

        cmd = self._doozer_base_command.copy()
        cmd.extend(self._include_exclude('images', self.build_plan.images_included, self.build_plan.images_excluded))
        cmd.extend(
            [
                'images:rebase',
                f'--version=v{self.version}',
                f'--release={self.release}',
                f"--message='Updating Dockerfile version and release v{self.version}-{self.release}'",
                '--push',
                f"--message='{os.environ['BUILD_URL']}'",
            ]
        )

        if self.runtime.dry_run:
            self.runtime.logger.info('Would have run: %s', ' '.join(cmd))
            return

        try:
            await exectools.cmd_assert_async(cmd)
            await self.update_rebase_fail_counters([])

        except ChildProcessError:
            # Get a list of images that failed to rebase
            with open(f'{self.runtime.doozer_working}/state.yaml') as state_yaml:
                state = yaml.safe_load(state_yaml)
            failed_images = list(
                dict(
                    filter(lambda item: item[1] is not True, state['images:rebase']['images'].items()),
                ).keys(),
            )

            if not failed_images:
                raise  # something else went wrong

            # Some images failed to rebase: log them, and track them in Redis
            self.runtime.logger.warning(
                'Following images failed to rebase and won\'t be built: %s', ', '.join(failed_images)
            )
            await self.update_rebase_fail_counters(failed_images)

            # Remove failed images from build plan
            self.build_plan.images_included = [i for i in self.build_plan.images_included if i not in failed_images]
            if not self.build_plan.images_included and not self.build_plan.images_excluded:
                self.build_plan.build_images = False

        util.notify_dockerfile_reconciliations(
            version=self.version,
            doozer_working=self.runtime.doozer_working,
            mail_client=self._mail_client,
        )

        # TODO: if a non-required rebase fails, notify ART and the image owners
        util.notify_bz_info_missing(
            version=self.version,
            doozer_working=self.runtime.doozer_working,
            mail_client=self._mail_client,
        )

    async def update_rebase_fail_counters(self, failed_images):
        """
        Update rebase fail counters for images that failed to rebase.
        """

        # Reset fail counters for images that were rebased successfully
        successful_images = []
        if self.build_images.lower() == 'all':
            successful_images = [image for image in self.group_images if image not in failed_images]
        elif self.build_images.lower() == 'except':
            successful_images = [
                image
                for image in self.group_images
                if image not in self.build_plan.images_excluded and image not in failed_images
            ]
        elif self.build_images.lower() == 'only':
            successful_images = [image for image in self.build_plan.images_included if image not in failed_images]
        await asyncio.gather(*[reset_rebase_fail_counter(image, self.version, 'brew') for image in successful_images])

        # Increment fail counters for failing images.
        await asyncio.gather(*[increment_rebase_fail_counter(image, self.version, 'brew') for image in failed_images])

    def _handle_image_build_failures(self):
        with open(f'{self.runtime.doozer_working}/record.log', 'r') as file:
            record_log: dict = record_util.parse_record_log(file)

        failed_map = record_util.get_failed_builds(record_log, full_record=True)
        if not failed_map:
            # failed so badly we don't know what failed; give up
            raise
        failed_images = list(failed_map.keys())

        if len(failed_images) <= 10:
            jenkins.update_description(f'Failed images: {", ".join(failed_images)}<br/>')
        else:
            jenkins.update_description(f'{len(failed_images)} images failed. Check record.log for details<br/>')

        self.runtime.logger.warning('Failed images: %s', ', '.join(failed_images))

        ratio = record_util.determine_build_failure_ratio(record_log)
        if ratio['failed'] == ratio['total']:
            self.all_image_build_failed = True
            failed_messages = ''
            for i in range(len(failed_images)):
                failed_messages += f"{failed_images[i]}:{failed_map[failed_images[i]]['task_url']}\n"

        if (ratio['total'] > 10 and ratio['ratio'] > 0.25) or (
            ratio['ratio'] > 1 and ratio['failed'] == ratio['total']
        ):
            self.runtime.logger.warning(
                "%s of %s image builds failed; probably not the owners' fault, will not spam",
                {ratio['failed']},
                {ratio['total']},
            )

        else:
            util.mail_build_failure_owners(
                failed_builds=failed_map,
                doozer_working=self.runtime.doozer_working,
                mail_client=self._mail_client,
                default_owner=self.mail_list_failure,
            )

    async def _build_images(self):
        # Even if the computed build plan included images, some may have been removed because they failed to rebase.
        # Check once again if the current build plan includes any image, return otherwise
        if not self.build_plan.build_images and self.build_images.lower() != 'all':
            self.runtime.logger.warning('No images to build according to current build plan')
            return

        self.runtime.logger.info('Building images')

        signing_mode = await util.get_signing_mode(
            group=f'openshift-{self.version}',
            assembly=self.assembly,
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )

        # Doozer command
        cmd = self._doozer_base_command.copy()
        cmd.extend(self._include_exclude('images', self.build_plan.images_included, self.build_plan.images_excluded))
        cmd.extend(['images:build', '--repo-type', signing_mode])

        if self.comment_on_pr:
            cmd.extend(['--comment-on-pr'])

        if self.runtime.dry_run:
            self.runtime.logger.info('Would have executed: %s', ' '.join(cmd))
            return

        # Build images. If more than one version is undergoing mass rebuilds,
        # serialize them to prevent flooding the queue
        try:
            await exectools.cmd_assert_async(cmd)

        except ChildProcessError:
            self._handle_image_build_failures()

        # If the API server builds, we mirror out the streams to CI. If ART builds a bad golang builder image it will
        # break CI builds for most upstream components if we don't catch it before we push. So we use apiserver as
        # bellweather to make sure that the current builder image is good enough. We can still break CI (e.g. pushing a
        # bad ruby-25 image along with this push, but it will not be a catastrophic event like breaking the apiserver.
        with open(f'{self.runtime.doozer_working}/record.log', 'r') as file:
            record_log: dict = record_util.parse_record_log(file)

        success_map = record_util.get_successful_builds(record_log, full_record=True)
        if success_map.get('ose-openshift-apiserver', None):
            self.runtime.logger.warning('apiserver rebuilt: mirroring streams to CI...')

            # Make sure our api.ci token is fresh
            await oc.registry_login(self.runtime)
            cmd = self._doozer_base_command.copy()
            cmd.extend(['images:streams', 'mirror'])
            await exectools.cmd_assert_async(cmd)

        # Kick off SAST scans for builds that succeeded using the NVR
        # Gives a comma separated list of NVRs, the filter-lambda function will handle the case of nvrs filed not found
        successful_build_nvrs = list(filter(lambda x: x, [build.get("nvrs") for build in success_map.values()]))

        if successful_build_nvrs:
            self.success_nvrs += successful_build_nvrs

    async def _rebase_and_build_images(self):
        if not self.build_plan.build_images:
            return  # to facilitate testing

        if self.mass_rebuild:
            await self._slack_client.say(
                f':construction: Starting image builds for {self.version} mass rebuild :construction:'
            )

        # In case of mass rebuilds, rebase and build should happend within the same lock scope
        # Otherwise we might rebase, then get blocked on the mass rebuild lock
        # As a consequence, we might be building outdated stuff
        await self._rebase_images()
        await self._build_images()

        if self.mass_rebuild:
            await self._slack_client.say(f'::done_it_is: Mass rebuild for {self.version} complete :done_it_is:')

    async def _sync_images(self):
        """
        Run an image sync after a build. This will mirror content from internal registries to quay.
        After a successful sync an image stream is updated with the new tags and pullspecs.
        Also update the app registry with operator manifests.
        If operator_nvrs is given, will only build manifests for specified operator NVRs.
        """
        if not self.build_plan.build_images:
            self.runtime.logger.info('No built images to sync.')
            return

        self.runtime.logger.info('Syncing built images')

        with open(f'{self.runtime.doozer_working}/record.log', 'r') as file:
            record_log: dict = record_util.parse_record_log(file)

        records = record_log.get('build', [])
        operator_nvrs = []

        for record in records:
            if record['has_olm_bundle'] == '1' and record['status'] == '0' and record.get('nvrs', None):
                operator_nvrs.append(record['nvrs'].split(',')[0])

        if self.runtime.dry_run:
            self.runtime.logger.info('Skipping build-sync and olm-bundle for dry run mode')
            return

        if self.assembly == 'test':
            self.runtime.logger.warning('Skipping build-sync job for test assembly')

        else:
            # Trigger ocp4 build sync only for streams that are not being updated with konflux builds
            if self.version in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS:
                self.runtime.logger.info(
                    f'Skipping build-sync job for streams updated by konflux builds {KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS}'
                )
            else:
                jenkins.start_build_sync(
                    build_version=self.version,
                    assembly=self.assembly,
                    doozer_data_path=self.data_path,
                    doozer_data_gitref=self.data_gitref,
                )

        if operator_nvrs:
            jenkins.start_olm_bundle(
                build_version=self.version,
                assembly=self.assembly,
                operator_nvrs=operator_nvrs,
                doozer_data_path=self.data_path,
                doozer_data_gitref=self.data_gitref,
            )

    async def _sweep(self):
        if self.all_image_build_failed:
            self.runtime.logger.warning('All image builds failed: skipping sweep')
            return
        if self.assembly != 'stream':
            self.runtime.logger.info('Not setting bugs to ON_QA since assembly is not stream')
            return

        cmd = [
            'elliott',
            f'--group=openshift-{self.version}',
            "find-bugs:qe",
        ]
        if self.runtime.dry_run:
            cmd.append('--dry-run')

        try:
            await exectools.cmd_assert_async(cmd)
        except ChildProcessError:
            if self.runtime.dry_run:
                return
            self._slack_client.bind_channel(f'openshift-{self.version}')
            await self._slack_client.say(f'Bug sweep failed for {self.version}. Please investigate')

        await self._golang_bug_sweep()

    async def _golang_bug_sweep(self):
        # find-bugs:golang only modifies bug state after verifying
        # that the bug is fixed in the builds found in latest nightly / rpms in candidate tag
        # therefore we do not need to check which builds are successful to run this
        if self.assembly != 'stream':
            self.runtime.logger.info('Not setting golang bugs to ON_QA since assembly is not stream')
            return

        cmd = [
            'elliott',
            '--assembly',
            'stream',
            f'--group=openshift-{self.version}',
            "find-bugs:golang",
            "--analyze",
            "--update-tracker",
        ]

        if self.runtime.dry_run:
            cmd.append('--dry-run')

        try:
            await exectools.cmd_assert_async(cmd)
        except ChildProcessError:
            if self.runtime.dry_run:
                return
            self._slack_client.bind_channel(f'openshift-{self.version}')
            await self._slack_client.say(f'Golang bug sweep failed for {self.version}. Please investigate')

    def _report_success(self):
        # Update description with build metrics
        if self.runtime.dry_run or (not self.build_plan.build_rpms and not self.build_plan.build_images):
            record_log = {}  # Nothing was actually built
        else:
            with open(f'{self.runtime.doozer_working}/record.log', 'r') as file:
                record_log: dict = record_util.parse_record_log(file)
        metrics = record_log.get('image_build_metrics', {})

        if not metrics:
            timing_report = 'No images actually built.'
        else:
            timing_report = (
                f'Images built: {metrics[0]["task_count"]}\n'
                f'Elapsed image build time: {metrics[0]["elapsed_total_minutes"]} minutes\n'
                f'Time spent waiting for OSBS capacity: {metrics[0]["elapsed_wait_minutes"]} minutes'
            )

        jenkins.update_description(f'<hr />Build results:<br/><br/>{timing_report}<br/>')

    async def _request_mass_rebuild(self):
        queue = locks.Keys.BREW_MASS_REBUILD_QUEUE.value
        mapping = {self.version: mass_rebuild_score(self.version)}

        # add yourself to the queue
        # if queue does not exist, it will be created with the value
        # nx=True to only create new elements and not to update scores for elements that already exist.
        # This is so that release-artists can manually add versions with scores to set mass rebuild order
        # and they will not be overwritten
        await redis.call('zadd', queue, mapping, nx=True)
        self.runtime.logger.info('Queued in for mass rebuild lock')

        # if we reach timeout, quit but don't remove yourself from the queue
        # it will prevent other versions from acquiring the lock, until you get back in the queue
        # for accidental mass rebuild requests, we will need to delete the keys manually for now
        return await locks.enqueue_for_lock(
            coro=self._rebase_and_build_images(),
            lock=Lock.MASS_REBUILD,
            lock_name=Lock.MASS_REBUILD.value,
            lock_id=self.lock_identifier,
            ocp_version=self.version,
            version_queue_name=queue,
        )

    async def run(self):
        await self._initialize()

        # Rebase and build RPMs
        await self._rebase_and_build_rpms()

        # Build plashets
        if not self.skip_plashets and self.version not in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS:
            jenkins.start_build_plashets(
                version=self.version,
                release=self.release,
                assembly=self.assembly,
                data_path=self.data_path,
                data_gitref=self.data_gitref,
                dry_run=self.runtime.dry_run,
                block_until_complete=True,
            )

        else:
            self.runtime.logger.warning('Skipping plashets creation')

        # Rebase and build images
        if self.build_plan.build_images:
            if self.mass_rebuild:
                await self._slack_client.say(
                    f':loading-correct: Enqueuing mass rebuild for {self.version} :loading-correct:'
                )
                await self._request_mass_rebuild()
            else:
                await self._rebase_and_build_images()
        else:
            self.runtime.logger.info('Not building images.')

        # Sync images
        await self._sync_images()

        # Find MODIFIED bugs for the target-releases, and set them to ON_QA
        await self._sweep()

        # All good
        self._report_success()
        await self.runtime.cleanup_sources('sources')


@cli.command(
    "ocp4",
    help="Build OCP 4.y components incrementally. In typical usage, scans for changes that could affect "
    "package or image builds and rebuilds the affected components. Creates new plashets if the "
    "automation is not frozen or if there are RPMs that are built in this run, and runs other jobs to "
    "sync builds to nightlies, create operator metadata, and sets MODIFIED bugs to ON_QA",
)
@click.option('--version', required=True, help='OCP version to scan, e.g. 4.14')
@click.option('--assembly', required=True, help='The name of an assembly to rebase & build for')
@click.option(
    '--data-path',
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option(
    '--build-rpms',
    required=True,
    type=click.Choice(['all', 'only', 'except', 'none'], case_sensitive=False),
    help='Which RPMs are candidates for building? "only/except" refer to --rpm-list param',
)
@click.option(
    '--rpm-list',
    required=False,
    default='',
    help='(Optional) Comma/space-separated list to include/exclude per BUILD_RPMS (e.g. openshift,openshift-kuryr)',
)
@click.option(
    '--build-images',
    required=True,
    type=click.Choice(['all', 'only', 'except', 'none'], case_sensitive=False),
    help='Which images are candidates for building? "only/except" refer to --image-list param',
)
@click.option(
    '--image-list',
    required=False,
    default='',
    help='(Optional) Comma/space-separated list to include/exclude per BUILD_IMAGES '
    '(e.g. logging-kibana5,openshift-jenkins-2)',
)
@click.option(
    '--skip-plashets',
    is_flag=True,
    default=False,
    help='Do not build plashets (for example to save time when running multiple builds against test assembly)',
)
@click.option(
    '--mail-list-failure',
    required=False,
    default='aos-art-automation+failed-ocp4-build@redhat.com',
    help='Failure Mailing List',
)
@click.option(
    '--ignore-locks',
    is_flag=True,
    default=False,
    help='Do not wait for other builds in this version to complete (use only if you know they will not conflict)',
)
@click.option('--comment-on-pr', is_flag=True, default=False, help='Comment on source PR after successful build')
@pass_runtime
@click_coroutine
async def ocp4(
    runtime: Runtime,
    version: str,
    assembly: str,
    data_path: str,
    data_gitref: str,
    build_rpms: str,
    rpm_list: str,
    build_images: str,
    image_list: str,
    skip_plashets: bool,
    mail_list_failure: str,
    ignore_locks: bool,
    comment_on_pr: bool,
):
    lock_identifier = jenkins.get_build_path()
    if not lock_identifier:
        runtime.logger.warning('Env var BUILD_URL has not been defined: a random identifier will be used for the locks')

    pipeline = Ocp4Pipeline(
        runtime=runtime,
        assembly=assembly,
        version=version,
        data_path=data_path,
        data_gitref=data_gitref,
        build_rpms=build_rpms,
        rpm_list=rpm_list,
        build_images=build_images,
        image_list=image_list,
        skip_plashets=skip_plashets,
        mail_list_failure=mail_list_failure,
        lock_identifier=lock_identifier,
        comment_on_pr=comment_on_pr,
    )

    if ignore_locks:
        await pipeline.run()

    else:
        await locks.run_with_lock(
            coro=pipeline.run(),
            lock=Lock.BUILD,
            lock_name=Lock.BUILD.value.format(version=version),
            lock_id=lock_identifier,
        )
