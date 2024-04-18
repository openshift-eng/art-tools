import json
import os.path
import shutil
import traceback

import click
import yaml

from pyartcd import locks
from pyartcd import util
from pyartcd import plashets
from pyartcd import exectools
from pyartcd import constants
from pyartcd import jenkins
from pyartcd import record as record_util
from pyartcd import oc
from pyartcd.cli import cli, pass_runtime, click_coroutine
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime
from pyartcd.s3 import sync_repo_to_s3_mirror
from artcommonlib import redis


class BuildPlan:
    def __init__(self):
        self.active_image_count = 1  # number of images active in this version
        self.dry_run = False  # report build plan without performing it
        self.pin_builds = False  # build only specified rpms/images regardless of whether source has changed
        self.build_rpms = False  # should we build rpms
        self.rpms_included = []  # include list for rpms to build
        self.rpms_excluded = []  # exclude list for rpms to build
        self.build_images = False  # should we build images
        self.images_included = []  # include list for images to build
        self.images_excluded = []  # exclude list for images to build

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)


class Version:
    def __init__(self):
        self.stream = ''  # "X.Y" e.g. "4.0"
        self.branch = ''  # e.g. "rhaos-4.0-rhel-7"
        self.release = ''  # e.g. "201901011200.?"
        self.major = 0  # X in X.Y, e.g. 4
        self.minor = 0  # Y in X.Y, e.g. 0

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)


class RpmMirror:
    def __init__(self):
        self.url = ''
        self.local_plashet_path = ''
        self.plashet_dir_name = ''

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)


class Ocp4Pipeline:
    def __init__(self, runtime: Runtime, version: str, assembly: str, data_path: str, data_gitref: str,
                 pin_builds: bool, build_rpms: str, rpm_list: str, build_images: str, image_list: str,
                 skip_plashets: bool, mail_list_failure: str, comment_on_pr: bool, copy_links: bool = False,
                 lock_identifier: str = None):

        self.runtime = runtime
        self.assembly = assembly
        self.pin_builds = pin_builds
        self.build_rpms = build_rpms
        self.rpm_list = rpm_list
        self.build_images = build_images
        self.image_list = image_list
        self.skip_plashets = skip_plashets
        self.mail_list_failure = mail_list_failure

        self.build_plan = BuildPlan()
        self.mass_rebuild = False
        self.version = Version()
        self.version.stream = version
        self.rpm_mirror = RpmMirror()  # will be filled in later by build-compose stage
        self.rpm_mirror.url = f'{constants.MIRROR_BASE_URL}/enterprise/enterprise/{self.version.stream}'
        self.all_image_build_failed = False
        self.comment_on_pr = comment_on_pr
        self.copy_links = copy_links

        self._doozer_working = os.path.abspath(f'{self.runtime.working_dir / "doozer_working"}')
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
            f'--working-dir={self._doozer_working}',
            f'--data-path={data_path}',
            group_param
        ]

        self._slack_client = runtime.new_slack_client()
        self._mail_client = self.runtime.new_mail_client()

    async def _check_assembly(self):
        """
        If assembly != 'stream' and assemblies not enabled for <version>, raise an error
        """

        shutil.rmtree(self._doozer_working, ignore_errors=True)
        cmd = self._doozer_base_command.copy()
        cmd.extend(['config:read-group', '--default=False', 'assemblies.enabled'])
        _, out, err = await exectools.cmd_gather_async(cmd)
        assemblies_enabled = out.strip() == 'True'
        self.runtime.logger.info('Assemblies %s enabled for %s',
                                 'NOT' if not assemblies_enabled else '', self.version.stream)

        if self.assembly != 'stream' and not assemblies_enabled:
            raise RuntimeError(
                f"ASSEMBLY cannot be set to '{self.assembly}' because assemblies are not enabled in ocp-build-data.")

        if self.assembly.lower() == "test":
            jenkins.update_title(" [TEST]")

    async def _initialize_version(self):
        """
        Initialize the "version" data structure
        """

        # version.branch
        shutil.rmtree(self._doozer_working, ignore_errors=True)
        cmd = self._doozer_base_command.copy()
        cmd.extend(['config:read-group', 'branch'])
        _, out, _ = await exectools.cmd_gather_async(cmd)
        self.version.branch = out.strip()

        # version.major, version.minor
        self.version.major, self.version.minor = [int(val) for val in self.version.stream.split('.')]

        # version.release
        self.version.release = util.default_release_suffix()

        self.runtime.logger.info('Initializing build:\n%s', str(self.version))
        jenkins.update_title(f' - {self.version.stream}-{self.version.release} ')

    async def _initialize_build_plan(self):
        """
        Initialize the "build_plan" data structure based on job parameters
        """

        # build_plan.active_image_count
        shutil.rmtree(self._doozer_working, ignore_errors=True)
        cmd = self._doozer_base_command.copy()
        cmd.append('images:list')
        _, out, _ = await exectools.cmd_gather_async(cmd)  # Last line looks like this: "219 images"
        self.build_plan.active_image_count = int(out.splitlines()[-1].split(' ')[0].strip())

        # build_plan.dry_run
        self.build_plan.dry_run = self.runtime.dry_run

        # build_plan.pin_builds
        self.build_plan.pin_builds = self.pin_builds

        # build_plan.build_rpms, build_plan.rpms_included, build_plan.rpms_excluded
        self.build_plan.build_rpms = True

        if self.build_rpms.lower() == 'none':
            self.build_plan.build_rpms = False

        elif self.build_rpms.lower() == 'all':
            assert not self.rpm_list, \
                'Aborting because a list of RPMs was specified; you probably want to specify only/except.'

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
            assert not self.image_list, \
                'Aborting because a list of RPMs was specified; you probably want to specify only/except.'

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
        self.mass_rebuild = \
            (include_count and self.build_plan.active_image_count < include_count * 2) or \
            (exclude_count and self.build_plan.active_image_count > exclude_count * 2) or \
            (not include_count and not exclude_count)

    async def _is_build_permitted(self):
        # For assembly != 'stream' always permit ocp4 builds
        if self.assembly != 'stream':
            self.runtime.logger.info('Permitting build for assembly %s', self.assembly)
            return True

        # For assembly = 'stream', we need to check automation freeze state
        return await util.is_build_permitted(
            version=self.version.stream,
            data_path=self.data_path,
            doozer_working=self._doozer_working,
            doozer_data_gitref=self.data_gitref)

    async def _initialize(self):
        jenkins.init_jenkins()

        if not await self._is_build_permitted():
            jenkins.update_description('Builds not permitted', append=False)
            raise RuntimeError(
                'This build is being terminated because it is not permitted according to current group.yml')

        await self._check_assembly()
        await self._initialize_version()
        await self._initialize_build_plan()

    def _plan_pinned_builds(self):
        """
        When "--pin_builds" is provided, always builds what the operator selected, regardless of source changes.
        Update build title and description accordingly.
        """

        jenkins.update_description('Pinned builds (whether source changed or not).<br/>')
        self.runtime.logger.info('Pinned builds (whether source changed or not)')

        if not self.build_plan.build_rpms:
            jenkins.update_description('RPMs: not building.<br/>')

        elif self.build_plan.rpms_included:
            jenkins.update_description(f'RPMs: building {self.build_plan.rpms_included}.<br/>')

        elif self.build_plan.rpms_excluded:
            jenkins.update_description(
                f'RPMs: building all except {self.build_plan.rpms_excluded}.<br/>')

        else:
            jenkins.update_description('RPMs: building all.<br/>')

        if self.build_plan.rpms_included:
            jenkins.update_title(self._display_tag_for(self.build_plan.rpms_included, 'RPM'))

        elif self.build_plan.rpms_excluded:
            jenkins.update_title(
                self._display_tag_for(self.build_plan.rpms_excluded, 'RPM', is_excluded=True))

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
            jenkins.update_title(
                self._display_tag_for(self.build_plan.images_excluded, 'image', is_excluded=True))

        elif self.build_plan.build_images:
            jenkins.update_title(' [all images]')

    async def _get_changes(self) -> dict:
        """
        Check for changes by calling doozer config:scan-sources
        Changed rpms, images or rhcos are recorded in self.changes
        """

        # Scan sources
        shutil.rmtree(self._doozer_working, ignore_errors=True)
        cmd = self._doozer_base_command.copy()
        cmd.extend(['config:scan-sources', '--yaml', '--ci-kubeconfig', f'{os.environ["KUBECONFIG"]}'])
        _, out, _ = await exectools.cmd_gather_async(cmd)
        self.runtime.logger.info('scan-sources output:\n%s', out)

        # Get changes
        yaml_data = yaml.safe_load(out)
        changes = util.get_changes(yaml_data)
        if changes:
            self.runtime.logger.info('Detected source changes:\n%s', yaml.safe_dump(changes))
        else:
            self.runtime.logger.info('No changes detected in RPMs, images or RHCOS')

        return changes

    def _report(self, msg: str):
        """
        Logs the message and appends it to current job description
        """
        self.runtime.logger.info(msg)
        jenkins.update_description(f'{msg}<br/>')

    def _check_changed_rpms(self, changes: dict):
        # Check changed RPMs
        if not self.build_plan.build_rpms:
            self._report('RPMs: not building.')
            self._report('Will not create RPM compose if automation is frozen.')

        elif changes.get('rpms', None):
            changed_rpms = changes["rpms"]
            self._report(f'RPMs: building {",".join(changed_rpms)}')
            self._report('Will create RPM compose.')
            self.build_plan.rpms_included = changed_rpms
            self.build_plan.rpms_excluded.clear()
            jenkins.update_title(self._display_tag_for(self.build_plan.rpms_included, 'RPM'))

        else:
            self.build_plan.build_rpms = False
            self._report('RPMs: none changed.')
            self._report('Will still create RPM compose.')
            jenkins.update_title(' [no changed RPMs]')

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

    async def _check_changed_images(self, changes: dict):
        # Check changed images
        if not self.build_plan.build_images:
            self._report('Images: not building.')
            return

        changed_images = changes.get('images', None)
        if not changed_images:
            self._report('Images: none changed.')
            self.build_plan.build_images = False
            jenkins.update_title(' [no changed images]')
            return

        self._report(f'Found {len(changed_images)} image(s) with changes:\n{",".join(changed_images)}')

        # also determine child images of changed
        changed_children = await self._check_changed_child_images(changes)

        # Update build plan
        self.build_plan.images_included = changed_images + [child for child in changed_children if
                                                            child not in changed_images]
        self.build_plan.images_excluded.clear()
        jenkins.update_title(self._display_tag_for(self.build_plan.images_included, 'image'))

    def _gather_children(self, all_images: list, data: dict, initial: list, gather: bool):
        """
        Scan the image tree for changed and their children using recursive closure

        :param all_images: all images gathered so far while traversing tree
        :param data: the part of the yaml image tree we're looking at
        :param initial: all images initially found to have changed
        :param gather: whether this is a subtree of an image with changed source
        """

        for image, children in data.items():
            gather_this = gather or image in initial
            if gather_this:  # this or an ancestor was a changed image
                all_images.append(image)

            # scan children recursively
            self._gather_children(all_images, children, initial, gather_this)

    async def _check_changed_child_images(self, changes: dict) -> list:
        cmd = self._doozer_base_command.copy()
        cmd.extend(self._include_exclude('images', self.build_plan.images_included, self.build_plan.images_excluded))
        cmd.extend([
            'images:show-tree',
            '--yml'
        ])
        _, out, _ = await exectools.cmd_gather_async(cmd)
        self.runtime.logger.info('images:show-tree output:\n%s', out)
        yaml_data = yaml.safe_load(out)

        children = []
        self._gather_children(
            all_images=children,
            data=yaml_data,
            initial=changes.get('images', []),
            gather=False
        )
        changed_children = [image for image in children if image not in changes['images']]

        if changed_children:
            self._report(f'Images: also building {len(changed_children)} child(ren):\n {", ".join(changed_children)}')
        else:
            self.runtime.logger.info('No changed children found')

        return changed_children

    async def _plan_builds(self):
        """
        Plan what will be built.
        Figure out whether we're building RPMs and/or images, and which ones, based on
        the parameters and which sources have changed.
        Update in the "build_plan" data structure.
        """

        jenkins.update_description('Building sources that have changed.<br/>')
        changes = await self._get_changes()
        self._check_changed_rpms(changes)
        await self._check_changed_images(changes)
        self.runtime.logger.info('Updated build plan:\n%s', self.build_plan)

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
        cmd.extend([
            'rpms:rebase-and-build',
            f'--version={self.version.stream}',
            f'--release={self.version.release}'
        ])

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
            with open(f'{self._doozer_working}/record.log', 'r') as file:
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
        with open(f'{self._doozer_working}/record.log', 'r') as file:
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
            version=self.version.stream,
            doozer_data_path=self.data_path,
            doozer_working=self._doozer_working,
            doozer_data_gitref=self.data_gitref
        )
        self.runtime.logger.info('Automation freeze for %s: %s', self.version.stream, automation_state)

        if automation_state not in ['scheduled', 'yes', 'True']:
            return True

        if automation_state == 'scheduled' and self.build_plan.build_rpms and util.is_manual_build():
            # Send a Slack notification since we're running compose build during automation freeze
            self._slack_client.bind_channel(f'openshift-{self.version.stream}')
            await self._slack_client.say(
                "*:alert: ocp4 build compose running during automation freeze*\n"
                "There were RPMs in the build plan that forced build compose during automation freeze."
            )

            return True

        return False

    async def _build_compose(self):
        """
        If any RPMs have changed, create multiple yum repos (one for each arch) based on -candidate tags
        Those repos can be signed (release state) or unsigned (pre-release state)"
        """

        # Check if compose build is permitted
        if not await self._is_compose_build_permitted():
            self.runtime.logger.info("Skipping compose build as it's not permitted")
            return

        # Build compose
        try:
            plashets_built = await plashets.build_plashets(
                stream=self.version.stream,
                release=self.version.release,
                assembly=self.assembly,
                doozer_working=self._doozer_working,
                data_path=self.data_path,
                data_gitref=self.data_gitref,
                dry_run=self.runtime.dry_run,
                copy_links=self.copy_links
            )
            self.runtime.logger.info('Built plashets: %s', json.dumps(plashets_built, indent=4))

            # public rhel7 ose plashet, if present, needs mirroring to /enterprise/ for CI
            if plashets_built.get('rhel-server-ose-rpms', None):
                self.rpm_mirror.plashet_dir_name = plashets_built['rhel-server-ose-rpms']['plashetDirName']
                self.rpm_mirror.local_plashet_path = plashets_built['rhel-server-ose-rpms']['localPlashetPath']
                self.runtime.logger.info('rhel7 plashet to mirror: %s', str(self.rpm_mirror))

        except ChildProcessError as e:
            error_msg = f'Failed building compose: {e}'
            self.runtime.logger.error(error_msg)
            self.runtime.logger.error(traceback.format_exc())
            self._slack_client.bind_channel(f'openshift-{self.version.stream}')
            await self._slack_client.say("Failed building compose")
            raise

        if self.assembly == 'stream':
            # Since plashets may have been rebuilt, fire off sync for CI. This will transfer RPMs out to
            # mirror.openshift.com/enterprise so that they may be consumed through CI rpm mirrors.
            # Set block_until_building=False since it can take a very long time for the job to start
            # it is enough for it to be queued
            jenkins.start_sync_for_ci(version=self.version.stream, block_until_building=False)

            # Also trigger rhcos builds for the release in order to absorb any changes from plashets or RHEL which may
            # have triggered our rebuild. If there are no changes to the RPMs, the build should exit quickly. If there
            # are changes, the hope is that by the time our images are done building, RHCOS will be ready and build-sync
            # will find consistent RPMs.
            jenkins.start_rhcos(build_version=self.version.stream, new_build=False)

    async def _rebase_images(self):
        self.runtime.logger.info('Rebasing images')

        cmd = self._doozer_base_command.copy()
        cmd.extend(self._include_exclude('images', self.build_plan.images_included, self.build_plan.images_excluded))
        cmd.extend([
            'images:rebase', f'--version=v{self.version.stream}', f'--release={self.version.release}',
            f"--message='Updating Dockerfile version and release v{self.version.stream}-{self.version.release}'",
            '--push', f"--message='{os.environ['BUILD_URL']}'"
        ])

        if self.runtime.dry_run:
            self.runtime.logger.info('Would have run: %s', ' '.join(cmd))
            return

        await exectools.cmd_assert_async(cmd)

        # TODO: if rebase fails for required images, notify image owners, and still notify on other reconciliations
        util.notify_dockerfile_reconciliations(
            version=self.version.stream,
            doozer_working=self._doozer_working,
            mail_client=self._mail_client
        )

        # TODO: if a non-required rebase fails, notify ART and the image owners
        util.notify_bz_info_missing(
            version=self.version.stream,
            doozer_working=self._doozer_working,
            mail_client=self._mail_client
        )

    def _handle_image_build_failures(self):
        with open(f'{self._doozer_working}/record.log', 'r') as file:
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

        if (ratio['total'] > 10 and ratio['ratio'] > 0.25) or \
                (ratio['ratio'] > 1 and ratio['failed'] == ratio['total']):
            self.runtime.logger.warning("%s of %s image builds failed; probably not the owners' fault, "
                                        "will not spam", {ratio['failed']}, {ratio['total']})

        else:
            util.mail_build_failure_owners(
                failed_builds=failed_map,
                doozer_working=self._doozer_working,
                mail_client=self._mail_client,
                default_owner=self.mail_list_failure
            )

    async def _build_images(self):
        self.runtime.logger.info('Building images')

        signing_mode = await util.get_signing_mode(
            group=f'openshift-{self.version.stream}',
            assembly=self.assembly
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
        with open(f'{self._doozer_working}/record.log', 'r') as file:
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
                f':construction: Starting image builds for {self.version.stream} mass rebuild :construction:')

        # In case of mass rebuilds, rebase and build should happend within the same lock scope
        # Otherwise we might rebase, then get blocked on the mass rebuild lock
        # As a consequence, we might be building outdated stuff
        await self._rebase_images()
        await self._build_images()

        if self.mass_rebuild:
            await self._slack_client.say(f'::done_it_is: Mass rebuild for {self.version.stream} complete :done_it_is:')

    async def _sync_images(self):
        if not self.build_plan.build_images:
            self.runtime.logger.info('No built images to sync.')
            return

        self.runtime.logger.info('Syncing built images')

        with open(f'{self._doozer_working}/record.log', 'r') as file:
            record_log: dict = record_util.parse_record_log(file)

        records = record_log.get('build', [])
        operator_nvrs = []

        for record in records:
            if record['has_olm_bundle'] == '1' and record['status'] == '0' and record.get('nvrs', None):
                operator_nvrs.append(record['nvrs'].split(',')[0])

        await util.sync_images(
            version=self.version.stream,
            assembly=self.assembly,
            operator_nvrs=operator_nvrs,
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref
        )

    async def _mirror_rpms(self):
        if self.assembly != 'stream':
            self.runtime.logger.info('No need to mirror rpms for non-stream assembly')
            return

        if not self.rpm_mirror.local_plashet_path:
            self.runtime.logger.info('No updated RPMs to mirror.')
            return

        s3_base_dir = f'/enterprise/enterprise-{self.version.stream}'

        # Sync plashets to mirror
        try:
            await locks.run_with_lock(
                coro=sync_repo_to_s3_mirror(local_dir=self.rpm_mirror.local_plashet_path,
                                            s3_path=f'{s3_base_dir}/latest/',
                                            dry_run=self.runtime.dry_run),
                lock=Lock.MIRRORING_RPMS,
                lock_name=Lock.MIRRORING_RPMS.value.format(version=self.version.stream),
                lock_id=self.lock_identifier
            )

            await locks.run_with_lock(
                coro=sync_repo_to_s3_mirror(
                    local_dir=self.rpm_mirror.local_plashet_path,
                    s3_path=f'/enterprise/all/{self.version.stream}/latest/',
                    dry_run=self.runtime.dry_run
                ),
                lock=Lock.MIRRORING_RPMS,
                lock_name=Lock.MIRRORING_RPMS.value.format(version=self.version.stream),
                lock_id=self.lock_identifier
            )
            self.runtime.logger.info('Finished mirroring OCP %s to openshift mirrors',
                                     f'{self.version.stream}-{self.version.release}')

        except ChildProcessError as e:
            error_msg = f'Failed syncing {self.rpm_mirror.local_plashet_path} repo to art-srv-enterprise S3: {e}',
            self.runtime.logger.error(error_msg)
            self.runtime.logger.error(traceback.format_exc())
            self._slack_client.bind_channel(f'openshift-{self.version.stream}')
            await self._slack_client.say(f"Failed syncing {self.rpm_mirror.local_plashet_path} repo to "
                                         f"art-srv-enterprise S3")
            raise

    async def _sweep(self):
        if self.all_image_build_failed:
            self.runtime.logger.warning('All image builds failed: skipping sweep')
            return
        if self.assembly != 'stream':
            self.runtime.logger.info('Not setting bugs to QE, as assembly is not stream')
            return

        cmd = [
            'elliott',
            f'--group=openshift-{self.version.stream}',
            "find-bugs:qe"
        ]
        if self.runtime.dry_run:
            cmd.append('--dry-run')

        try:
            await exectools.cmd_assert_async(cmd)

        except ChildProcessError:
            if self.runtime.dry_run:
                return

            self._mail_client.send_mail(
                to='aos-art-automation+failed-sweep@redhat.com',
                subject=f'Problem sweeping after {jenkins.current_build_url}',
                content=f'Check Jenkins console for details: {jenkins.current_build_url}/console'
            )

    def _report_success(self):
        # Update description with build metrics
        if self.runtime.dry_run or (not self.build_plan.build_rpms and not self.build_plan.build_images):
            record_log = {}  # Nothing was actually built
        else:
            with open(f'{self._doozer_working}/record.log', 'r') as file:
                record_log: dict = record_util.parse_record_log(file)
        metrics = record_log.get('image_build_metrics', None)

        if not metrics:
            timing_report = 'No images actually built.'
        else:
            timing_report = f'Images built: {metrics[0]["task_count"]}\n' \
                            f'Elapsed image build time: {metrics[0]["elapsed_total_minutes"]} minutes\n' \
                            f'Time spent waiting for OSBS capacity: {metrics[0]["elapsed_wait_minutes"]} minutes'

        jenkins.update_description(f'<hr />Build results:<br/><br/>{timing_report}<br/>')

    def _mass_rebuild_score(self) -> int:
        """For the ocp_version (e.g. `4.16`) return an integer score value
        Higher the score, higher the priority
        """
        return round(float(self.version.stream) * 100)  # '4.16' -> 416

    async def _request_mass_rebuild(self):
        queue = locks.Keys.MASS_REBUILD_QUEUE.value
        mapping = {self.version.stream: self._mass_rebuild_score()}

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
            ocp_version=self.version.stream,
            version_queue_name=queue
        )

    async def run(self):
        await self._initialize()

        # Plan builds
        if self.build_plan.pin_builds:
            self._plan_pinned_builds()
        else:
            self.runtime.logger.info('Building only where source has changed.')
            await self._plan_builds()

        # Rebase and build RPMs
        await self._rebase_and_build_rpms()
        if not self.skip_plashets:
            lock = Lock.PLASHET
            lock_name = lock.value.format(assembly=self.assembly, version=self.version.stream)
            await locks.run_with_lock(
                coro=self._build_compose(),
                lock=lock,
                lock_name=lock_name,
                lock_id=self.lock_identifier
            )

        else:
            self.runtime.logger.warning('Skipping plashets creation as SKIP_PLASHETS was set to True')

        # Rebase and build images
        if self.build_plan.build_images:
            if self.mass_rebuild:
                await self._slack_client.say(
                    f':loading-correct: Enqueuing mass rebuild for {self.version.stream} :loading-correct:')
                await self._request_mass_rebuild()
            else:
                await self._rebase_and_build_images()
        else:
            self.runtime.logger.info('Not building images.')

        # Sync images
        await self._sync_images()

        # Mirror RPMs
        await self._mirror_rpms()

        # Find MODIFIED bugs for the target-releases, and set them to ON_QA
        await self._sweep()

        # All good
        self._report_success()


@cli.command("ocp4",
             help="Build OCP 4.y components incrementally. In typical usage, scans for changes that could affect "
                  "package or image builds and rebuilds the affected components. Creates new plashets if the "
                  "automation is not frozen or if there are RPMs that are built in this run, and runs other jobs to "
                  "sync builds to nightlies, create operator metadata, and sets MODIFIED bugs to ON_QA")
@click.option('--version', required=True, help='OCP version to scan, e.g. 4.14')
@click.option('--assembly', required=True, help='The name of an assembly to rebase & build for')
@click.option('--data-path', required=False, default=constants.OCP_BUILD_DATA_URL,
              help='ocp-build-data fork to use (e.g. assembly definition in your own fork)')
@click.option('--data-gitref', required=False, default='',
              help='Doozer data path git [branch / tag / sha] to use')
@click.option('--pin-builds', is_flag=True,
              help='Build only specified rpms/images regardless of whether source has changed')
@click.option('--build-rpms', required=True,
              type=click.Choice(['all', 'only', 'except', 'none'], case_sensitive=False),
              help='Which RPMs are candidates for building? "only/except" refer to --rpm-list param')
@click.option('--rpm-list', required=False, default='',
              help='(Optional) Comma/space-separated list to include/exclude per BUILD_RPMS '
                   '(e.g. openshift,openshift-kuryr)')
@click.option('--build-images', required=True,
              type=click.Choice(['all', 'only', 'except', 'none'], case_sensitive=False),
              help='Which images are candidates for building? "only/except" refer to --image-list param')
@click.option('--image-list', required=False, default='',
              help='(Optional) Comma/space-separated list to include/exclude per BUILD_IMAGES '
                   '(e.g. logging-kibana5,openshift-jenkins-2)')
@click.option('--skip-plashets', is_flag=True, default=False,
              help='Do not build plashets (for example to save time when running multiple builds against test assembly)')
@click.option('--mail-list-failure', required=False, default='aos-art-automation+failed-ocp4-build@redhat.com',
              help='Failure Mailing List')
@click.option('--ignore-locks', is_flag=True, default=False,
              help='Do not wait for other builds in this version to complete (use only if you know they will not conflict)')
@click.option('--comment-on-pr', is_flag=True, default=False, help='Comment on source PR after successful build')
@click.option('--copy-links', is_flag=True, default=False,
              help='Call rsync with --copy-links instead of --links')
@pass_runtime
@click_coroutine
async def ocp4(runtime: Runtime, version: str, assembly: str, data_path: str, data_gitref: str, pin_builds: bool,
               build_rpms: str, rpm_list: str, build_images: str, image_list: str, skip_plashets: bool,
               mail_list_failure: str, ignore_locks: bool, comment_on_pr: bool, copy_links: bool):

    lock_identifier = jenkins.get_build_path()
    if not lock_identifier:
        runtime.logger.warning('Env var BUILD_URL has not been defined: a random identifier will be used for the locks')

    pipeline = Ocp4Pipeline(
        runtime=runtime,
        assembly=assembly,
        version=version,
        data_path=data_path,
        data_gitref=data_gitref,
        pin_builds=pin_builds,
        build_rpms=build_rpms,
        rpm_list=rpm_list,
        build_images=build_images,
        image_list=image_list,
        skip_plashets=skip_plashets,
        mail_list_failure=mail_list_failure,
        lock_identifier=lock_identifier,
        comment_on_pr=comment_on_pr,
        copy_links=copy_links
    )

    if ignore_locks:
        await pipeline.run()

    else:
        await locks.run_with_lock(
            coro=pipeline.run(),
            lock=Lock.BUILD,
            lock_name=Lock.BUILD.value.format(version=version),
            lock_id=lock_identifier
        )
