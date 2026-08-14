import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

import click
from artcommonlib import exectools
from artcommonlib.constants import ACTIVE_OCP_VERSIONS
from doozerlib.cli.images_health import DELTA_DAYS, LIMIT_BUILD_RESULTS, ConcernCode
from doozerlib.constants import ART_BUILD_FAILURES_URL, ART_BUILD_HISTORY_URL

from pyartcd import util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.jira_client import JIRAClient
from pyartcd.runtime import Runtime

_LOGGER = logging.getLogger(__name__)

FAILURE_CODES = {ConcernCode.FAILING_AT_LEAST_FOR.value, ConcernCode.LATEST_ATTEMPT_FAILED.value}


class ImagesHealthPipeline:
    def __init__(
        self,
        runtime: Runtime,
        versions: str,
        send_to_release_channel: bool,
        public_channel: str,
        data_path: str,
        data_gitref: str,
        image_list: str,
        assembly: str,
        sync_jira: bool = True,
        ping_chai_bot: bool = False,
    ):
        self.runtime = runtime
        self.versions = versions.split(',') if versions else ACTIVE_OCP_VERSIONS
        self.doozer_working = self.runtime.working_dir / "doozer_working"
        self.send_to_release_channel = send_to_release_channel
        self.public_channel = public_channel
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.image_list = image_list.split(',') if image_list else []
        self.assembly = assembly
        self._sync_jira = sync_jira
        self.ping_chai_bot = ping_chai_bot
        self.report = []
        self.slack_client = self.runtime.new_slack_client()
        self.scanned_versions = []
        self.rebase_failures = {}  # version -> {image: {failure_count, url, build_system}}
        self.ec_failures = {}  # version -> {image: {failure_count, jenkins_url, nvr, pipeline_url, ...}}
        self.release_failures = {}  # version -> {image: {failure_count, jenkins_url, nvr, pipeline_url, ...}}
        self.jira_tickets: dict[tuple[str, str], str] = {}  # (image_name, group) -> ticket key
        self._valid_images_cache: dict[str, set[str]] = {}

    async def run(self):
        await asyncio.gather(*(self.get_report(v) for v in self.versions))
        await asyncio.gather(
            *(self.get_rebase_failures(v) for v in self.versions),
            *(self.get_ec_failures(v) for v in self.versions),
            *(self.get_release_failures(v) for v in self.versions),
        )
        self.runtime.logger.info('Found %s concerns', len(self.report))

        if self._sync_jira and self.assembly == "stream":
            try:
                self.sync_jira()
            except Exception:
                _LOGGER.exception("Jira sync failed; continuing without Jira ticket updates")

        if self.send_to_release_channel:
            for version in self.scanned_versions:
                await self.notify_release_channel(version)

        if self.public_channel:
            await self.notify_public_channel()

        if self.ping_chai_bot:
            multi_build_failures = self._get_multi_failure_images()
            multi_rebase_failures = self._get_multi_rebase_failures()
            all_versions = set(multi_build_failures) | set(multi_rebase_failures)
            for version in all_versions:
                await self.notify_chai_bot(
                    version,
                    multi_build_failures.get(version, []),
                    multi_rebase_failures.get(version, {}),
                )

    async def get_report(self, version: str) -> Optional[list]:
        doozer_working = f'{self.doozer_working}-{version}'
        if not await util.is_build_permitted(
            version,
            doozer_working=str(doozer_working),
            data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        ):
            self.runtime.logger.info('Skipping %s scan as the group is frozen', version)
            return

        self.scanned_versions.append(version)

        group = f'openshift-{version}'
        failures = await util.get_counter_failures('build-failure', group=group, logger=self.runtime.logger)

        # Use Redis failures to scope the image list for the BigQuery query.
        # If an explicit image_list was provided, intersect it with failing images.
        failing_images = set(failures.keys())
        if self.image_list:
            failing_images &= set(self.image_list)

        if not failing_images:
            self.runtime.logger.info('No build failures in Redis for %s; skipping BigQuery scan', group)
            return

        # Filter failing_images to only include images that exist in current ocp-build-data
        valid_images = await self._get_valid_images(version, doozer_working)
        filtered_failing_images = failing_images & valid_images
        skipped_images = failing_images - valid_images

        if skipped_images:
            self.runtime.logger.warning(
                'Filtered out %d image(s) from Redis that do not exist in %s metadata: %s',
                len(skipped_images),
                group,
                ', '.join(sorted(skipped_images)),
            )

        if not filtered_failing_images:
            self.runtime.logger.info(
                'No valid failing images remain for %s after filtering; skipping BigQuery scan', group
            )
            return

        self.runtime.logger.info(
            'Redis reports %d failing image(s) for %s; querying BigQuery for details',
            len(filtered_failing_images),
            group,
        )

        group_param = f'--group={group}'
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'
        cmd = [
            'doozer',
            f'--working-dir={doozer_working}',
            f'--data-path={self.data_path}',
            group_param,
            f'--images={",".join(sorted(filtered_failing_images))}',
            'images:health',
        ]

        if self.assembly:
            cmd.append(f'--assembly={self.assembly}')

        _, out, err = await exectools.cmd_gather_async(cmd, stderr=None)
        report = json.loads(out.strip())
        self.runtime.logger.info('images:health output for %s:\n%s', group, out)
        self.report.extend(report)

    async def get_rebase_failures(self, version: str):
        """
        Fetch OCP rebase failure data from Redis for a specific version.
        Checks both Brew and Konflux build systems.
        Filters out images that don't exist in current ocp-build-data.
        Populates self.rebase_failures[version] with {image: {failure_count, url, build_system}}.

        Arg(s):
            version (str): OCP version (e.g., "4.18")
        """
        # Fetch all rebase failures from Redis
        group = f'openshift-{version}'
        all_failures = await util.get_rebase_failures(
            group=group,
            branches=['rebase-failure'],
            build_systems=['brew', 'konflux'],
            logger=self.runtime.logger,
        )

        # Get list of valid images for this version from metadata
        doozer_working = f'{self.doozer_working}-{version}'
        valid_images = await self._get_valid_images(version, doozer_working)

        # Filter to only include images that exist in metadata
        filtered_failures = {}
        skipped_images = []

        for image_name, failure_info in all_failures.items():
            if image_name in valid_images:
                filtered_failures[image_name] = failure_info
            else:
                skipped_images.append(image_name)

        if skipped_images:
            self.runtime.logger.warning(
                'Filtered out %d rebase failure(s) from Redis that do not exist in openshift-%s metadata: %s',
                len(skipped_images),
                version,
                ', '.join(sorted(skipped_images)),
            )

        self.rebase_failures[version] = filtered_failures

    async def _get_valid_images(self, version: str, doozer_working: str) -> set[str]:
        """
        Get the set of valid image distgit keys for a given version from ocp-build-data.
        Results are cached per version to avoid duplicate doozer subprocess calls.

        Arg(s):
            version (str): OCP version (e.g., "4.18")
            doozer_working (str): Doozer working directory path
        Return Value(s):
            set[str]: Set of valid image distgit keys (e.g. "ose-console.rhel9")
        """
        if version in self._valid_images_cache:
            return self._valid_images_cache[version]

        group_param = f'--group=openshift-{version}'
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'

        cmd = [
            'doozer',
            f'--working-dir={doozer_working}',
            f'--data-path={self.data_path}',
            group_param,
            'images:print',
            '--show-base',
            '--show-non-release',
            '--short',
            '{distgit_key}',
        ]

        try:
            _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
            valid_images = {line.strip() for line in out.strip().split('\n') if line.strip()}
            self.runtime.logger.info('Found %d valid images for openshift-%s', len(valid_images), version)
            self._valid_images_cache[version] = valid_images
            return valid_images
        except Exception as e:
            self.runtime.logger.warning(
                'Failed to fetch valid images for openshift-%s: %s. Proceeding without filtering.', version, e
            )
            return set()

    async def _get_typed_failures(self, version: str, counter_type: str) -> dict:
        """
        Fetch failure data from Redis for a specific counter type and version.
        Filters out images that don't exist in current ocp-build-data.

        Arg(s):
            version (str): OCP version (e.g., "4.18")
            counter_type (str): Redis counter type (e.g., 'ec-failure', 'release-failure')
        Return Value(s):
            dict: {image_name: {failure_count, jenkins_url, nvr, ...}}
        """
        group = f'openshift-{version}'
        all_failures = await util.get_counter_failures(counter_type, group=group, logger=self.runtime.logger)

        if not all_failures:
            return {}

        doozer_working = f'{self.doozer_working}-{version}'
        valid_images = await self._get_valid_images(version, doozer_working)

        filtered_failures = {}
        skipped_images = []

        for image_name, failure_info in all_failures.items():
            if image_name in valid_images:
                filtered_failures[image_name] = failure_info
            else:
                skipped_images.append(image_name)

        if skipped_images:
            self.runtime.logger.warning(
                'Filtered out %d %s(s) from Redis that do not exist in %s metadata: %s',
                len(skipped_images),
                counter_type,
                group,
                ', '.join(sorted(skipped_images)),
            )

        return filtered_failures

    async def get_ec_failures(self, version: str):
        """
        Fetch EC verification failure data from Redis for a specific version.

        Arg(s):
            version (str): OCP version (e.g., "4.18")
        """
        self.ec_failures[version] = await self._get_typed_failures(version, 'ec-failure')

    async def get_release_failures(self, version: str):
        """
        Fetch release-to-authz failure data from Redis for a specific version.

        Arg(s):
            version (str): OCP version (e.g., "4.18")
        """
        self.release_failures[version] = await self._get_typed_failures(version, 'release-failure')

    def sync_jira(self):
        jira_client = self.runtime.new_jira_client()
        scanned_groups = {f"openshift-{v}" for v in self.scanned_versions}

        # Sync build failure tickets (from doozer concerns)
        build_failures: dict[tuple[str, str], dict] = {}
        for concern in self.report:
            if concern['code'] in FAILURE_CODES:
                key = (concern['image_name'], concern['group'])
                build_failures[key] = concern
        self._sync_jira_for_label(
            jira_client,
            'art:image-build-failure',
            build_failures,
            scanned_groups,
            summary_prefix='Image build failure',
            description_builder=self._build_description,
            label_builder=self._build_failure_labels,
            updater=self._update_build_failure_ticket,
        )

        # Sync EC failure tickets (from Redis)
        ec_failures = self._redis_failures_to_jira_dict(self.ec_failures)
        self._sync_jira_for_label(
            jira_client,
            'art:image-ec-failure',
            ec_failures,
            scanned_groups,
            summary_prefix='Image EC verification failure',
            description_builder=self._build_redis_description,
            label_builder=self._redis_failure_labels,
        )

        # Sync release failure tickets (from Redis)
        release_failures = self._redis_failures_to_jira_dict(self.release_failures)
        self._sync_jira_for_label(
            jira_client,
            'art:image-release-failure',
            release_failures,
            scanned_groups,
            summary_prefix='Image release to authz failure',
            description_builder=self._build_redis_description,
            label_builder=self._redis_failure_labels,
        )

    def _sync_jira_for_label(
        self,
        jira_client: JIRAClient,
        failure_label: str,
        current_failures: dict[tuple[str, str], dict],
        scanned_groups: set[str],
        summary_prefix: str,
        description_builder,
        label_builder,
        updater=None,
    ):
        """
        Generic Jira ticket sync for a specific failure label type.

        Arg(s):
            jira_client (JIRAClient): Jira client instance
            failure_label (str): Jira label for this failure type (e.g., 'art:image-build-failure')
            current_failures (dict): {(image_name, group): failure_data}
            scanned_groups (set[str]): Groups that were scanned this run
            summary_prefix (str): Prefix for ticket summary
            description_builder (callable): Function to build ticket description from failure data
            label_builder (callable): Function to build labels list from failure data
            updater (callable): Optional custom updater for existing tickets
        """
        jql = f'project = ART AND labels = "{failure_label}" AND statusCategory != Done'
        open_tickets = jira_client.search_issues(jql, maxResults=False)

        ticket_index: dict[tuple[str, str], object] = {}
        for ticket in open_tickets:
            image_name = None
            group = None
            for label in ticket.fields.labels:
                if label.startswith("art:package:"):
                    image_name = label[len("art:package:") :]
                elif label.startswith("art:group:"):
                    group = label[len("art:group:") :]
            if image_name and group:
                ticket_index[(image_name, group)] = ticket

        for (image_name, group), failure_data in current_failures.items():
            if (image_name, group) in ticket_index:
                ticket = ticket_index[(image_name, group)]
                self.jira_tickets[(image_name, group)] = ticket.key
                _LOGGER.info("Ticket already exists for %s (%s): %s — updating", image_name, group, ticket.key)
                if updater:
                    updater(ticket, failure_data)
                else:
                    ticket.update(
                        fields={
                            "labels": label_builder(failure_label, image_name, group, failure_data),
                            "description": description_builder(failure_data),
                        }
                    )
                continue

            labels = label_builder(failure_label, image_name, group, failure_data)
            ticket = jira_client.create_issue(
                project="ART",
                issue_type="Task",
                summary=f"{summary_prefix}: {image_name} ({group})",
                description=description_builder(failure_data),
                labels=labels,
                components=["daily-ops"],
            )
            _LOGGER.info("Created ticket %s for %s (%s)", ticket.key, image_name, group)
            self.jira_tickets[(image_name, group)] = ticket.key

        for (image_name, group), ticket in ticket_index.items():
            if group not in scanned_groups:
                _LOGGER.info("Skipping close check for %s (%s): version not scanned", image_name, group)
                continue
            if self.image_list and image_name not in self.image_list:
                _LOGGER.info("Skipping close check for %s (%s): image not in scan list", image_name, group)
                continue
            if (image_name, group) not in current_failures:
                _LOGGER.info("Closing resolved ticket %s for %s (%s)", ticket.key, image_name, group)
                jira_client.close_task(ticket.key)

    @staticmethod
    def _redis_failures_to_jira_dict(failures_by_version: dict) -> dict[tuple[str, str], dict]:
        """
        Convert version-keyed Redis failures to (image_name, group) keyed dict for Jira sync.

        Arg(s):
            failures_by_version (dict): {version: {image: {failure_count, ...}}}
        Return Value(s):
            dict: {(image_name, group): failure_data}
        """
        result = {}
        for version, failures in failures_by_version.items():
            group = f'openshift-{version}'
            for image_name, failure_info in failures.items():
                result[(image_name, group)] = {**failure_info, 'image_name': image_name, 'group': group}
        return result

    @staticmethod
    def _get_fail_count(concern: dict) -> int:
        """
        Return the number of consecutive build failures from a concern.
        """
        if concern['code'] == ConcernCode.FAILING_AT_LEAST_FOR.value:
            return LIMIT_BUILD_RESULTS
        return concern['latest_success_idx']

    @staticmethod
    def _fail_count_label(concern: dict) -> str:
        return f"art:fail-count:{ImagesHealthPipeline._get_fail_count(concern)}"

    @staticmethod
    def _build_failure_labels(failure_label: str, image_name: str, group: str, concern: dict) -> list[str]:
        """
        Build labels for a build failure Jira ticket.
        """
        return [
            failure_label,
            f"art:package:{image_name}",
            f"art:group:{group}",
            ImagesHealthPipeline._fail_count_label(concern),
        ]

    @staticmethod
    def _redis_failure_labels(failure_label: str, image_name: str, group: str, failure_data: dict) -> list[str]:
        """
        Build labels for an EC or release failure Jira ticket.
        """
        labels = [
            failure_label,
            f"art:package:{image_name}",
            f"art:group:{group}",
        ]
        failure_count = failure_data.get('failure_count', 0)
        if failure_count:
            labels.append(f"art:fail-count:{failure_count}")
        return labels

    def _update_build_failure_ticket(self, ticket, concern: dict):
        """
        Update fail-count label and description on an existing build failure ticket.
        """
        new_label = self._fail_count_label(concern)
        labels = [label for label in ticket.fields.labels if not label.startswith("art:fail-count:")]
        if new_label not in labels:
            labels.append(new_label)
        ticket.update(fields={"labels": labels, "description": self._build_description(concern)})

    def _build_description(self, concern: dict) -> str:
        image_name = concern['image_name']
        group = concern['group']
        code = concern['code']
        nvr = concern.get('latest_failed_nvr', 'N/A')
        logs_url = self.get_logs_url(concern)
        search_url = self.get_search_url(concern)

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            category = f"Persistent failure (>{LIMIT_BUILD_RESULTS} consecutive failures)"
        else:
            category = f"Recent failure ({concern.get('latest_success_idx', '?')} attempts since last success)"

        last_good_url = concern.get("latest_successful_task_url", "")

        description = (
            f"Image build failure detected by images-health.\n\n"
            f"*Image:* {image_name}\n"
            f"*Group:* {group}\n"
            f"*NVR:* {nvr}\n"
            f"*Category:* {category}\n"
            f"*Build history:* {search_url}\n"
            f"*Error logs:* {logs_url}\n"
        )
        if last_good_url:
            description += f"*Last successful build:* {last_good_url}\n"
        return description

    @staticmethod
    def _build_redis_description(failure_data: dict) -> str:
        """
        Build a Jira ticket description from Redis failure data (EC or release failures).
        """
        image_name = failure_data.get('image_name', 'N/A')
        group = failure_data.get('group', 'N/A')
        failure_count = failure_data.get('failure_count', 0)
        nvr = failure_data.get('nvr', 'N/A')
        jenkins_url = failure_data.get('jenkins_url', '')

        description = (
            f"Image failure detected by images-health.\n\n"
            f"*Image:* {image_name}\n"
            f"*Group:* {group}\n"
            f"*NVR:* {nvr}\n"
            f"*Consecutive failures:* {failure_count}\n"
        )
        if jenkins_url:
            description += f"*Last failure job:* {jenkins_url}\n"
        pipeline_url = failure_data.get('pipeline_url', '')
        if pipeline_url:
            description += f"*Pipeline:* {pipeline_url}\n"
        return description

    def _jira_link(self, concern: dict) -> str:
        key = self.jira_tickets.get((concern['image_name'], concern['group']))
        if not key:
            return ''
        jira_url = f'{self.runtime.config["jira"]["url"]}/browse/{key}'
        return f' | {self.url_text(jira_url, key)}'

    async def notify_release_channel(self, version):
        self.slack_client.bind_channel(version)

        concerns = [
            concern
            for concern in self.report
            if concern.get('group', '') == f'openshift-{version}'
            and concern['code'] != ConcernCode.LATEST_BUILD_SUCCEEDED.value
        ]

        rebase_failures = self.rebase_failures.get(version, {})
        ec_failures = self.ec_failures.get(version, {})
        release_failures = self.release_failures.get(version, {})

        version_tag = f'`openshift-{version}`'
        if self.assembly != 'stream':
            version_tag += f' (assembly `{self.assembly}`)'

        if not concerns and not rebase_failures and not ec_failures and not release_failures:
            await self.slack_client.say(f':white_check_mark: All images are healthy for {version_tag}')
            return

        summary_parts = []
        if concerns:
            n = len(concerns)
            summary_parts.append(f'{n} image{"s" if n > 1 else ""} failed to build')
        if ec_failures:
            n = len(ec_failures)
            summary_parts.append(f'{n} image{"s" if n > 1 else ""} failed EC verification')
        if release_failures:
            n = len(release_failures)
            summary_parts.append(f'{n} image{"s" if n > 1 else ""} failed to be released to authz')
        if rebase_failures:
            n = len(rebase_failures)
            summary_parts.append(f'{n} image{"s" if n > 1 else ""} with rebase failures')

        issues = '\n- '.join(summary_parts)
        response = await self.slack_client.say(
            f':alert: There are some issues to look into for {version_tag}:\n- {issues}'
        )

        report = ''

        if concerns:
            report += f'*Build Failures ({len(concerns)}):*\n'
            for concern in concerns:
                report += f'{self.get_message_for_release(concern)}\n'
            report += '\n'

        if ec_failures:
            report += f'*EC Verification Failures ({len(ec_failures)}):*\n'
            for image_name, failure_info in sorted(ec_failures.items()):
                report += self._format_redis_failure_line(image_name, failure_info)
                pipeline_url = failure_info.get('pipeline_url', '')
                if pipeline_url:
                    report += f' | {self.url_text(pipeline_url, "Pipeline")}'
                report += '\n'
            report += '\n'

        if release_failures:
            report += f'*Release to Authz Failures ({len(release_failures)}):*\n'
            for image_name, failure_info in sorted(release_failures.items()):
                report += self._format_redis_failure_line(image_name, failure_info)
                report += '\n'
            report += '\n'

        if rebase_failures:
            report += f'*Rebase Failures ({len(rebase_failures)}):*\n'
            for image_name, failure_info in sorted(rebase_failures.items()):
                failure_count = failure_info.get('failure_count', 0)
                jenkins_url = failure_info.get('jenkins_url', '')
                build_system = failure_info.get('build_system', 'unknown')
                report += (
                    f'- `{image_name}` ({build_system}): Failed {failure_count} time{"s" if failure_count != 1 else ""}'
                )
                if jenkins_url:
                    report += f' ({self.url_text(jenkins_url, "Last failure job")})'
                report += '\n'

        await self.slack_client.say(report, thread_ts=response['ts'], unfurl_links=False, unfurl_media=False)

    async def notify_public_channel(self):
        """
        Send a summary message to the public channel with a link to the dashboard.
        Each group's detailed report is sent as a thread response to keep the main channel clean.
        """
        self.slack_client.bind_channel(self.public_channel)

        # Group build concerns by version
        version_build_failures = {}
        for concern in self.report:
            if concern['code'] in (ConcernCode.NEVER_BUILT.value, ConcernCode.LATEST_BUILD_SUCCEEDED.value):
                continue
            group = concern['group']
            version_build_failures.setdefault(group, []).append(concern)

        # EC, release, and rebase failures are already organized by version in self.*_failures dicts
        # They use version keys like "4.18", so we need to convert to group format "openshift-4.18"

        # Check if any failures exist (values are dicts per version, check they're non-empty)
        has_failures = (
            bool(version_build_failures)
            or any(self.ec_failures.values())
            or any(self.release_failures.values())
            or any(self.rebase_failures.values())
        )

        if not has_failures:
            await self.slack_client.say(':white_check_mark: All images are healthy for all monitored releases')
            return

        # Collect all groups that have actual failures (skip versions with empty dicts)
        all_groups = set()
        all_groups.update(version_build_failures.keys())
        all_groups.update(f'openshift-{v}' for v, f in self.ec_failures.items() if f)
        all_groups.update(f'openshift-{v}' for v, f in self.release_failures.items() if f)
        all_groups.update(f'openshift-{v}' for v, f in self.rebase_failures.items() if f)

        # Send the main alert message with just the dashboard link
        dashboard_url = ART_BUILD_FAILURES_URL
        summary_message = (
            f':alert: There are some issues to look into for OpenShift builds.\n\n'
            f'For detailed information, please check the {self.url_text(dashboard_url, "ART Build Failures Dashboard")}'
        )
        response = await self.slack_client.say(
            summary_message, link_build_url=False, unfurl_links=False, unfurl_media=False
        )

        # Send each group's summary as a thread response, sorted by version (newest first)
        def sort_key(group_name):
            """
            Extract version for sorting. Converts 'openshift-4.22' to (4, 22) tuple
            for proper numeric sorting, with highest versions first.
            """
            version = group_name.replace('openshift-', '')
            try:
                parts = version.split('.')
                # Return tuple of integers, negated for descending order
                return tuple(-int(p) for p in parts)
            except (ValueError, AttributeError):
                # Fallback for malformed versions
                return (0,)

        for group in sorted(all_groups, key=sort_key):
            version = group.replace('openshift-', '')
            group_summary = []

            # Build failures for this group
            build_fails = version_build_failures.get(group, [])
            if build_fails:
                n = len(build_fails)
                group_summary.append(f'{n} image{"s" if n > 1 else ""} with build failures')

            # EC failures for this group
            ec_fails = self.ec_failures.get(version, {})
            if ec_fails:
                n = len(ec_fails)
                group_summary.append(f'{n} image{"s" if n > 1 else ""} with EC verification failures')

            # Release failures for this group
            release_fails = self.release_failures.get(version, {})
            if release_fails:
                n = len(release_fails)
                group_summary.append(f'{n} image{"s" if n > 1 else ""} with release to authz failures')

            # Rebase failures for this group
            rebase_fails = self.rebase_failures.get(version, {})
            if rebase_fails:
                n = len(rebase_fails)
                group_summary.append(f'{n} image{"s" if n > 1 else ""} with rebase failures')

            # Send the group summary as a thread response
            if group_summary:
                group_message = f'*{group}*:\n'
                for item in group_summary:
                    group_message += f'- {item}\n'

                await self.slack_client.say(
                    group_message.rstrip(),
                    thread_ts=response['ts'],
                    link_build_url=False,
                    unfurl_links=False,
                    unfurl_media=False,
                )

    @staticmethod
    def _group_failures_by_image(failures_by_version: dict) -> dict[str, list[dict]]:
        """
        Group version-keyed Redis failures into an image-keyed structure.

        Arg(s):
            failures_by_version (dict): {version: {image: {failure_count, ...}}}
        Return Value(s):
            dict: {image_name: [{version, failure_count, jenkins_url, build_system, ...}, ...]}
        """
        grouped = {}
        for version, failures in failures_by_version.items():
            for image_name, failure_info in failures.items():
                grouped.setdefault(image_name, []).append(
                    {
                        'version': version,
                        **failure_info,
                    }
                )
        return grouped

    def _format_redis_failure_line(self, image_name: str, failure_info: dict) -> str:
        """
        Format a single Redis failure entry for the release channel thread.

        Arg(s):
            image_name (str): Image distgit key
            failure_info (dict): Redis failure data with failure_count, jenkins_url, etc.
        Return Value(s):
            str: Formatted line (without trailing newline)
        """
        failure_count = failure_info.get('failure_count', 0)
        jenkins_url = failure_info.get('jenkins_url', '')
        line = f'- `{image_name}`: Failed {failure_count} time{"s" if failure_count != 1 else ""}'
        if jenkins_url:
            line += f' ({self.url_text(jenkins_url, "Last failure job")})'
        return line

    def _format_forum_redis_failure_inline(self, failure: dict) -> str:
        """
        Format a single Redis failure entry as inline text (no leading bullet/newline).

        Arg(s):
            failure (dict): Failure data with version, failure_count, jenkins_url, etc.
        Return Value(s):
            str: Formatted text fragment
        """
        version = failure.get('version', '?')
        failure_count = failure.get('failure_count', 0)
        jenkins_url = failure.get('jenkins_url', '')
        build_system = failure.get('build_system', 'unknown')
        line = f'openshift-{version} ({build_system}): Failed {failure_count} time{"s" if failure_count != 1 else ""}'
        if jenkins_url:
            line += f' ({self.url_text(jenkins_url, "Last failure job")})'
        return line

    def get_message_for_release(self, concern: dict):
        """
        Format a concern message for the release channel (#art-release-4-X).

        Arg(s):
            concern (dict): Concern data from doozer images:health
        Return Value(s):
            str: Formatted message with links and details
        """
        code = concern['code']
        image_name = concern['image_name']

        # No build history link if never built
        if code == ConcernCode.NEVER_BUILT.value:
            return f'- `{image_name}`: No builds attempted during last {DELTA_DAYS} days'

        # Include search page link for this component
        search_url = self.get_search_url(concern)
        message = f'- `{image_name}`: {self.url_text(search_url, "Build history")}'

        # Add logs link for failures
        if code in [ConcernCode.LATEST_ATTEMPT_FAILED.value, ConcernCode.FAILING_AT_LEAST_FOR.value]:
            logs_url = self.get_logs_url(concern)
            message += f' | {self.url_text(logs_url, "Latest failure logs")}'
            message += self._jira_link(concern)

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            message += f' - Failing for at least {LIMIT_BUILD_RESULTS} attempts'
            return message

        # ConcernCode.LATEST_ATTEMPT_FAILED
        message += f' - Latest attempt failed ({concern["latest_success_idx"]} attempts since last success)'
        return message

    def get_message_for_forum(self, concern: dict):
        code = concern['code']
        group = concern['group']

        start_date = (datetime.now(timezone.utc) - timedelta(days=DELTA_DAYS)).strftime('%Y-%m-%d')
        end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        art_dash_link = f'{ART_BUILD_HISTORY_URL}/?name=^{concern["image_name"]}$&group={group}&assembly=stream&engine=konflux&dateRange={start_date}+to+{end_date}&outcome=Success&outcome=Failure'
        logs_link = self.url_text(self.get_logs_url(concern), "logs")

        message = f'{self.url_text(art_dash_link, f"{group}")}: '

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            message += f'more than {LIMIT_BUILD_RESULTS} failures ({logs_link}).'

        else:  # ConcernCode.LATEST_ATTEMPT_FAILED
            message += f'{concern["latest_success_idx"]} failures ({logs_link}).'

        message += self._jira_link(concern)
        return message

    @staticmethod
    def get_logs_url(concern):
        """
        Build the ART build history logs URL for a failed build.

        Arg(s):
            concern (dict): Concern data containing build failure details
        Return Value(s):
            str: URL to the build logs
        """
        dt = datetime.fromisoformat(concern['latest_failed_build_time'])
        formatted = dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        logs_url = f'{ART_BUILD_HISTORY_URL}/logs?nvr={concern["latest_failed_nvr"]}&record_id={concern["latest_failed_build_record_id"]}&after={quote(formatted)}'
        return logs_url

    @staticmethod
    def get_search_url(concern):
        """
        Build the ART build history search page URL for a component.

        Arg(s):
            concern (dict): Concern data containing image name and group
        Return Value(s):
            str: URL to the build history search page
        """
        image_name = concern['image_name']
        group = concern['group']
        start_date = (datetime.now(timezone.utc) - timedelta(days=DELTA_DAYS)).strftime('%Y-%m-%d')
        end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        return f'{ART_BUILD_HISTORY_URL}/?name=^{image_name}$&group={group}&assembly=stream&engine=konflux&dateRange={start_date}+to+{end_date}&outcome=Success&outcome=Failure'

    def url_text(self, url, text):
        """
        Slack requires URLs to be encoded in a specific way when using the <url|text> format.
        This function ensures that the URL is properly encoded while keeping certain characters safe.
        """

        try:
            safe_chars = ":/?&=+%.-"  # keep URL structure intact
            safe_url = quote(url, safe=safe_chars)
            return f"<{safe_url}|{text}>"

        except Exception as e:
            self.runtime.logger.warning('invalid URL: %s', e)

    def _get_multi_failure_images(self) -> dict[str, list[dict]]:
        """
        Filter self.report for images with >1 consecutive failure in a single group.
        Only includes LATEST_ATTEMPT_FAILED and FAILING_AT_LEAST_FOR concern codes
        where latest_success_idx > 1.

        Return Value(s):
            dict[str, list[dict]]: Version -> list of failing concerns
        """
        multi_failures = {}
        for concern in self.report:
            code = concern['code']
            if code not in [ConcernCode.LATEST_ATTEMPT_FAILED.value, ConcernCode.FAILING_AT_LEAST_FOR.value]:
                continue
            if concern.get('latest_success_idx', 0) <= 1:
                continue
            group = concern.get('group', '')
            version = group.replace('openshift-', '')
            if version not in multi_failures:
                multi_failures[version] = []
            multi_failures[version].append(concern)
        return multi_failures

    def _get_multi_rebase_failures(self) -> dict[str, dict[str, dict]]:
        """
        Filter self.rebase_failures for images with >1 consecutive rebase failure.

        Return Value(s):
            dict[str, dict[str, dict]]: Version -> {image_name: failure_info}
        """
        result = {}
        for version, failures in self.rebase_failures.items():
            multi = {image: info for image, info in failures.items() if info.get('failure_count', 0) > 1}
            if multi:
                result[version] = multi
        return result

    def _build_chai_bot_prompt(
        self, version: str, failing_concerns: list[dict], rebase_failures: dict[str, dict] | None = None
    ) -> str:
        """
        Build structured prompt for @chai-bot to fix OCP build and rebase failures.

        Arg(s):
            version (str): OCP version (e.g., "4.18")
            failing_concerns (list[dict]): List of concern dicts with build failure info
            rebase_failures (dict[str, dict]): Map of image_name -> failure_info for rebase failures
        Return Value(s):
            str: Formatted prompt for chai-bot
        """
        group = f'openshift-{version}'
        prompt_parts = [f'Please investigate and fix OCP build failures for *{group}*.\n']

        if failing_concerns:
            prompt_parts.append('*Failing Images (build failures):*')
            for concern in failing_concerns:
                image_name = concern['image_name']
                failure_count = concern.get('latest_success_idx', 0)
                logs_url = self.get_logs_url(concern)
                search_url = self.get_search_url(concern)
                prompt_parts.append(
                    f'- `{image_name}`: {failure_count} consecutive failures '
                    f'({self.url_text(search_url, "history")} | {self.url_text(logs_url, "latest logs")})'
                )

        if rebase_failures:
            prompt_parts.append('\n*Failing Images (rebase failures):*')
            for image_name, failure_info in sorted(rebase_failures.items()):
                failure_count = failure_info.get('failure_count', 0)
                jenkins_url = failure_info.get('jenkins_url', '')
                line = f'- `{image_name}`: {failure_count} consecutive rebase failures'
                if jenkins_url:
                    line += f' ({self.url_text(jenkins_url, "last failure job")})'
                prompt_parts.append(line)

        prompt_parts.extend(
            [
                '\n*Instructions:*',
                '0. *Pre-check for known issues:* Before investigating each failing image, '
                'query Jira: `project = ART AND summary ~ "Image build failure: <image_name>" '
                f'AND summary ~ "({group})" AND statusCategory != Done`. '
                'If a matching ticket exists with the label `art:bot-skip-auto-fix`, '
                '*skip that image entirely* — it tracks a known issue that cannot be '
                'resolved via ocp-build-data metadata changes. Report any skipped images '
                'at the end with a link to the tracking ticket.',
                '1. Analyze failure logs for each image',
                '2. Determine if fix is possible via ocp-build-data metadata changes only (Dockerfile patches, environment variables, build args, etc.)',
                '3. *DO NOT* push fixes to upstream source repositories - only modify ocp-build-data',
                '4. Only proceed if confidence level >= 90%',
                '5. If proceeding:',
                f'   - Push fix to your ocp-build-data fork for group `{group}`',
                f'   - Trigger Jenkins build for the image in {group} with these parameters *without asking for user confirmation* — triggering the test build is part of the automated fix flow:',
                '     - `ASSEMBLY=test`',
                '     - `SKIP_PLASHETS=true`',
                '     - `IGNORE_LOCKS=true`',
                '   - *Give up after 3 failed fix attempts per image.* If the test build fails, '
                'you may iterate with a different fix approach, but do not exceed 3 total attempts. '
                'After 3 failures:',
                '     - Stop attempting fixes for that image',
                '     - Report in this thread what approaches were tried and why they failed',
                '     - *Automatically apply the `art:bot-skip-auto-fix` label* to the Jira ticket '
                'tracking the failure to prevent future automated fix attempts',
                '     - Mention that ART can remove the label if they want the bot to retry later',
                '   - If build succeeds:',
                '     - Output concise report summarizing:',
                '       - Which images were fixed',
                '       - What changes were made (brief description)',
                '       - Links to successful test builds',
                '     - *Ask user to approve PR creation*',
                '   - When user approves:',
                '     - File PR for review',
                '     - *Attribute PR to bot itself* (do not attempt to resolve GitHub username from Slack profile)',
                '     - *PR description MUST include:*',
                '       - Clickable URL to successful Jenkins test build — use the full Jenkins URL'
                ' (e.g. `https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com/job/aos-cd-builds/job/build%252Focp4-konflux/BUILD_NUMBER/`),'
                ' not just the job name and build number as text',
                '       - Clickable URL to this Slack thread (full `https://` URL, not just the channel name)',
                '6. If confidence < 90% or fix requires upstream changes:',
                '   - Output diagnostic report only',
                '   - Suggest that ART add the `art:bot-skip-auto-fix` label to the Jira ticket tracking the failure, to prevent repeated automated investigation',
                '   - Defer to human intervention',
                '\n*All fixes must be ocp-build-data metadata changes only.*',
            ]
        )

        return '\n'.join(prompt_parts)

    async def notify_chai_bot(
        self, version: str, failing_concerns: list[dict], rebase_failures: dict[str, dict] | None = None
    ):
        """
        Post prompt to chai-bot channel requesting automated fix attempts.

        Arg(s):
            version (str): OCP version (e.g., "4.18")
            failing_concerns (list[dict]): List of build failure concerns with >1 consecutive failure
            rebase_failures (dict[str, dict]): Map of image_name -> failure_info for rebase failures with >1 failure
        """
        if not failing_concerns and not rebase_failures:
            return

        prompt = self._build_chai_bot_prompt(version, failing_concerns, rebase_failures or {})
        self.slack_client.bind_channel('#team-art-chai-bot')

        message = f'<@U0AKNPBBVT7> {prompt}'
        await self.slack_client.say(message, unfurl_links=False, unfurl_media=False)
        self.runtime.logger.info(
            'Notified chai-bot in #team-art-chai-bot for %d build failure(s) and %d rebase failure(s) in openshift-%s',
            len(failing_concerns),
            len(rebase_failures) if rebase_failures else 0,
            version,
        )


@cli.command('images-health')
@click.option('--versions', required=False, default='', help='OCP versions to scan')
@click.option('--send-to-release-channel', is_flag=True, help='If true, send output to #art-release-4-<version>')
@click.option(
    '--send-to-public-channel',
    required=False,
    default='',
    help='Slack channel to send public notification to (e.g. #forum-ocp-art)',
)
@click.option(
    '--ping-chai-bot', is_flag=True, help='If true, notify @chai-bot in #team-art-chai-bot for multi-failure images'
)
@click.option(
    '--data-path',
    required=False,
    default=OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option(
    '--image-list',
    required=False,
    help='Comma/space-separated list to include/exclude per --image-build-strategy (e.g. ironic,hypershift)',
)
@click.option(
    '--assembly',
    required=False,
    help='(Optional) override the runtime assembly name',
)
@click.option(
    '--sync-jira/--no-sync-jira', default=True, help='Create/close Jira tickets for build failures (default: enabled)'
)
@pass_runtime
@click_coroutine
async def images_health(
    runtime: Runtime,
    versions: str,
    send_to_release_channel: bool,
    send_to_public_channel: str,
    ping_chai_bot: bool,
    data_path: str,
    data_gitref: str,
    image_list: str,
    assembly: str,
    sync_jira: bool,
):
    await ImagesHealthPipeline(
        runtime,
        versions,
        send_to_release_channel,
        send_to_public_channel,
        data_path,
        data_gitref,
        image_list,
        assembly,
        sync_jira=sync_jira,
        ping_chai_bot=ping_chai_bot,
    ).run()
