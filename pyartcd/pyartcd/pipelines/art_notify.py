import asyncio
import json
import logging
import os
import socket
import ssl
from datetime import datetime, timedelta, timezone
from time import time
from urllib.parse import unquote

import click
import requests
from artcommonlib import redis
from slack_bolt import App
from slack_sdk.errors import SlackApiError

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

SEARCH_WINDOW_HOURS = 8  # Window of last X hours that we consider for our failed builds search
RELEASE_ARTIST_HANDLE = 'release-artists'
ART_KONFLUX_TEMPLATE_REPO = 'openshift-priv/art-konflux-template'
ART_DOCS_TASK_BUNDLES_URL = 'https://art-docs.engineering.redhat.com/konflux/update-konflux-task-bundles/'


class SSLCertificateChecker:
    def __init__(self):
        # List of URLs to check
        self.urls = [
            "art-dash.engineering.redhat.com",
            "ocp-artifacts.engineering.redhat.com",
            "art-docs.engineering.redhat.com",
        ]
        self.logger = logging.getLogger(__name__)

    def _get_certificate_expiry_date(self, url):
        self.logger.info('Getting expiry date for %s', url)

        context = ssl.create_default_context()
        with socket.create_connection((url, 443), timeout=3.0) as sock:
            with context.wrap_socket(sock, server_hostname=url) as ssock:
                cert = ssock.getpeercert()

        expiry_str = cert['notAfter']
        return datetime.strptime(expiry_str, '%b %d %H:%M:%S %Y %Z')

    async def _check_url(self, url, days_in_advance=30):
        """Asynchronously check a URL and return alert if certificate expires soon."""
        self.logger.info('Checking URL %s', url)
        notification_threshold = datetime.now() + timedelta(days=days_in_advance)

        try:
            expiry_date = await asyncio.to_thread(self._get_certificate_expiry_date, url)
        except Exception as e:
            return f"ERROR: Failed to check {url}: {e}"

        if expiry_date < notification_threshold:
            return f"ALERT: The SSL certificate for {url} will expire on {expiry_date}"
        return None

    async def check_expired_certificates(self):
        """Asynchronously check all URLs and return a summary of expiring certificates."""
        results = await asyncio.gather(*[self._check_url(url) for url in self.urls])
        expired_certificates = [res for res in results if res]
        return "\n".join(expired_certificates) if expired_certificates else None


class ArtNotifyPipeline:
    def __init__(self, runtime, channel):
        self.runtime = runtime
        self.channel = channel
        self.logger = logging.getLogger(__name__)

        self.app = App(
            token=os.getenv('SLACK_API_TOKEN'),
            signing_secret=os.getenv('SLACK_SIGNING_SECRET'),
        )

    def _get_failed_jobs_text(self):
        art_bot_jenkins_userid = 'openshift-art'
        projects = ["aos-cd-builds", "scheduled-builds", "maintenance"]
        failed_jobs = []

        # API reference
        # for a job: <jenkins_url>/job/<project>/job/<job_name>/api/json?pretty=true
        # for a build: <jenkins_url>/job/<project>/job/<job_name>/<build_number>/api/json?pretty=true
        # by default jenkins returns last 100 builds for each job, which is fine for us
        # some very frequent running jobs will have more than 100 builds, but we only report on the last 100
        query = "?tree=jobs[name,url,builds[number,result,timestamp,displayName,actions[causes[userId]]]]"
        now = datetime.now(timezone.utc)
        for project in projects:
            api_url = f"{os.getenv('JENKINS_URL').rstrip('/')}/job/{project}/api/json"
            response = requests.get(
                api_url + query, auth=(os.getenv('JENKINS_SERVICE_ACCOUNT'), os.getenv('JENKINS_SERVICE_ACCOUNT_TOKEN'))
            )
            response.raise_for_status()
            data = response.json()
            self.logger.info(f"Fetched {len(data['jobs'])} jobs from {api_url}")

            for job in data['jobs']:
                if not job.get('builds', None):
                    continue

                job_name = unquote(job['name'])
                self.logger.info(f"Found {len(job['builds'])} builds for job {job_name}")
                failed_job_ids = []
                total_eligible_builds = 0
                oldest_build_hours_ago = None
                for build in job['builds']:
                    dt = datetime.fromtimestamp(build['timestamp'] / 1000, tz=timezone.utc)
                    td = now - dt
                    started_hours_ago = td.days * 24 + td.seconds // 3600
                    if oldest_build_hours_ago is None or oldest_build_hours_ago < started_hours_ago:
                        oldest_build_hours_ago = started_hours_ago
                    if started_hours_ago > SEARCH_WINDOW_HOURS:
                        continue

                    # Filter all builds that were not triggered by our automation account
                    # We do not want to report on these since they are manually triggered and
                    # would be monitored by whoever triggered them
                    # Builds which are triggered by another build will not have userId
                    # so we include them by default
                    user_id = next(
                        (
                            cause.get('userId')
                            for action in build.get('actions', [])
                            for cause in action.get('causes', [])
                        ),
                        None,
                    )
                    if user_id and user_id != art_bot_jenkins_userid:
                        continue
                    total_eligible_builds += 1

                    # this is a special case for build-sync job
                    # we want to filter out UNVIABLE builds since they are not real failures
                    if job_name == 'build/build-sync' and 'UNVIABLE' in build['displayName']:
                        self.logger.info(f"unviable build-sync run found: {build['number']}. skipping")
                        continue

                    if build['result'] == 'FAILURE':
                        failed_job_ids.append(build['number'])

                if oldest_build_hours_ago < SEARCH_WINDOW_HOURS:
                    self.logger.info(
                        f"[WARNING] Oldest build in api response for job {job_name} started {oldest_build_hours_ago} hours ago. There maybe older failed builds that were not fetched."
                    )
                if len(failed_job_ids) > 0:
                    fail_rate = (len(failed_job_ids) / total_eligible_builds) * 100
                    self.logger.info(
                        f"Job {job_name} failed {len(failed_job_ids)} times out of {total_eligible_builds} builds. Fail rate: {fail_rate:.1f}%"
                    )
                    failed_jobs.append((job_name, failed_job_ids, fail_rate, job['url']))

        if failed_jobs:
            # sort by highest fail rate
            failed_jobs.sort(key=lambda x: x[2], reverse=True)
            failed_jobs_list = []
            for job_name, failed_job_ids, fail_rate, job_url in failed_jobs:
                link = self._slack_link(job_name, job_url)
                text = f"* {link}: {len(failed_job_ids)} "
                # sort job ids by most recent
                failed_job_ids.sort(reverse=True)
                # only link to the first 3
                for i, job_id in enumerate(failed_job_ids[:3]):
                    text += f"[{self._slack_link(job_name, job_url, job_id=job_id, text=i + 1)}] "
                fail_rate_text = f"Fail rate: {fail_rate:.1f}%"
                text += fail_rate_text
                failed_jobs_list.append(text)
                self.logger.info(f"* {job_name}: {len(failed_job_ids)} {failed_job_ids[:3]}. {fail_rate_text}")
            failed_jobs_list = "\n".join(failed_jobs_list)
            failed_jobs_text = f"Failed jobs in last `{SEARCH_WINDOW_HOURS}` hours triggered by `{art_bot_jenkins_userid}`: \n{failed_jobs_list}"
            return failed_jobs_text
        return ''

    def _get_konflux_task_update_pr(self):
        self.logger.info('Checking for Konflux template update PRs...')
        github_token = os.getenv('GITHUB_TOKEN')
        if not github_token:
            raise ValueError('GITHUB_TOKEN environment variable must be set')

        api_url = f'https://api.github.com/repos/{ART_KONFLUX_TEMPLATE_REPO}/pulls'
        headers = {
            'Authorization': f'token {github_token}',
            'Accept': 'application/vnd.github.v3+json',
        }

        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            prs = response.json()
        except requests.RequestException as e:
            self.logger.error(f'Failed to fetch PRs from {ART_KONFLUX_TEMPLATE_REPO}: {e}')
            return None

        for pr in prs:
            if pr['state'] == 'open' and pr['title'] == 'Update Konflux references':
                pr_url = pr['html_url']
                self.logger.info(f'Found open Konflux template update PR: {pr_url}')

                return f'* <{pr_url}|Review Konflux Task Update PR>, refer docs ({ART_DOCS_TASK_BUNDLES_URL})'

        self.logger.info('No open Konflux template update PRs found.')
        return None

    @staticmethod
    def _slack_link(job_name, job_url, job_id=None, text=None):
        link = job_url
        if job_id:
            link += f"{job_id}/console"
        if not text:
            if job_id:
                text = f"#{job_id}"
            else:
                text = unquote(job_name)
        return f"<{link}|{text}>"

    def _get_messages(self):
        """
        Fetch all Slack messages with the :art-attention: emoji
        """

        all_matches = []
        next_cursor = '*'
        while next_cursor:
            # https://api.slack.com/methods/search.messages#examples
            slack_response = self.app.client.search_messages(
                token=os.getenv('SLACK_USER_TOKEN'),
                query='has::art-attention: -has::art-attention-resolved:',
                cursor=next_cursor,
            )
            messages = slack_response.get('messages', {})
            all_matches.extend(messages.get('matches', []))

            # https://api.slack.com/docs/pagination
            response_metadata = slack_response.get('response_metadata', {})
            next_cursor = response_metadata.get('next_cursor', None)

        all_matches = sorted(all_matches, key=lambda m: m.get('ts', '0.0'))
        self.logger.info('Found matching messages: \n%s', json.dumps(all_matches, indent=4))
        return all_matches

    def _notify_messages(self, failed_jobs_text, messages, extra_notifications):
        header_text = "Currently unresolved ART threads"
        fallback_text = header_text

        response_messages = []
        channel_warnings = {}
        current_epoch_time = time()

        for match in messages:
            channel_id = match.get('channel', {}).get('id', '')
            channel_name = match.get('channel', {}).get('name', 'Unknown')
            channel_handle = f'<https://redhat-internal.slack.com/archives/{channel_id}|#{channel_name}>'

            if len(channel_id) > 0:
                try:
                    self.app.client.conversations_info(channel=channel_id)

                except SlackApiError:
                    msg = (
                        f':warning: Found an unresolved thread in channel {channel_handle}'
                        f' but the channel is not accessible by the bot. Please invite <@art-bot> to {channel_handle}'
                    )
                    if not channel_warnings.get(channel_id, None):
                        channel_warnings[channel_id] = msg
                    continue

            text = match.get('text', 'Link')
            permalink = match.get('permalink', None)
            if not permalink:
                permalink = 'about:blank'
                text = json.dumps(match)

            fallback_text += f'\n{permalink}'
            timestamp = int(float(match.get('ts', '0.0')))
            str_date = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            age = current_epoch_time - timestamp
            age_type = 'seconds'
            if age > 120:
                age /= 60
                age_type = 'minutes'

                if age >= 60:
                    age /= 60
                    age_type = 'hours'

                    if age >= 48:
                        age /= 24
                        age_type = 'days'

            age = int(age)

            # Block builder: https://app.slack.com/block-kit-builder/T04714LEPHA#%7B%22blocks%22:%5B%5D%7D
            snippet = ' '.join(text.split(' ')[0:30])

            # Slack includes an arrow character in the text if should be replaced by rich text elements (e.g. a n@username).
            # We just remove them since we are just trying for a short summary.
            snippet = snippet.replace('\ue006', '...')
            response_messages.append(
                f"*Channel:* {channel_handle}\n*Date:* {str_date}Z\n*Age:* {age} {age_type}\n*Message:* <{permalink}|Link>\n*Snippet:* {snippet}..."
            )

        n_threads = len(response_messages)
        if failed_jobs_text:
            n_threads += 1
        n_threads += len(extra_notifications)

        header_block = [
            {"type": "header", "text": {"type": "plain_text", "text": f"{header_text} ({n_threads})", "emoji": True}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"Attention @{RELEASE_ARTIST_HANDLE}"}},
        ]

        if self.runtime.dry_run:
            self.logger.info("[DRY RUN] Would have messaged to Slack")

            for warning in channel_warnings.values():
                self.logger.info(warning)

            for response_message in response_messages:
                self.logger.info(response_message)

            if failed_jobs_text:
                self.logger.info(failed_jobs_text)

            for notification in extra_notifications:
                self.logger.info(notification)

        else:
            # https://api.slack.com/methods/chat.postMessage#examples
            response = self.app.client.chat_postMessage(
                channel=self.channel,
                text=f'@{RELEASE_ARTIST_HANDLE} - {fallback_text}',
                blocks=header_block,
                unfurl_links=False,
            )

            # Post warnings about inaccessible channels first
            for warning in channel_warnings.values():
                self.app.client.chat_postMessage(channel=self.channel, text=warning, thread_ts=response['ts'])

            for response_message in response_messages:
                self.app.client.chat_postMessage(
                    channel=self.channel, text=response_message, thread_ts=response['ts']
                )  # use the timestamp from the response

            if failed_jobs_text:
                self.app.client.chat_postMessage(channel=self.channel, text=failed_jobs_text, thread_ts=response['ts'])

            for notification in extra_notifications:
                self.app.client.chat_postMessage(channel=self.channel, text=notification, thread_ts=response['ts'])

    async def _get_rebase_failures(self):
        """
        Read the rebase failure counters from Redis, for both Brew and Konflux build systems
        """

        failures = {}

        redis_branch = 'count:rebase-failure:konflux'
        self.logger.info(f'Reading from {redis_branch}...')
        all_keys = await redis.get_keys(f'{redis_branch}:*')

        # Filter out URL and failure suffix keys to get only the base image keys
        # Keys look like: count:rebase-failure:konflux:4.18:image-name:failure
        #                 count:rebase-failure:konflux:4.18:image-name:url
        # We want to get: count:rebase-failure:konflux:4.18:image-name
        failed_images = [key for key in all_keys if key.endswith(':failure')]
        # Remove the :failure suffix to get base keys
        base_keys = [key[:-8] for key in failed_images]  # Remove ':failure' (8 chars)

        if base_keys:
            # Get the counter values from keys ending with :failure
            fail_counters = await redis.get_multiple_values(failed_images)
            # Also get the corresponding job URLs
            url_keys = [f'{base_key}:url' for base_key in base_keys]
            job_urls = await redis.get_multiple_values(url_keys)

            for base_key, fail_counter, job_url in zip(base_keys, fail_counters, job_urls):
                image_name = base_key.split(':')[-1]
                version = base_key.split(':')[-2]
                failures.setdefault('konflux', {}).setdefault(version, {})[image_name] = {
                    'count': fail_counter,
                    'url': job_url,
                }

        return failures

    async def _notify_rebase_failures(self):
        """
        Notify about rebase failures in Brew and Konflux
        """

        rebase_failures = await self._get_rebase_failures()

        if not rebase_failures:
            self.logger.info('No rebase failures found.')
            return

        self.logger.info('Found rebase failures:')
        self.logger.info(json.dumps(rebase_failures, indent=4))

        if self.runtime.dry_run:
            self.logger.info("[DRY RUN] Would have notified rebase failures")
            return

        for engine, versions in rebase_failures.items():
            for version, failures in versions.items():
                major, minor = version.split('.')
                channel = f'#art-release-{major}-{minor}'
                response = self.app.client.chat_postMessage(
                    channel=channel,
                    text=f":warning: {len(failures)} image{'s' if len(failures) > 1 else ''} failed to rebase in *{engine.capitalize()}*",
                    link_names=True,
                )

                for image, failure_info in failures.items():
                    counter = failure_info['count']
                    job_url = failure_info.get('url')
                    if job_url:
                        text = f'- `{image}`: failed {counter} times - <{job_url}|View Job>'
                    else:
                        text = f'- `{image}`: failed {counter} times'
                    self.app.client.chat_postMessage(channel=channel, text=text, thread_ts=response['ts'])

    async def run(self):
        failed_jobs_text = self._get_failed_jobs_text()
        messages = self._get_messages()
        expired_certificates = await SSLCertificateChecker().check_expired_certificates()
        konflux_pr_text = self._get_konflux_task_update_pr()

        extra_notifications = [n for n in [expired_certificates, konflux_pr_text] if n]

        if any([failed_jobs_text, messages, extra_notifications]):
            self._notify_messages(failed_jobs_text, messages, extra_notifications)

        else:
            self.logger.info('No messages matching attention emoji criteria and no failed jobs found')
            self.app.client.chat_postMessage(
                channel=self.channel, text=':check: no unresolved threads / job failures found'
            )

        await self._notify_rebase_failures()


@cli.command('art-notify', help='Rebase and build FBC segments for OLM operators')
@click.option('--channel', required=True, help='Where to send ART notifications')
@pass_runtime
@click_coroutine
async def art_notify(runtime: Runtime, channel: str):
    pipeline = ArtNotifyPipeline(runtime, channel)
    await pipeline.run()
