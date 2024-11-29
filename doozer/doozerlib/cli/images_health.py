import asyncio
import datetime
import json
import logging
import urllib.parse
import click
from tenacity import retry, stop_after_attempt, wait_fixed

from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBuildRecord
from doozerlib import Runtime
from doozerlib.cli import cli, pass_runtime, click_coroutine
from doozerlib.constants import ART_BUILD_HISTORY_URL

DELTA_DAYS = 7  # look at latest 7 days


class ImagesHealthPipeline:
    def __init__(self, runtime: Runtime, limit: int, url_markup: str):
        self.runtime = runtime
        self.limit = limit
        self.url_markup = url_markup
        self.start_search = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=DELTA_DAYS)
        self.concerns = {}
        self.logger = logging.getLogger(__name__)
        self.runtime.konflux_db.bind(KonfluxBuildRecord)

    def generate_art_dash_history_link(self, dg_name):
        base_url = f"{ART_BUILD_HISTORY_URL}/search"

        # Validating essential parameters
        if not dg_name or not self.runtime or not self.runtime.group_config or not self.runtime.group_config.name:
            raise ValueError("Missing essential parameters for generating Art-Dash link")

        formatted_dg_name = dg_name.split("/")[-1]

        params = {
            "group": self.runtime.group_config.name,
            "name": f'^{formatted_dg_name}$',
            "engine": "brew",
            "assembly": "stream",
            "outcome": "both",
            "art-job-url": "",
            "after": f'{self.start_search.year}-{self.start_search.month}-{self.start_search.day}'
        }

        query_string = urllib.parse.urlencode(params)
        return f"{base_url}?{query_string}"

    async def run(self):
        # Gather concerns for all images
        await asyncio.gather(*[self.get_concerns(image_meta) for image_meta in self.runtime.image_metas()])

        # Filter out results without concerns
        concerns = {k: v for k, v in self.concerns.items() if v}

        # We should now have a dict of qualified_key => [concern, ...]
        if not concerns:
            self.logger.info('No concerns to report!')
        click.echo(json.dumps(concerns, indent=4))

    def url_text(self, url, text):
        if self.url_markup == 'slack':
            return f'<{url}|{text}>'
        if self.url_markup == 'github':
            return f'[{text}]({url})'
        raise IOError(f'Unknown markup mode: {self.url_markup}')

    async def get_concerns(self, image_meta):
        key = image_meta.distgit_key
        self.concerns[key] = []

        builds = await self.query(image_meta)

        if not builds:
            self.logger.info('Image build for %s has never been attempted during last %s days',
                             image_meta.distgit_key, DELTA_DAYS)
            return

        latest_success_idx = -1
        latest_success_bi_task_url = ''
        latest_success_bi_build_id = ''
        latest_success_bi_dt = ''

        for idx, build in enumerate(builds):
            if build.outcome == KonfluxBuildOutcome.SUCCESS:
                latest_success_idx = idx
                latest_success_bi_task_url = build.build_pipeline_url
                latest_success_bi_build_id = build.build_id
                latest_success_bi_dt = build.start_time
                break

        latest_attempt_build_url = builds[0].art_job_url
        latest_attempt_task_url = builds[0].build_pipeline_url
        oldest_attempt_bi_dt = builds[-1].start_time

        # Generate the Art-Dash link
        art_dash_link = self.generate_art_dash_history_link(key)

        if latest_success_idx != 0:
            msg = (f'Latest attempt {self.url_text(latest_attempt_task_url, "failed")} '
                   f'({self.url_text(latest_attempt_build_url, "jenkins job")}); ')

            # The latest attempt was a failure
            if latest_success_idx == -1:
                # No success record was found
                msg += f'Failing for at least the last {len(builds)} attempts / {oldest_attempt_bi_dt}'
            else:
                msg += (f'Last {self.url_text(latest_success_bi_task_url, "success")} '
                        f'was {latest_success_idx} attempts ago')

            # Append the Art-Dash link to the message
            msg += f'. {self.url_text(art_dash_link, "Details")}'
            self.concerns[key].append(msg)

        else:
            if latest_success_bi_dt < datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=14):
                # Successful build is older than 2 weeks
                self.concerns[key].append(
                    f'Last {self.url_text(latest_success_bi_task_url, "build")} '
                    f'({self.url_text(latest_success_bi_build_id, "jenkins job")}) was over two weeks ago.')

    @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
    async def query(self, image_meta):
        """
        For 'stream' assembly only, query 'builds' table  for component 'name' from BigQuery
        """

        results = await self.runtime.konflux_db.search_builds_by_fields(
            start_search=self.start_search,
            where={
                'name': image_meta.distgit_key,
                'group': self.runtime.group_config.name,
                'engine': 'brew',
                'assembly': 'stream'
            },
            order_by='start_time',
            limit=self.limit)
        return [self.runtime.konflux_db.from_result_row(result) for result in results]


@cli.command("images:health", short_help="Create a health report for this image group (requires DB read)")
@click.option('--limit', default=100, help='How far back in the database to search for builds')
@click.option('--url-markup', default='slack', help='How to markup hyperlinks (slack, github)')
@click_coroutine
@pass_runtime
async def images_health(runtime, limit, url_markup):
    runtime.initialize(clone_distgits=False, clone_source=False)
    await ImagesHealthPipeline(runtime, limit, url_markup).run()
