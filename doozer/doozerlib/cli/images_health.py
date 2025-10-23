import asyncio
import datetime
import json
import logging
from enum import Enum

import click
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.model import Missing
from tenacity import retry, stop_after_attempt, wait_fixed

from doozerlib import Runtime
from doozerlib.cli import cli, click_coroutine, pass_runtime

DELTA_DAYS = 90  # look at latest 90 days
LIMIT_BUILD_RESULTS = 100  # how many build records to fetch from DB


class ConcernCode(Enum):
    NEVER_BUILT = 'NEVER_BUILT'
    LATEST_ATTEMPT_FAILED = 'LATEST_ATTEMPT_FAILED'
    FAILING_AT_LEAST_FOR = 'FAILING_AT_LEAST_FOR'
    LATEST_BUILD_SUCCEEDED = 'LATEST_BUILT_SUCCEEDED'


class Concern:
    def __init__(
        self,
        image_name: str,
        code: str,
        latest_success_idx: int = None,
        latest_failed_job_url: str = None,
        latest_attempt_task_url: str = None,
        latest_successful_task_url: str = None,
        latest_failed_nvr: str = None,
        latest_built_nvr: str = None,
        latest_failed_build_record_id: str = None,
        latest_failed_build_time: datetime.datetime = None,
        group: str = None,
        for_release: bool = True,
    ):
        self.image_name = image_name
        self.code = code
        self.latest_success_idx = latest_success_idx
        self.latest_failed_job_url = latest_failed_job_url
        self.latest_attempt_task_url = latest_attempt_task_url
        self.latest_successful_task_url = latest_successful_task_url
        self.latest_failed_nvr = latest_failed_nvr
        self.latest_built_nvr = latest_built_nvr
        self.latest_failed_build_record_id = latest_failed_build_record_id
        self.latest_failed_build_time = latest_failed_build_time
        self.group = group
        self.for_release = for_release

    def to_dict(self):
        return self.__dict__.copy()


class ImagesHealthPipeline:
    def __init__(self, runtime: Runtime, limit: int, group: str = None, assembly: str = None):
        self.runtime = runtime
        self.limit = limit
        self.start_search = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=DELTA_DAYS)
        self.concerns = []
        self.logger = logging.getLogger(__name__)
        self.runtime.konflux_db.bind(KonfluxBuildRecord)
        # Set group and assembly overrides
        self.group = group or self.runtime.group_config.name  # default to runtime group
        self.assembly = assembly or 'stream'  # default to 'stream' assembly

    async def run(self):
        # Gather concerns for all images we build with Konflux
        tasks = [
            self.get_concerns(image_meta)
            for image_meta in self.runtime.image_metas()
            if not image_meta.config.konflux.mode == 'disabled' and not image_meta.mode == 'disabled'
        ]
        await asyncio.gather(*tasks)

        # We should now have a dict of qualified_key => [concern, ...]
        if not self.concerns:
            self.logger.info('No concerns to report!')
        click.echo(
            json.dumps(self.concerns, indent=4, default=lambda o: o.to_dict() if isinstance(o, Concern) else str(o))
        )

    async def get_concerns(self, image_meta):
        for_release = image_meta.config.for_release
        if for_release is Missing:
            for_release = True

        builds = await self.query(image_meta)
        if not builds:
            message = f'Image build for {image_meta.distgit_key} has never been attempted during last {DELTA_DAYS} days'
            self.logger.info(message)
            self.add_concern(
                Concern(
                    image_name=image_meta.distgit_key,
                    code=ConcernCode.NEVER_BUILT.value,
                    for_release=for_release,
                )
            )
            return

        latest_success_idx = -1
        latest_success_bi_task_url = ''

        for idx, build in enumerate(builds):
            if build.outcome == KonfluxBuildOutcome.SUCCESS:
                latest_success_idx = idx
                latest_success_bi_task_url = build.build_pipeline_url
                break

        latest_attempt_task_url = builds[0].build_pipeline_url

        if latest_success_idx == 0:
            # The latest attempt was a success
            self.add_concern(
                Concern(
                    code=ConcernCode.LATEST_BUILD_SUCCEEDED.value,
                    image_name=image_meta.distgit_key,
                    latest_built_nvr=builds[0].nvr,
                )
            )

        elif latest_success_idx == -1:
            # No success record was found: add a concern
            self.add_concern(
                Concern(
                    image_name=image_meta.distgit_key,
                    code=ConcernCode.FAILING_AT_LEAST_FOR.value,
                    latest_failed_job_url=builds[0].art_job_url,
                    latest_attempt_task_url=latest_attempt_task_url,
                    latest_failed_nvr=builds[0].nvr,
                    latest_failed_build_record_id=builds[0].record_id,
                    latest_failed_build_time=builds[0].start_time,
                    for_release=for_release,
                ),
            )

        if latest_success_idx <= 3:
            # The latest attempt was a failure, but there was a success within the last 3 attempts: skip notification
            self.logger.info(
                f'Latest attempt for {image_meta.distgit_key} failed, but the one before it succeeded, skipping notification.'
            )
            return

        else:
            # The latest attempt was a failure, and the last success was more than 3 attempts ago: add a concern
            self.add_concern(
                Concern(
                    image_name=image_meta.distgit_key,
                    code=ConcernCode.LATEST_ATTEMPT_FAILED.value,
                    latest_success_idx=latest_success_idx,
                    latest_successful_task_url=latest_success_bi_task_url,
                    latest_failed_job_url=builds[0].art_job_url,
                    latest_attempt_task_url=latest_attempt_task_url,
                    latest_failed_nvr=builds[0].nvr,
                    latest_failed_build_record_id=builds[0].record_id,
                    latest_failed_build_time=builds[0].start_time,
                    for_release=for_release,
                ),
            )

    def add_concern(self, concern: Concern):
        concern.group = self.runtime.group
        self.concerns.append(concern)

    @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
    async def query(self, image_meta):
        """
        For 'stream' assembly only, query 'builds' table  for component 'name' from BigQuery
        """
        results = [
            build
            async for build in self.runtime.konflux_db.search_builds_by_fields(
                start_search=self.start_search,
                where={
                    'name': image_meta.distgit_key,
                    'group': self.group,
                    'engine': 'konflux',
                    'assembly': self.assembly,
                },
                order_by='start_time',
                limit=self.limit,
            )
        ]
        return results


@cli.command("images:health", short_help="Create a health report for this image group (requires DB read)")
@click.option('--limit', default=LIMIT_BUILD_RESULTS, help='How far back in the database to search for builds')
@click.option('--group', required=False, help='(Optional) override the group name from the config')
@click.option('--assembly', required=False, help='(Optional) override the runtime assembly name')
@click_coroutine
@pass_runtime
async def images_health(runtime, limit, group, assembly):
    runtime.initialize(clone_distgits=False, clone_source=False)
    await ImagesHealthPipeline(runtime, limit, group, assembly).run()
