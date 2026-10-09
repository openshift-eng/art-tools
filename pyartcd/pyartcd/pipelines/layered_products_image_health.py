"""
Collect and report health information for layered-product image groups.

The command in this module is intentionally separate from the OCP and OKD
image-health pipelines because layered products use product-specific build
variants and an aggregated Slack report.
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import click
from artcommonlib import exectools
from artcommonlib.variants import BuildVariant, get_build_variant_for_product
from doozerlib.cli.images_health import DELTA_DAYS, LIMIT_BUILD_RESULTS, ConcernCode
from doozerlib.constants import ART_BUILD_FAILURES_URL, ART_BUILD_HISTORY_URL

from pyartcd import util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.runtime import Runtime
from pyartcd.slack import SlackClient


def _parse_groups(groups: str) -> list[str]:
    """
    Parse a comma-separated layered-product group list.

    Args:
        groups: Comma-separated group names.

    Returns:
        Group names with whitespace removed and duplicates removed in order.

    Raises:
        ValueError: If no group name is provided.
    """
    parsed_groups = list(dict.fromkeys(group.strip() for group in groups.split(",") if group.strip()))
    if not parsed_groups:
        raise ValueError("At least one layered-product group is required")
    return parsed_groups


@dataclass
class LayeredProductHealthReport:
    """
    Store health information collected for one layered-product group.
    """

    group: str
    product: str
    variant: BuildVariant | None
    build_concerns: list[dict]
    build_failures: dict[str, dict]
    its_failures: dict[str, dict]
    release_failures: dict[str, dict]
    rebase_failures: dict[str, dict]
    error: str | None = None


class LayeredProductsImageHealthPipeline:
    """
    Collect and report health data for layered-product image groups.
    """

    def __init__(
        self,
        runtime: Runtime,
        groups: str,
        data_path: str,
        data_gitref: str,
        assembly: str,
        image_list: str,
    ) -> None:
        """
        Initialize the layered-product health pipeline.

        Args:
            runtime: The pyartcd runtime.
            groups: Comma-separated layered-product groups.
            data_path: ocp-build-data repository path.
            data_gitref: Optional ocp-build-data Git reference.
            assembly: Assembly to inspect.
            image_list: Optional comma-separated image filter.
        """
        self.runtime = runtime
        self.groups = _parse_groups(groups)
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.assembly = assembly
        self.image_list = [image.strip() for image in image_list.split(",") if image.strip()]
        self.slack_client = self.runtime.new_slack_client()
        self._doozer_working = self.runtime.working_dir / "doozer_working"

    async def run(self) -> None:
        """
        Collect all configured groups and publish one message per variant.
        """
        reports = await asyncio.gather(*(self._collect_group_safely(group) for group in self.groups))
        reports = sorted(reports, key=lambda report: report.group)
        await self._notify_slack(reports)

        failed_groups = [report.group for report in reports if report.error]
        if failed_groups:
            raise RuntimeError(f"Layered product health collection failed for: {', '.join(failed_groups)}")

    async def _collect_group_safely(self, group: str) -> LayeredProductHealthReport:
        """
        Collect one group while preserving an error report for aggregation.

        Args:
            group: Layered-product group name.

        Returns:
            A successful group report or a report containing the collection error.
        """
        try:
            return await self._collect_group(group)
        except Exception as error:
            self.runtime.logger.exception("Failed to collect layered product health for %s", group)
            return LayeredProductHealthReport(
                group=group,
                product="unknown",
                variant=None,
                build_concerns=[],
                build_failures={},
                its_failures={},
                release_failures={},
                rebase_failures={},
                error=str(error),
            )

    async def _collect_group(self, group: str) -> LayeredProductHealthReport:
        """
        Collect variant-aware health data for one layered-product group.

        Args:
            group: Layered-product group name.

        Returns:
            Health data collected for the group.
        """
        group_config = await util.load_group_config(
            group=group,
            assembly=self.assembly,
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )
        product = group_config.get("product")
        if not product:
            raise ValueError(f"No product found in group config for {group}")
        variant = get_build_variant_for_product(product)

        build_failures, its_failures, release_failures, rebase_failures = await asyncio.gather(
            util.get_counter_failures(
                "build-failure",
                group=group,
                logger=self.runtime.logger,
                build_variant=variant.value,
            ),
            util.get_counter_failures(
                "ec-failure",
                group=group,
                logger=self.runtime.logger,
                build_variant=variant.value,
            ),
            util.get_counter_failures(
                "release-failure",
                group=group,
                logger=self.runtime.logger,
                build_variant=variant.value,
            ),
            util.get_rebase_failures(
                group=group,
                branches=["rebase-failure"],
                build_systems=["konflux"],
                build_variant=variant.value,
                logger=self.runtime.logger,
            ),
        )

        failure_images = set(build_failures) | set(its_failures) | set(release_failures)
        if self.image_list:
            failure_images &= set(self.image_list)

        build_concerns = await self._get_build_concerns(group, variant, failure_images)
        return LayeredProductHealthReport(
            group=group,
            product=product,
            variant=variant,
            build_concerns=build_concerns,
            build_failures=build_failures,
            its_failures=its_failures,
            release_failures=release_failures,
            rebase_failures=rebase_failures,
        )

    async def _get_build_concerns(
        self,
        group: str,
        variant: BuildVariant,
        image_names: set[str],
    ) -> list[dict]:
        """
        Return build concerns for affected images.

        Args:
            group: Layered-product group name.
            variant: Product-specific build variant.
            image_names: Redis-reported image names to inspect.

        Returns:
            Doozer health concerns for the group.
        """
        if not image_names:
            return []

        valid_images = await self._get_valid_images(group, variant)
        filtered_images = image_names & valid_images
        if not filtered_images:
            return []

        group_param = group
        if self.data_gitref:
            group_param += f"@{self.data_gitref}"
        working_dir = self._doozer_working / group
        command = [
            "doozer",
            f"--working-dir={working_dir}",
            f"--data-path={self.data_path}",
            f"--group={group_param}",
            f"--assembly={self.assembly}",
            "--build-system=konflux",
            f"--variant={variant.value}",
            f"--images={','.join(sorted(filtered_images))}",
            "images:health",
        ]
        _, output, _ = await exectools.cmd_gather_async(command, stderr=None)
        return json.loads(output.strip()) if output.strip() else []

    async def _get_valid_images(self, group: str, variant: BuildVariant) -> set[str]:
        """
        Return image names currently defined for a layered-product group.

        Args:
            group: Layered-product group name.
            variant: Product-specific build variant.

        Returns:
            Image names available in the selected group and variant.
        """
        images = await util.get_group_images(
            group=group,
            assembly=self.assembly,
            build_system="konflux",
            working_dir=self._doozer_working / group,
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
            variant=variant.value,
        )
        return set(images)

    def _build_variant_message(self, variant_name: str, reports: list[LayeredProductHealthReport]) -> str:
        """
        Build one Slack summary message for a layered-product variant.

        Args:
            variant_name: Product-specific build variant.
            reports: Health reports for groups belonging to the variant.

        Returns:
            Slack-formatted variant summary.
        """
        message_parts = [f":alert: Layered product image health report for `{variant_name}`:"]
        for report in reports:
            if report.error:
                message_parts.append(f"- `{report.group}`: :warning: incomplete ({report.error})")
                continue

            failure_parts = self._get_failure_parts(report)
            if failure_parts:
                message_parts.append(f"- `{report.group}`: {', '.join(failure_parts)}")
            else:
                message_parts.append(f"- `{report.group}`: :white_check_mark: healthy")

        message_parts.append(f"\nFor details, see <{ART_BUILD_FAILURES_URL}|ART Build Failures Dashboard>.")
        return "\n".join(message_parts)

    def _build_group_message(self, report: LayeredProductHealthReport) -> str:
        """
        Build a detailed Slack thread message for one product group.

        Args:
            report: Group health report to format.

        Returns:
            Slack-formatted group details.
        """
        if report.error:
            return f"*{report.group}*\n:warning: Report incomplete: {report.error}"
        return self._build_group_details(report)

    def _build_group_details(self, report: LayeredProductHealthReport) -> str:
        """
        Build detailed Slack report sections for one product group.

        Args:
            report: Group health report to format.

        Returns:
            Slack-formatted group details.
        """
        sections = [f"*{report.group}*"]
        build_concerns = self._get_failure_concerns(report.build_concerns)
        if build_concerns:
            lines = [f"*Build Failures ({len(build_concerns)}):*"]
            lines.extend(self._format_build_concern(concern, report.group) for concern in build_concerns)
            sections.append("\n".join(lines))
        if report.its_failures:
            sections.append(self._format_counter_section("ITS Verification Failures", report.its_failures))
        if report.release_failures:
            sections.append(self._format_counter_section("Release to Authz Failures", report.release_failures))
        if report.rebase_failures:
            sections.append(self._format_counter_section("Rebase Failures", report.rebase_failures))
        if len(sections) == 1:
            sections.append(":white_check_mark: Healthy")
        return "\n\n".join(sections)

    def _get_failure_parts(self, report: LayeredProductHealthReport) -> list[str]:
        """
        Return summary labels for all failures in a group report.

        Args:
            report: Group health report to summarize.

        Returns:
            Failure categories with their counts.
        """
        failure_parts = []
        build_concerns = self._get_failure_concerns(report.build_concerns)
        if build_concerns:
            failure_parts.append(f"{len(build_concerns)} build failure(s)")
        if report.its_failures:
            failure_parts.append(f"{len(report.its_failures)} ITS failure(s)")
        if report.release_failures:
            failure_parts.append(f"{len(report.release_failures)} release failure(s)")
        if report.rebase_failures:
            failure_parts.append(f"{len(report.rebase_failures)} rebase failure(s)")
        return failure_parts

    async def _notify_slack(self, reports: list[LayeredProductHealthReport]) -> None:
        """
        Send one summary message per variant and thread failure details under it.

        Args:
            reports: Group health reports sorted for display.
        """
        self.slack_client.bind_channel(SlackClient.DEFAULT_CHANNEL_LAYERED_OPERATORS)

        reports_by_variant: dict[str, list[LayeredProductHealthReport]] = {}
        variant_names: dict[str, str] = {}
        for report in reports:
            variant_name = report.variant.value if report.variant else "unknown"
            variant_key = variant_name if report.variant else f"unknown:{report.group}"
            variant_names[variant_key] = variant_name
            reports_by_variant.setdefault(variant_key, []).append(report)

        for variant_key, variant_reports in reports_by_variant.items():
            response = await self.slack_client.say(
                self._build_variant_message(variant_names[variant_key], variant_reports),
                link_build_url=False,
                unfurl_links=False,
                unfurl_media=False,
            )
            for report in variant_reports:
                if not self._get_failure_parts(report):
                    continue
                await self.slack_client.say(
                    self._build_group_message(report),
                    thread_ts=response["ts"],
                    unfurl_links=False,
                    unfurl_media=False,
                )

    @staticmethod
    def _get_failure_concerns(concerns: list[dict]) -> list[dict]:
        """
        Exclude successful-build entries from a health report.

        Args:
            concerns: Raw Doozer health concerns.

        Returns:
            Concerns representing a failed or never-built image.
        """
        return [concern for concern in concerns if concern.get("code") != ConcernCode.LATEST_BUILD_SUCCEEDED.value]

    def _format_build_concern(self, concern: dict, group: str) -> str:
        """
        Format one build concern with history and optional log links.

        Args:
            concern: Doozer health concern.
            group: Layered-product group associated with the concern.

        Returns:
            Slack-formatted build concern line.
        """
        image_name = concern["image_name"]
        code = concern.get("code")
        if code == ConcernCode.NEVER_BUILT.value:
            return f"- `{image_name}`: No builds attempted during last {DELTA_DAYS} days"

        line = f"- `{image_name}`: {self._url_text(self._build_history_url(group, image_name), 'Build history')}"
        logs_url = self._build_logs_url(concern)
        if logs_url:
            line += f" | {self._url_text(logs_url, 'Latest failure logs')}"
        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            line += f" - Failing for at least {LIMIT_BUILD_RESULTS} attempts"
        else:
            line += f" - Latest attempt failed ({concern.get('latest_success_idx', '?')} attempts since last success)"
        return line

    @staticmethod
    def _format_counter_section(title: str, failures: dict[str, dict]) -> str:
        """
        Format a Redis failure-counter section.

        Args:
            title: Section title.
            failures: Image failure data keyed by image name.

        Returns:
            Slack-formatted counter section.
        """
        lines = [f"*{title} ({len(failures)}):*"]
        for image_name, failure in sorted(failures.items()):
            count = failure.get("failure_count", 0)
            suffix = "" if count == 1 else "s"
            line = f"- `{image_name}`: Failed {count} time{suffix}"
            url = failure.get("pipeline_url") or failure.get("jenkins_url")
            if url:
                line += f" ({LayeredProductsImageHealthPipeline._url_text(url, 'Last failure')})"
            lines.append(line)
        return "\n".join(lines)

    def _build_history_url(self, group: str, image_name: str) -> str:
        """
        Build an ART build-history URL for an image and group.

        Args:
            group: Layered-product group name.
            image_name: Image name.

        Returns:
            ART build-history search URL.
        """
        start_date = (datetime.now(timezone.utc) - timedelta(days=DELTA_DAYS)).strftime("%Y-%m-%d")
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return (
            f"{ART_BUILD_HISTORY_URL}/?name=^{image_name}$&group={group}&assembly={self.assembly}"
            f"&engine=konflux&dateRange={start_date}+to+{end_date}&outcome=Success&outcome=Failure"
        )

    @staticmethod
    def _build_logs_url(concern: dict) -> str:
        """
        Build a logs URL when the concern contains complete failure metadata.

        Args:
            concern: Doozer health concern.

        Returns:
            ART logs URL or an empty string when metadata is incomplete.
        """
        nvr = concern.get("latest_failed_nvr")
        record_id = concern.get("latest_failed_build_record_id")
        failed_time = concern.get("latest_failed_build_time")
        if not nvr or not record_id or not failed_time:
            return ""
        timestamp = datetime.fromisoformat(str(failed_time)).astimezone(timezone.utc)
        formatted = timestamp.strftime("%a, %d %b %Y %H:%M:%S GMT")
        return f"{ART_BUILD_HISTORY_URL}/logs?nvr={nvr}&record_id={record_id}&after={quote(formatted)}"

    @staticmethod
    def _url_text(url: str, text: str) -> str:
        """
        Format a URL as Slack link text.

        Args:
            url: URL to link.
            text: Display text.

        Returns:
            Slack link markup.
        """
        return f"<{quote(url, safe=':/?&=+%.-')}|{text}>"


@cli.command("layered-products-image-health")
@click.option("--groups", required=True, help="Comma-separated layered-product groups to scan")
@click.option("--assembly", required=False, default="stream", help="Assembly to scan for")
@click.option(
    "--data-path",
    required=False,
    default=OCP_BUILD_DATA_URL,
    help="ocp-build-data fork to use (e.g. assembly definition in your own fork)",
)
@click.option("--data-gitref", required=False, default="", help="Doozer data path git [branch / tag / sha] to use")
@click.option("--image-list", required=False, default="", help="Comma-separated list of images to scan")
@pass_runtime
@click_coroutine
async def layered_products_image_health(
    runtime: Runtime,
    groups: str,
    assembly: str,
    data_path: str,
    data_gitref: str,
    image_list: str,
) -> None:
    """
    Collect and report health for comma-separated layered-product groups.
    """
    await LayeredProductsImageHealthPipeline(
        runtime=runtime,
        groups=groups,
        data_path=data_path,
        data_gitref=data_gitref,
        assembly=assembly,
        image_list=image_list,
    ).run()
