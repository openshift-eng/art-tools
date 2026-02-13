import os
import re

import click
import yaml
from artcommonlib import exectools, redis
from artcommonlib.redis import RedisError
from artcommonlib.release_util import SoftwareLifecyclePhase
from artcommonlib.telemetry import start_as_current_span_async
from artcommonlib.util import split_git_url
from ghapi.all import GhApi
from opentelemetry import trace

from pyartcd import constants, jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.jenkins import get_build_url
from pyartcd.oc import registry_login
from pyartcd.runtime import GroupRuntime, Runtime
from pyartcd.util import branch_arches

TRACER = trace.get_tracer(__name__)
GEN_PAYLOAD_ARTIFACTS_OUT_DIR = "gen-payload-artifacts"


class BuildSyncMultiPipeline:
    @classmethod
    async def create(cls, *args, **kwargs):
        self = cls(*args, **kwargs)
        self.group_runtime = await GroupRuntime.create(
            self.runtime.config,
            self.working_dir,
            self.group,
            self.assembly,
            self.data_path,
            self.doozer_data_gitref,
        )
        return self

    def __init__(
        self,
        runtime: Runtime,
        version: str,
        assembly: str,
        data_path: str,
        emergency_ignore_issues: bool,
        doozer_data_gitref: str,
        multi_model: str,
        exclude_arches: str,
        embargo_permit_ack: bool,
    ):
        self.runtime = runtime
        self.version = version
        self.group = f"openshift-{version}"
        self.assembly = assembly
        self.data_path = data_path
        self.emergency_ignore_issues = emergency_ignore_issues
        self.doozer_data_gitref = doozer_data_gitref
        self.multi_model = multi_model
        self.exclude_arches = [] if not exclude_arches else exclude_arches.replace(",", " ").split()
        self.supported_arches = []
        self.embargo_permit_ack = embargo_permit_ack
        self.logger = runtime.logger
        self.working_dir = self.runtime.working_dir
        self.fail_count_name = f"count:build-sync-multi-failure:{assembly}:{version}"
        self.job_run = get_build_url()

        self.slack_client = self.runtime.new_slack_client()
        self.slack_client.bind_channel(f"openshift-{self.version}")

        # Multi-model payloads always use the multi imagestream naming convention
        self.is_base_name = f"{self.version}-art-latest"

    @start_as_current_span_async(TRACER, "build-sync-multi.comment-on-assembly-pr")
    async def comment_on_assembly_pr(self, text_body):
        """
        Comment the link to this jenkins build on the assembly PR if it was triggered automatically
        """
        current_span = trace.get_current_span()
        current_span.set_attribute("build-sync-multi.assembly", self.assembly)

        # Keeping in try-except so that job doesn't fail because of any error here
        try:
            _, _, repository = split_git_url(self.data_path)
            branch = self.doozer_data_gitref
            token = os.environ.get("GITHUB_TOKEN")

            pattern = r"github\.com/([^/]+)/"
            match = re.search(pattern, self.data_path)

            repo_owner = None
            if match:
                repo_owner = match.group(1)

            api = GhApi(owner=constants.GITHUB_OWNER, repo=repository, token=token)

            # Check if the doozer_data_gitref is given then, if not
            # then it set the branch to openshift-{major}.{minor}
            if not branch:
                branch = f"openshift-{self.version}"

            # Head needs to have the repo name prepended for GhApi to fetch the correct one
            head = f"{repo_owner}:{branch}"
            # Find our assembly PR.
            prs = api.pulls.list(head=head, state="open")

            if len(prs) == 0:
                self.logger.warning(f"No assembly PRs were found with head={head}")
                return

            if len(prs) > 1:
                self.logger.warning(f"{len(prs)} PR(s) were found with head={head}. We need only 1.")
                return

            pr_number = prs[0]["number"]

            if self.runtime.dry_run:
                self.logger.warning(
                    f"[DRY RUN] Would have commented on PR {constants.GITHUB_OWNER}/{repository}/pull/{pr_number} "
                    f"with the message: \n {text_body}"
                )
                return

            # https://docs.github.com/en/rest/issues/comments?apiVersion=2022-11-28#create-an-issue-comment
            # PR is an issue as far as  GitHub API is concerned
            api.issues.create_comment(issue_number=pr_number, body=text_body)

        except Exception as e:
            self.logger.warning(f"Failed commenting to PR: {e}")

    @start_as_current_span_async(TRACER, "build-sync-multi.run")
    async def run(self):
        current_span = trace.get_current_span()
        current_span.set_attribute("build-sync-multi.version", self.version)
        current_span.set_attribute("build-sync-multi.assembly", self.assembly)
        current_span.set_attribute("build-sync-multi.multi_model", self.multi_model)

        # For multi-model, we determine arches from the Konflux configuration
        self.supported_arches = await branch_arches(
            group=f"openshift-{self.version}",
            assembly=self.assembly,
            build_system="konflux",  # Multi-model always uses Konflux
            data_path=self.data_path,
            doozer_data_gitref=self.doozer_data_gitref,
        )
        current_span.set_attribute("build-sync-multi.supported_arches_count", len(self.supported_arches))
        self.logger.info("Supported arches for this build: %s", ", ".join(self.supported_arches))

        jenkins.update_title(" [MULTI-MODEL]")

        if self.assembly not in ("stream", "test") and not self.runtime.dry_run:
            # Comment on PR if triggered from gen assembly
            text_body = f"Multi-model build sync job [run]({self.job_run}) has been triggered"
            await self.comment_on_assembly_pr(text_body)

        # Make sure we're logged into the OC registry
        await registry_login()

        # Generate multi-model nightly imagestream
        self.logger.info("Generate multi-model nightly imagestream...")
        await self._generate_multi_model_imagestream()

    @start_as_current_span_async(TRACER, "build-sync-multi.handle-success")
    async def handle_success(self):
        current_span = trace.get_current_span()
        current_span.set_attribute("build-sync-multi.assembly", self.assembly)

        if self.assembly != "stream":
            # Comment on the PR that the job succeeded
            await self.comment_on_assembly_pr(f"Multi-model build sync job [run]({self.job_run}) succeeded!")
            await self.slack_client.say(
                f"@release-artists Multi-model <{self.job_run}|build-sync> for assembly `{self.assembly}` succeeded!"
            )

        #  All good: delete fail counter
        if self.assembly == "stream" and not self.runtime.dry_run:
            current_count = await redis.get_value(self.fail_count_name)
            if current_count and int(current_count) > 1:
                await self.slack_client.say(f"<{self.job_run}|Multi-model build-sync> succeeded!")

            res = await redis.delete_key(self.fail_count_name)
            if res:
                self.runtime.logger.debug('Fail count "%s" deleted', self.fail_count_name)

    @start_as_current_span_async(TRACER, "build-sync-multi.generate-multi-model-imagestream")
    async def _generate_multi_model_imagestream(self):
        """
        Generate multi-model payload using doozer release:gen-payload with --multi-model
        """
        current_span = trace.get_current_span()
        current_span.set_attribute("build-sync-multi.version", self.version)
        current_span.set_attribute("build-sync-multi.assembly", self.assembly)
        current_span.set_attribute("build-sync-multi.multi_model", self.multi_model)

        jenkins.init_jenkins()

        self.logger.info("Generating multi-model payload")
        mirror_working = "MIRROR_working"

        # Run release:gen-payload with --multi-model
        cmd = ["doozer", f"--assembly={self.assembly}"]
        cmd.extend(
            [
                f"--working-dir={mirror_working}",
                f"--data-path={self.data_path}",
            ]
        )
        group_param = f"--group=openshift-{self.version}"
        if self.doozer_data_gitref:
            group_param += f"@{self.doozer_data_gitref}"
        cmd.append(group_param)
        cmd.extend(
            [
                "release:gen-payload",
                f"--output-dir={GEN_PAYLOAD_ARTIFACTS_OUT_DIR}",
                f"--multi-model={self.multi_model}",
                "--apply",
                "--apply-multi-arch",
            ]
        )
        if self.emergency_ignore_issues:
            cmd.append("--emergency-ignore-issues")
        if self.embargo_permit_ack:
            cmd.append("--embargo-permit-ack")
        if self.exclude_arches:
            cmd.extend([f"--exclude-arch={arch}" for arch in self.exclude_arches])
        if self.runtime.dry_run:
            cmd.extend(["--skip-gc-tagging", "--moist-run"])
        await exectools.cmd_assert_async(cmd, env=os.environ.copy())

    def get_unpermissable_assembly_issues(self) -> dict[str, dict[any, any]]:
        """
        Filters assembly issues with 'permitted: false' from assembly_report.yml.
        """

        # path to local assembly_report.yml
        file_path = f"{GEN_PAYLOAD_ARTIFACTS_OUT_DIR}/assembly-report.yaml"

        # Open and load the YAML file
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)

        filtered_issues = {"assembly_issues": {}}

        if "assembly_issues" in data:
            for component, issues in data["assembly_issues"].items():
                filtered_data = [issue for issue in issues if not issue.get("permitted", False)]
                if filtered_data:  # Add component only if there are valid issues
                    filtered_issues["assembly_issues"][component] = filtered_data

            return filtered_issues

    @start_as_current_span_async(TRACER, "build-sync-multi.handle-failure")
    async def handle_failure(self):
        current_span = trace.get_current_span()
        current_span.set_attribute("build-sync-multi.assembly", self.assembly)
        current_span.set_attribute("build-sync-multi.version", self.version)

        if self.assembly != "stream":
            await self.comment_on_assembly_pr(f"Multi-model build sync job [run]({self.job_run}) failed!")
            await self.slack_client.say(
                f"@release-artists Multi-model <{self.job_run}|build-sync> for assembly {self.assembly} failed!"
            )

        # Increment failure count
        current_count = await redis.get_value(self.fail_count_name)
        if current_count is None:  # does not yet exist in Redis
            current_count = 0
        fail_count = int(current_count) + 1
        self.runtime.logger.info("Failure count for multi-model %s: %s", self.version, fail_count)

        # Update fail counter on Redis
        await redis.set_value(self.fail_count_name, fail_count)

        unpermissable_issues = self.get_unpermissable_assembly_issues()
        if unpermissable_issues["assembly_issues"]:
            report = yaml.safe_dump(unpermissable_issues)
            jenkins.update_title(" [UNVIABLE]")
        else:
            report = "Unknown Failure. Please investigate"
            jenkins.update_title(" [FAILURE]")

        # Less than 2 failures, assembly != stream: just break the build
        if fail_count < 2 or self.assembly != "stream":
            raise

        # More than 2 failures: we need to notify ART and #forum-release before breaking the build
        if 10 <= fail_count <= 50:
            art_notify_frequency = 5
            forum_release_notify_frequency = 10

        elif 50 <= fail_count <= 200:
            art_notify_frequency = 10
            forum_release_notify_frequency = 50

        elif fail_count > 200:
            art_notify_frequency = 100
            forum_release_notify_frequency = 100

        else:
            # Default notify frequency
            art_notify_frequency = 2
            forum_release_notify_frequency = 5

        # Spam ourselves a little more often than forum-ocp-release
        if fail_count % art_notify_frequency == 0:
            await self.notify_failures(f"openshift-{self.version}", report, fail_count)

        if fail_count % forum_release_notify_frequency == 0:
            # For GA releases, let forum-ocp-release know why no new builds
            phase = SoftwareLifecyclePhase.from_name(self.group_runtime.group_config["software_lifecycle"]["phase"])
            if phase == SoftwareLifecyclePhase.RELEASE:
                await self.notify_failures("#forum-ocp-release", report, fail_count)

    @start_as_current_span_async(TRACER, "build-sync-multi.notify-failures")
    async def notify_failures(self, channel, assembly_report, fail_count):
        current_span = trace.get_current_span()
        current_span.set_attribute("build-sync-multi.channel", channel)
        current_span.set_attribute("build-sync-multi.fail_count", fail_count)
        current_span.set_attribute("build-sync-multi.version", self.version)
        current_span.set_attribute("build-sync-multi.assembly", self.assembly)

        msg = (
            f"Pipeline has failed to assemble multi-model release payload for {self.version} "
            f"(assembly `{self.assembly}`) {fail_count} times."
        )
        self.slack_client.bind_channel(channel)
        slack_response = await self.slack_client.say(msg)
        slack_thread = slack_response["message"]["ts"]
        await self.slack_client.say(f"```{assembly_report}```", slack_thread)


@cli.command("build-sync-multi")
@click.option("--version", required=True, help="The OCP version to sync")
@click.option("--assembly", required=True, default="stream", help="The name of an assembly to sync")
@click.option(
    "--data-path",
    required=True,
    default=constants.OCP_BUILD_DATA_URL,
    help="ocp-build-data fork to use (e.g. assembly definition in your own fork)",
)
@click.option(
    "--emergency-ignore-issues",
    is_flag=True,
    help="Ignore all issues with constructing payload. Only supported for assemblies of type: stream. "
    "Do not use without approval.",
)
@click.option("--data-gitref", required=False, help="(Optional) Doozer data path git [branch / tag / sha] to use")
@click.option(
    "--multi-model",
    required=False,
    default="amd64:0",
    help="Multi-model specification (e.g. 'amd64:0' for latest amd64 nightly)",
)
@click.option(
    "--exclude-arches",
    required=False,
    help="(Optional) Comma-separated list of arches NOT to sync (aarch64, ppc64le, s390x, x86_64)",
)
@click.option(
    "--embargo-permit-ack",
    is_flag=True,
    default=False,
    help="To permit embargoed builds to be promoted after embargo lift",
)
@pass_runtime
@click_coroutine
@start_as_current_span_async(TRACER, "build-sync-multi")
async def build_sync_multi(
    runtime: Runtime,
    version: str,
    assembly: str,
    data_path: str,
    emergency_ignore_issues: bool,
    data_gitref: str,
    multi_model: str,
    exclude_arches: str,
    embargo_permit_ack: bool,
):
    jenkins.init_jenkins()
    pipeline = await BuildSyncMultiPipeline.create(
        runtime=runtime,
        version=version,
        assembly=assembly,
        data_path=data_path,
        emergency_ignore_issues=emergency_ignore_issues,
        doozer_data_gitref=data_gitref,
        multi_model=multi_model,
        exclude_arches=exclude_arches,
        embargo_permit_ack=embargo_permit_ack,
    )

    # Multi-model uses a separate lock from regular build-sync
    lock = locks.Lock.BUILD_SYNC_MULTI
    lock_name = locks.Lock.BUILD_SYNC_MULTI.value.format(version=version)

    span = trace.get_current_span()
    span.set_attributes(
        {
            "pyartcd.param.dry_run": runtime.dry_run,
            "pyartcd.param.version": version,
            "pyartcd.param.assembly": assembly,
            "pyartcd.param.multi_model": multi_model,
        }
    )
    try:
        # Only for stream assembly, lock the build to avoid parallel runs
        if assembly == "stream":
            lock_identifier = get_build_url().replace(f"{constants.JENKINS_UI_URL}/", "")
            runtime.logger.info("Lock identifier: %s", lock_identifier)

            await locks.run_with_lock(
                coro=pipeline.run(),
                lock=lock,
                lock_name=lock_name,
                lock_id=lock_identifier,
            )
        else:
            await pipeline.run()

        await pipeline.handle_success()
        span.set_status(trace.StatusCode.OK)

    except (RuntimeError, ChildProcessError):
        # Only for 'stream' assembly, track failure to enable future notifications
        if assembly == "stream" and not runtime.dry_run:
            await pipeline.handle_failure()

        # Re-raise the exception to mark the job as failed
        raise

    except RedisError as e:
        runtime.logger.error("Encountered error when updating the fail counter: %s", e)
        raise
