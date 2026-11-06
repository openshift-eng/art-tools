from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

import click
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.pipelines.rhcos_sync_fast_channel import (
    DEFAULT_GO_ARCHES,
    RhcosSyncFastChannelPipeline,
    _ArchSyncOutcome,
    rhcos_sync_fast_channel,
)


def _triggered(arch: str, tag: str, result: str = "SUCCESS", url: str = "https://jenkins/rhcos_sync/1"):
    """A real (non-dry-run) arch outcome whose downstream build already completed."""
    return _ArchSyncOutcome(arch=arch, tag=tag, triggered=True, result=result, url=url)


def _skipped(arch: str):
    """An arch outcome where no sync was needed (no nodes / mirror already current)."""
    return _ArchSyncOutcome(arch=arch, tag=None, triggered=False, result=None, url=None)


def _would_trigger(arch: str, tag: str):
    """A dry-run arch outcome: a tag that would be synced, but nothing triggered."""
    return _ArchSyncOutcome(arch=arch, tag=tag, triggered=False, result=None, url=None)


def _make_runtime(dry_run: bool = False) -> MagicMock:
    runtime = MagicMock()
    runtime.dry_run = dry_run
    runtime.logger = MagicMock()
    return runtime


def _graph(*versions: str) -> dict:
    """Build a minimal Cincinnati graph dict with one release node per given version."""
    return {
        "nodes": [{"version": v, "payload": f"quay.io/openshift-release-dev/ocp-release@sha256:{v}"} for v in versions]
    }


class TestRhcosSyncFastChannelPipeline(IsolatedAsyncioTestCase):
    def test_default_go_arches_excludes_multi(self):
        self.assertNotIn("multi", DEFAULT_GO_ARCHES)
        self.assertIn("amd64", DEFAULT_GO_ARCHES)
        self.assertIn("arm64", DEFAULT_GO_ARCHES)

    def test_arch_option_uses_choice_allowlist(self):
        """The --arch option is a click.Choice over the canonical GO-arch allow-list (excluding "multi")."""
        # Invalid arches are rejected at the CLI boundary via click.Choice over the
        # canonical supported GO-arch allow-list.
        arch_opt = next(p for p in rhcos_sync_fast_channel.params if p.name == "arches")
        self.assertIsInstance(arch_opt.type, click.Choice)
        self.assertEqual(list(arch_opt.type.choices), list(DEFAULT_GO_ARCHES))
        self.assertNotIn("multi", arch_opt.type.choices)

    def test_data_path_option_defaults_to_ocp_build_data_url(self):
        """The --data-path option defaults to the canonical ocp-build-data URL."""
        data_path_opt = next(p for p in rhcos_sync_fast_channel.params if p.name == "data_path")
        self.assertEqual(data_path_opt.default, OCP_BUILD_DATA_URL)

    async def test_resolve_arches_uses_explicit_override(self):
        # An explicit --arch override is used verbatim; group.yml is NOT consulted.
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(), version="4.19", arches=["amd64", "s390x"])
        with patch("pyartcd.pipelines.rhcos_sync_fast_channel.util.branch_arches", new=AsyncMock()) as mock_ba:
            arches = await pipeline._resolve_arches()
        self.assertEqual(arches, ["amd64", "s390x"])
        mock_ba.assert_not_awaited()

    async def test_resolve_arches_from_group_yml_413_is_x86_64_only(self):
        # group.yml for openshift-4.13 lists only the BREW arch x86_64; it must resolve to
        # the single GO arch amd64 (and never scan other arches).
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(), version="4.13")
        with patch(
            "pyartcd.pipelines.rhcos_sync_fast_channel.util.branch_arches",
            new=AsyncMock(return_value=["x86_64"]),
        ) as mock_ba:
            arches = await pipeline._resolve_arches()
        self.assertEqual(arches, ["amd64"])
        mock_ba.assert_awaited_once_with(
            group="openshift-4.13", assembly="stream", data_path=OCP_BUILD_DATA_URL, doozer_data_gitref=""
        )

    async def test_resolve_arches_from_group_yml_translates_and_drops_multi(self):
        # BREW arches from group.yml are translated to GO arches; "multi" is dropped.
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(), version="4.19")
        with patch(
            "pyartcd.pipelines.rhcos_sync_fast_channel.util.branch_arches",
            new=AsyncMock(return_value=["x86_64", "aarch64", "s390x", "ppc64le", "multi"]),
        ):
            arches = await pipeline._resolve_arches()
        self.assertEqual(arches, ["amd64", "arm64", "s390x", "ppc64le"])

    async def test_run_413_scans_only_amd64_path(self):
        # End-to-end: with no --arch override, 4.13 resolves to amd64 only and only that
        # arch is handled.
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=False), version="4.13")
        handle = AsyncMock(side_effect=[_triggered("amd64", "4.13.50-x86_64")])
        with (
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins"),
            patch(
                "pyartcd.pipelines.rhcos_sync_fast_channel.util.branch_arches",
                new=AsyncMock(return_value=["x86_64"]),
            ),
            patch.object(pipeline, "_handle_arch", new=handle),
        ):
            await pipeline.run()
        handle.assert_awaited_once_with("amd64")

    async def test_run_summary_dry_run_says_would_trigger(self):
        """Dry-run logs the "[DRY-RUN] Would trigger ..." summary and never the real "triggered" summary."""
        runtime = _make_runtime(dry_run=True)
        pipeline = RhcosSyncFastChannelPipeline(runtime, version="4.19", arches=["amd64"])
        with (
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins"),
            patch.object(
                pipeline, "_handle_arch", new=AsyncMock(return_value=_would_trigger("amd64", "4.19.54-x86_64"))
            ),
        ):
            await pipeline.run()
        summaries = [c.args[0] for c in runtime.logger.info.call_args_list if c.args]
        self.assertTrue(any("[DRY-RUN] Would trigger RHCOS fast-channel sync for:" in s for s in summaries))
        self.assertFalse(any("RHCOS fast-channel sync triggered for:" in s for s in summaries))

    async def test_run_summary_real_run_says_triggered(self):
        """A real run logs the "RHCOS fast-channel sync triggered for:" summary and no [DRY-RUN] line."""
        runtime = _make_runtime(dry_run=False)
        pipeline = RhcosSyncFastChannelPipeline(runtime, version="4.19", arches=["amd64"])
        with (
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins"),
            patch.object(pipeline, "_handle_arch", new=AsyncMock(return_value=_triggered("amd64", "4.19.54-x86_64"))),
        ):
            await pipeline.run()
        summaries = [c.args[0] for c in runtime.logger.info.call_args_list if c.args]
        self.assertTrue(any("RHCOS fast-channel sync triggered for:" in s for s in summaries))
        self.assertFalse(any("[DRY-RUN]" in s for s in summaries))

    def test_version_normalization_and_channel(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(), version="4.19")
        self.assertEqual(pipeline.version, "4.19")
        self.assertEqual(pipeline.major, 4)
        self.assertEqual(pipeline.minor, 19)
        self.assertEqual(pipeline.channel, "fast-4.19")
        # No explicit --arch override: arches are derived from group.yml at run() time.
        self.assertIsNone(pipeline.arches)

    async def test_latest_fast_channel_version_picks_highest_semver(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(), version="4.19")
        graph = _graph("4.19.5", "4.19.54", "4.19.9", "4.18.99")
        with patch("pyartcd.pipelines.rhcos_sync_fast_channel.CincinnatiAPI") as MockAPI:
            MockAPI.return_value.get_graph = AsyncMock(return_value=graph)
            latest = await pipeline._latest_fast_channel_version("amd64")
        # highest within 4.19 y-stream, ignoring the 4.18 node
        self.assertEqual(latest, "4.19.54")
        MockAPI.return_value.get_graph.assert_awaited_once_with(channel="fast-4.19", arch="amd64")

    async def test_latest_fast_channel_version_no_nodes(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(), version="4.19")
        with patch("pyartcd.pipelines.rhcos_sync_fast_channel.CincinnatiAPI") as MockAPI:
            MockAPI.return_value.get_graph = AsyncMock(return_value={"nodes": []})
            latest = await pipeline._latest_fast_channel_version("s390x")
        self.assertIsNone(latest)

    async def test_handle_arch_needs_happening_triggers_and_waits(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=False), version="4.19")
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=True)
            # Downstream build returns (result, url) because block_until_complete+return_build_url.
            mock_jenkins.start_rhcos_sync.return_value = ("SUCCESS", "https://jenkins/rhcos_sync/42")
            mock_jenkins.get_propagatable_params.return_value = {}
            outcome = await pipeline._handle_arch("amd64")

        # The outcome carries the tag and the completed downstream build's result + URL.
        self.assertEqual(outcome.tag, "4.19.54-x86_64")
        self.assertTrue(outcome.triggered)
        self.assertEqual(outcome.result, "SUCCESS")
        self.assertEqual(outcome.url, "https://jenkins/rhcos_sync/42")
        # x86_64 brew-arch suffix used in the tag passed to RhcosSyncPipeline
        _, kwargs = MockRSP.call_args
        self.assertEqual(kwargs["release_tag"], "4.19.54-x86_64")
        # Jenkins is initialized lazily, only when a real trigger is about to happen.
        mock_jenkins.init_jenkins.assert_called_once()
        # The downstream job is triggered AND awaited: block_until_complete=True and the
        # build URL is requested back.
        mock_jenkins.start_rhcos_sync.assert_called_once_with(
            "4.19.54-x86_64",
            dry_run=False,
            sign_only=False,
            force=False,
            propagate_params={},
            block_until_complete=True,
            return_build_url=True,
        )

    async def test_handle_arch_brew_arch_suffix_for_arm(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=False), version="4.19")
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=True)
            mock_jenkins.start_rhcos_sync.return_value = ("SUCCESS", "https://jenkins/rhcos_sync/43")
            mock_jenkins.get_propagatable_params.return_value = {}
            outcome = await pipeline._handle_arch("arm64")

        # go arch arm64 -> brew arch aarch64 in the tag
        self.assertEqual(outcome.tag, "4.19.54-aarch64")
        mock_jenkins.start_rhcos_sync.assert_called_once_with(
            "4.19.54-aarch64",
            dry_run=False,
            sign_only=False,
            force=False,
            propagate_params={},
            block_until_complete=True,
            return_build_url=True,
        )

    async def test_handle_arch_matches_is_noop(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=False), version="4.19")
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=False)
            outcome = await pipeline._handle_arch("amd64")

        self.assertIsNone(outcome.tag)
        self.assertFalse(outcome.triggered)
        mock_jenkins.start_rhcos_sync.assert_not_called()

    async def test_handle_arch_no_nodes_skips(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=False), version="4.19")
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value=None)),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            outcome = await pipeline._handle_arch("ppc64le")

        self.assertIsNone(outcome.tag)
        self.assertFalse(outcome.triggered)
        MockRSP.assert_not_called()
        mock_jenkins.start_rhcos_sync.assert_not_called()

    async def test_handle_arch_dry_run_does_not_trigger(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=True), version="4.19")
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=True)
            outcome = await pipeline._handle_arch("amd64")

        # dry-run reports the tag it WOULD trigger, but never calls jenkins (and never
        # initializes it — init must not require Jenkins creds for a dry-run scan).
        self.assertEqual(outcome.tag, "4.19.54-x86_64")
        self.assertFalse(outcome.triggered)
        self.assertIsNone(outcome.result)
        mock_jenkins.init_jenkins.assert_not_called()
        mock_jenkins.start_rhcos_sync.assert_not_called()

    async def test_force_is_passed_through(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=True), version="4.19", force=True)
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins"),
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=True)
            await pipeline._handle_arch("amd64")

        _, kwargs = MockRSP.call_args
        self.assertTrue(kwargs["force"])

    async def test_force_forwarded_to_downstream_job(self):
        # force must be forwarded to the downstream RHCOS_SYNC job's FORCE parameter so the
        # --force semantics hold end-to-end (the downstream job runs its own needsHappening).
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=False), version="4.19", force=True)
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=True)
            mock_jenkins.start_rhcos_sync.return_value = ("SUCCESS", "https://jenkins/rhcos_sync/44")
            mock_jenkins.get_propagatable_params.return_value = {}
            await pipeline._handle_arch("amd64")

        mock_jenkins.start_rhcos_sync.assert_called_once_with(
            "4.19.54-x86_64",
            dry_run=False,
            sign_only=False,
            force=True,
            propagate_params={},
            block_until_complete=True,
            return_build_url=True,
        )

    async def test_handle_arch_propagates_orchestrator_params(self):
        # ART_TOOLS_COMMIT / PLR_TEMPLATE_COMMIT from get_propagatable_params() must be
        # forwarded to the downstream rhcos_sync build so it runs the same revision.
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=False), version="4.19")
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=True)
            mock_jenkins.start_rhcos_sync.return_value = ("SUCCESS", "https://jenkins/rhcos_sync/45")
            mock_jenkins.get_propagatable_params.return_value = {"ART_TOOLS_COMMIT": "user@branch"}
            await pipeline._handle_arch("amd64")

        mock_jenkins.get_propagatable_params.assert_called_once_with()
        _, kwargs = mock_jenkins.start_rhcos_sync.call_args
        self.assertEqual(kwargs["propagate_params"], {"ART_TOOLS_COMMIT": "user@branch"})

    async def test_run_iterates_all_arches(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=False), version="4.19", arches=["amd64", "s390x"])
        handle = AsyncMock(side_effect=[_triggered("amd64", "4.19.54-x86_64"), _skipped("s390x")])
        with (
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins"),
            patch.object(pipeline, "_handle_arch", new=handle),
        ):
            await pipeline.run()

        # Each requested arch is processed (gathered concurrently).
        handle.assert_has_awaits([call("amd64"), call("s390x")], any_order=True)
        self.assertEqual(handle.await_count, 2)

    async def test_run_one_arch_failure_does_not_abort_others_but_fails_overall(self):
        pipeline = RhcosSyncFastChannelPipeline(_make_runtime(dry_run=False), version="4.19", arches=["amd64", "s390x"])
        handle = AsyncMock(side_effect=[RuntimeError("boom"), _triggered("s390x", "4.19.54-s390x")])
        with (
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins"),
            patch.object(pipeline, "_handle_arch", new=handle),
        ):
            # Remaining arches are still processed, but the overall command fails.
            with self.assertRaises(RuntimeError):
                await pipeline.run()

        # The arch that failed AND the subsequent arch were both awaited (gather waits for all).
        handle.assert_has_awaits([call("amd64"), call("s390x")], any_order=True)
        self.assertEqual(handle.await_count, 2)

    async def test_run_waits_for_all_arches_and_triggers_each_with_block_until_complete(self):
        """Real run: each resolved arch triggers a blocking start_rhcos_sync (force forwarded) and all are awaited."""
        # End-to-end (real run): every resolved arch triggers start_rhcos_sync with
        # block_until_complete=True and the pipeline awaits ALL of them before returning.
        runtime = _make_runtime(dry_run=False)
        pipeline = RhcosSyncFastChannelPipeline(runtime, version="4.19", arches=["amd64", "s390x"], force=True)
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=True)
            mock_jenkins.start_rhcos_sync.return_value = ("SUCCESS", "https://jenkins/rhcos_sync/1")
            await pipeline.run()

        # One downstream trigger per arch, each blocking until complete, with force forwarded.
        self.assertEqual(mock_jenkins.start_rhcos_sync.call_count, 2)
        for c in mock_jenkins.start_rhcos_sync.call_args_list:
            self.assertEqual(c.kwargs["block_until_complete"], True)
            self.assertEqual(c.kwargs["return_build_url"], True)
            self.assertEqual(c.kwargs["force"], True)
        triggered_tags = {c.args[0] for c in mock_jenkins.start_rhcos_sync.call_args_list}
        self.assertEqual(triggered_tags, {"4.19.54-x86_64", "4.19.54-s390x"})

    async def test_run_aggregate_fails_when_downstream_build_not_success(self):
        # If any downstream build result is not SUCCESS, the overall command fails; the
        # aggregate error carries the (safe) Jenkins result string, not raw exception text.
        runtime = _make_runtime(dry_run=False)
        pipeline = RhcosSyncFastChannelPipeline(runtime, version="4.19", arches=["amd64", "s390x"])
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=True)
            # amd64 succeeds, s390x fails downstream.
            mock_jenkins.start_rhcos_sync.side_effect = [
                ("SUCCESS", "https://jenkins/rhcos_sync/1"),
                ("FAILURE", "https://jenkins/rhcos_sync/2"),
            ]
            with self.assertRaises(RuntimeError) as ctx:
                await pipeline.run()

        # Both arches were triggered and awaited (no early abort on the failure).
        self.assertEqual(mock_jenkins.start_rhcos_sync.call_count, 2)
        self.assertIn("s390x", str(ctx.exception))
        self.assertIn("FAILURE", str(ctx.exception))

    async def test_run_all_success_does_not_raise(self):
        runtime = _make_runtime(dry_run=False)
        pipeline = RhcosSyncFastChannelPipeline(runtime, version="4.19", arches=["amd64", "s390x"])
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=True)
            mock_jenkins.start_rhcos_sync.return_value = ("SUCCESS", "https://jenkins/rhcos_sync/1")
            # Should complete without raising when every downstream build succeeds.
            await pipeline.run()

        self.assertEqual(mock_jenkins.start_rhcos_sync.call_count, 2)

    async def test_run_dry_run_triggers_nothing(self):
        """Dry-run triggers/initializes nothing in Jenkins yet still logs the would-trigger summary."""
        # Dry-run must trigger/await NOTHING, yet still log the would-trigger summary.
        runtime = _make_runtime(dry_run=True)
        pipeline = RhcosSyncFastChannelPipeline(runtime, version="4.19", arches=["amd64", "s390x"])
        with (
            patch.object(pipeline, "_latest_fast_channel_version", new=AsyncMock(return_value="4.19.54")),
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.RhcosSyncPipeline") as MockRSP,
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins") as mock_jenkins,
        ):
            MockRSP.return_value.check_needs_happening = AsyncMock(return_value=True)
            await pipeline.run()

        mock_jenkins.start_rhcos_sync.assert_not_called()
        mock_jenkins.init_jenkins.assert_not_called()
        summaries = [c.args[0] for c in runtime.logger.info.call_args_list if c.args]
        self.assertTrue(any("[DRY-RUN] Would trigger RHCOS fast-channel sync for:" in s for s in summaries))

    async def test_run_per_arch_failure_omits_raw_exception_from_logs_and_aggregate_error(self):
        """A per-arch failure's raw exception text leaks into neither the logs nor the aggregate error."""
        # SECURITY: a private-hostname/credential marker embedded in the raw exception must
        # appear in NEITHER the logs NOR the aggregate RuntimeError message (oc/kubeconfig
        # stderr can carry internal hostnames or credentials).
        sensitive = "internal-host.secret.example.com/token=SUPERSECRET"
        runtime = _make_runtime(dry_run=False)
        pipeline = RhcosSyncFastChannelPipeline(runtime, version="4.19", arches=["amd64"])
        handle = AsyncMock(side_effect=[RuntimeError(f"oc failed talking to {sensitive}")])
        with (
            patch("pyartcd.pipelines.rhcos_sync_fast_channel.jenkins"),
            patch.object(pipeline, "_handle_arch", new=handle),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                await pipeline.run()

        # (a) The raised aggregate error must not leak the raw exception string.
        self.assertNotIn(sensitive, str(ctx.exception))

        # (b) No log call (any level) may contain the sensitive marker. Reconstruct every
        # logged string (format string % args) across all log levels.
        logged = []
        for log_method in (
            runtime.logger.error,
            runtime.logger.warning,
            runtime.logger.info,
        ):
            for log_call in log_method.call_args_list:
                args = log_call.args
                if not args:
                    continue
                try:
                    logged.append(args[0] % args[1:] if len(args) > 1 else args[0])
                except (TypeError, ValueError):
                    logged.append(" ".join(str(a) for a in args))
        combined = "\n".join(logged)
        self.assertNotIn(sensitive, combined)
        # But the arch and exception type should still be present for triage (logs + error).
        self.assertIn("amd64", combined)
        self.assertIn("RuntimeError", combined)
        self.assertIn("amd64", str(ctx.exception))
        self.assertIn("RuntimeError", str(ctx.exception))
