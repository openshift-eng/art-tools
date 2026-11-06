import os
import unittest
from unittest import mock

import pytest
from artcommonlib.constants import KONFLUX_DEFAULT_IMAGE_REPO, KONFLUX_IMAGE_TEST_REPO, RHCOS_IMAGE_REPO
from pyartcd.pipelines.sync_nvr_to_quay import SyncNvrToQuayPipeline, sync_image_to_quay
from pyartcd.runtime import Runtime


def _make_runtime(dry_run=False):
    runtime = mock.MagicMock(spec=Runtime)
    runtime.logger = mock.MagicMock()
    runtime.dry_run = dry_run
    return runtime


def _make_record(
    name="ose-foo",
    version="4.18.0",
    image_tag="foo-tag",
    embargoed=False,
    record_id="rec-1",
    outcome="success",
    image_pullspec="quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:deadbeef",
    start_time="2026-01-01T00:00:00+00:00",
):
    record = mock.MagicMock()
    record.name = name
    record.version = version
    record.image_tag = image_tag
    record.image_pullspec = image_pullspec
    record.embargoed = embargoed
    record.record_id = record_id
    record.outcome = outcome
    record.start_time = start_time
    return record


def _aiter(items):
    """Return a fresh async iterator over items (mimics search_builds_by_fields)."""

    async def _gen():
        for item in items:
            yield item

    return _gen()


def _db_with_results(mock_db_cls, *result_batches):
    """Wire the mocked KonfluxDb so each search_builds_by_fields call yields the next batch.

    Also records the ``where`` kwargs of each call on the returned MagicMock.
    """
    db = mock_db_cls.return_value
    batches = iter(result_batches)
    search = mock.MagicMock(side_effect=lambda **kwargs: _aiter(next(batches)))
    db.search_builds_by_fields = search
    return db, search


class TestSyncImageToQuayHelper(unittest.IsolatedAsyncioTestCase):
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    async def test_dry_run_does_not_sync(self, mock_sync):
        await sync_image_to_quay("pullspec", KONFLUX_IMAGE_TEST_REPO, ["a", "b"], dry_run=True)
        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    async def test_filters_empty_tags(self, mock_sync):
        await sync_image_to_quay("pullspec", KONFLUX_IMAGE_TEST_REPO, ["", "b", None], dry_run=False)
        mock_sync.assert_awaited_once_with("pullspec", KONFLUX_IMAGE_TEST_REPO, ["b"])


class TestSyncNvrToQuayPipelineValidation(unittest.IsolatedAsyncioTestCase):
    def test_allowlist_rejects_unapproved_repo(self):
        runtime = _make_runtime()
        with pytest.raises(ValueError, match="not allowed"):
            SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"], target_repo="quay.io/evil/repo")

    def test_empty_nvr_rejected(self):
        runtime = _make_runtime()
        with pytest.raises(ValueError, match="At least one NVR"):
            SyncNvrToQuayPipeline(runtime, nvrs=[])

    def test_default_target_repo(self):
        runtime = _make_runtime()
        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        assert pipeline.target_repo == KONFLUX_IMAGE_TEST_REPO
        assert pipeline.allow_failed is False
        assert pipeline.record_id == ""

    def test_record_id_with_multiple_nvrs_rejected(self):
        runtime = _make_runtime()
        with pytest.raises(ValueError, match="only meaningful for a single NVR"):
            SyncNvrToQuayPipeline(
                runtime,
                nvrs=["ose-foo-4.18.0-1", "ose-bar-4.18.0-1"],
                record_id="rec-1",
            )

    def test_realistic_nvr_passes_validation(self):
        runtime = _make_runtime()
        nvr = "rhcos-node-image-nvidia-container-v5.0.0-202610261547.p2.gb4b1579.assembly.stream.el10"
        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=[nvr])
        assert pipeline.nvrs == [nvr]

    def test_nvr_with_illegal_space_rejected(self):
        runtime = _make_runtime()
        with pytest.raises(ValueError, match="Invalid NVR"):
            SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1 extra"])

    def test_nvr_with_illegal_semicolon_rejected(self):
        runtime = _make_runtime()
        with pytest.raises(ValueError, match="Invalid NVR"):
            SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1;rm -rf"])

    def test_invalid_record_id_rejected(self):
        runtime = _make_runtime()
        with pytest.raises(ValueError, match="Invalid record_id"):
            SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"], record_id="abc/def")

    def test_uuid_record_id_passes_validation(self):
        runtime = _make_runtime()
        record_id = "123e4567-e89b-12d3-a456-426614174000"
        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"], record_id=record_id)
        assert pipeline.record_id == record_id

    def test_nvr_with_trailing_newline_rejected(self):
        """A value of valid chars plus a trailing newline must be rejected (re.fullmatch, not $)."""
        runtime = _make_runtime()
        with pytest.raises(ValueError, match="Invalid NVR"):
            SyncNvrToQuayPipeline(runtime, nvrs=["valid-nvr\n"])

    def test_record_id_with_trailing_newline_rejected(self):
        """A record_id of valid chars plus a trailing newline must be rejected."""
        runtime = _make_runtime()
        with pytest.raises(ValueError, match="Invalid record_id"):
            SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"], record_id="0123abcd\n")


class TestSyncNvrToQuayPipeline(unittest.IsolatedAsyncioTestCase):
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_happy_path_sync(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(name="ose-foo", version="4.18.0", image_tag="foo-tag")
        db, search = _db_with_results(mock_db_cls, [record])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        await pipeline.run()

        db.bind.assert_called_once()
        # allow_failed=False => outcome filter is success-only
        assert search.call_args.kwargs["where"] == {"nvr": "ose-foo-4.18.0-1", "outcome": "success"}
        mock_sync.assert_awaited_once_with(
            record.image_pullspec,
            KONFLUX_IMAGE_TEST_REPO,
            ["foo-tag"],
        )

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.jenkins.update_description")
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.jenkins.init_jenkins")
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_updates_build_description_on_success(
        self, mock_db_cls, mock_embargo, mock_sync, mock_init, mock_update
    ):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(
            name="ose-foo",
            image_tag="foo-tag",
            image_pullspec="quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:deadbeef",
        )
        _db_with_results(mock_db_cls, [record])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        await pipeline.run()

        mock_init.assert_called_once()
        mock_update.assert_called_once()
        description = mock_update.call_args.args[0]
        # The description lists the created destination pullspecs, grouped by NVR.
        assert "ose-foo-4.18.0-1" in description
        assert f"{KONFLUX_IMAGE_TEST_REPO}@sha256:deadbeef" in description
        assert f"{KONFLUX_IMAGE_TEST_REPO}:sha256-deadbeef" in description
        assert f"{KONFLUX_IMAGE_TEST_REPO}:foo-tag" in description

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.jenkins.update_description")
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.jenkins.init_jenkins")
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_dry_run_does_not_update_description(
        self, mock_db_cls, mock_embargo, mock_sync, mock_init, mock_update
    ):
        runtime = _make_runtime(dry_run=True)
        record = _make_record(name="ose-foo", image_tag="foo-tag")
        _db_with_results(mock_db_cls, [record])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        await pipeline.run()

        mock_update.assert_not_called()
        mock_init.assert_not_called()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_allow_failed_omits_outcome_filter(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(outcome="its_error")
        db, search = _db_with_results(mock_db_cls, [record])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"], allow_failed=True)
        await pipeline.run()

        # allow_failed=True => no outcome key, so default terminal set applies
        assert "outcome" not in search.call_args.kwargs["where"]
        assert search.call_args.kwargs["where"] == {"nvr": "ose-foo-4.18.0-1"}
        mock_sync.assert_awaited_once()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_empty_image_tag_omitted(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(name="ose-foo", version="4.18.0", image_tag="")
        _db_with_results(mock_db_cls, [record])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        await pipeline.run()

        # No image_tag and no floating tag -> only the internal sha256 GC tag remains,
        # so sync_to_quay is called with an empty additional-tags list.
        mock_sync.assert_awaited_once_with(
            record.image_pullspec,
            KONFLUX_IMAGE_TEST_REPO,
            [],
        )

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_no_record_found_raises(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        _db_with_results(mock_db_cls, [])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        with pytest.raises(ValueError, match="No build record found"):
            await pipeline.run()
        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_no_record_found_suggests_allow_failed(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        _db_with_results(mock_db_cls, [])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"], allow_failed=False)
        with pytest.raises(ValueError, match="--allow-failed"):
            await pipeline.run()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_multiple_records_without_record_id_raises(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        rec_a = _make_record(record_id="rec-a", outcome="success")
        rec_b = _make_record(record_id="rec-b", outcome="its_error")
        _db_with_results(mock_db_cls, [rec_a, rec_b])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"], allow_failed=True)
        with pytest.raises(ValueError) as exc_info:
            await pipeline.run()
        msg = str(exc_info.value)
        assert "Multiple build records found" in msg
        assert "rec-a" in msg and "rec-b" in msg
        assert "--record-id" in msg
        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_record_id_disambiguation(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(record_id="rec-b", outcome="its_error", image_tag="foo-tag", name="ose-foo")
        db, search = _db_with_results(mock_db_cls, [record])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"], allow_failed=True, record_id="rec-b")
        await pipeline.run()

        assert search.call_args.kwargs["where"] == {"nvr": "ose-foo-4.18.0-1", "record_id": "rec-b"}
        mock_sync.assert_awaited_once_with(
            record.image_pullspec,
            KONFLUX_IMAGE_TEST_REPO,
            ["foo-tag"],
        )

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_record_id_not_found_raises(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        _db_with_results(mock_db_cls, [])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"], record_id="rec-x")
        with pytest.raises(ValueError, match="record_id"):
            await pipeline.run()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=True)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_embargoed_nvr_skipped_even_when_failed(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(embargoed=False, outcome="its_error")
        _db_with_results(mock_db_cls, [record])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1.p3"], allow_failed=True)
        await pipeline.run()

        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_record_embargoed_flag_skipped(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(embargoed=True)
        _db_with_results(mock_db_cls, [record])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        await pipeline.run()

        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_no_pullspec_guard_raises(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(outcome="build_error", image_pullspec="", record_id="rec-np")
        _db_with_results(mock_db_cls, [record])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"], allow_failed=True)
        with pytest.raises(ValueError, match="has no image_pullspec"):
            await pipeline.run()
        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_dry_run_does_not_sync(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=True)
        record = _make_record()
        _db_with_results(mock_db_cls, [record])

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        await pipeline.run()

        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_multiple_nvrs_synced(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        _db_with_results(
            mock_db_cls,
            [_make_record(name="ose-foo", image_tag="foo-tag")],
            [_make_record(name="ose-bar", image_tag="bar-tag")],
        )

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1", "ose-bar-4.18.0-1"])
        await pipeline.run()

        assert mock_sync.await_count == 2


class TestSyncNvrToQuayMergedAuth(unittest.IsolatedAsyncioTestCase):
    """Tests for the merged registry-auth handling in run()."""

    def _patch_registry_config(self, mock_rc_cls, merged_path="/merged/auth.json"):
        """Wire the mocked RegistryConfig context manager to yield merged_path."""
        ctx = mock_rc_cls.return_value
        ctx.__enter__ = mock.MagicMock(return_value=merged_path)
        ctx.__exit__ = mock.MagicMock(return_value=False)
        return ctx

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.os.path.exists", return_value=True)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.RegistryConfig")
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_rhcos_auth_merges_both_source_files_and_repos(
        self, mock_db_cls, mock_embargo, mock_sync, mock_rc_cls, mock_exists
    ):
        self._patch_registry_config(mock_rc_cls)
        runtime = _make_runtime(dry_run=False)
        _db_with_results(mock_db_cls, [_make_record(name="ose-foo", image_tag="foo-tag")])

        captured = {}

        async def _capture(*args, **kwargs):
            captured["quay_auth_file"] = os.environ.get("QUAY_AUTH_FILE")

        mock_sync.side_effect = _capture

        env = {"QUAY_AUTH_FILE": "/default/auth.json", "RHCOS_QUAY_AUTH_FILE": "/rhcos/auth.json"}
        with mock.patch.dict(os.environ, env, clear=False):
            pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
            await pipeline.run()
            # QUAY_AUTH_FILE restored to the original default after run()
            assert os.environ.get("QUAY_AUTH_FILE") == "/default/auth.json"

        # Both source files passed; registries include RHCOS repo.
        kwargs = mock_rc_cls.call_args.kwargs
        assert kwargs["source_files"] == ["/default/auth.json", "/rhcos/auth.json"]
        assert kwargs["registries"] == [KONFLUX_DEFAULT_IMAGE_REPO, KONFLUX_IMAGE_TEST_REPO, RHCOS_IMAGE_REPO]
        # During the sync, QUAY_AUTH_FILE pointed at the merged file.
        assert captured["quay_auth_file"] == "/merged/auth.json"

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.os.path.exists", return_value=True)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.RegistryConfig")
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_without_rhcos_auth_uses_only_default_source(
        self, mock_db_cls, mock_embargo, mock_sync, mock_rc_cls, mock_exists
    ):
        self._patch_registry_config(mock_rc_cls)
        runtime = _make_runtime(dry_run=False)
        _db_with_results(mock_db_cls, [_make_record(name="ose-foo", image_tag="foo-tag")])

        # Ensure RHCOS_QUAY_AUTH_FILE is absent for this run.
        env = {"QUAY_AUTH_FILE": "/default/auth.json"}
        with mock.patch.dict(os.environ, env, clear=False):
            os.environ.pop("RHCOS_QUAY_AUTH_FILE", None)
            pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
            await pipeline.run()

        kwargs = mock_rc_cls.call_args.kwargs
        assert kwargs["source_files"] == ["/default/auth.json"]
        assert kwargs["registries"] == [KONFLUX_DEFAULT_IMAGE_REPO, KONFLUX_IMAGE_TEST_REPO]
        assert RHCOS_IMAGE_REPO not in kwargs["registries"]
        mock_sync.assert_awaited_once()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.RegistryConfig")
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_no_quay_auth_file_skips_merge(self, mock_db_cls, mock_embargo, mock_sync, mock_rc_cls):
        runtime = _make_runtime(dry_run=False)
        _db_with_results(mock_db_cls, [_make_record(name="ose-foo", image_tag="foo-tag")])

        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("QUAY_AUTH_FILE", None)
            os.environ.pop("RHCOS_QUAY_AUTH_FILE", None)
            pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
            await pipeline.run()

        # Without QUAY_AUTH_FILE, no merged config is built, but the sync still runs.
        mock_rc_cls.assert_not_called()
        mock_sync.assert_awaited_once()
