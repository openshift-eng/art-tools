import unittest
from unittest import mock

import pytest
from artcommonlib.constants import KONFLUX_IMAGE_TEST_REPO
from pyartcd.pipelines.sync_nvr_to_quay import SyncNvrToQuayPipeline, sync_image_to_quay
from pyartcd.runtime import Runtime


def _make_runtime(dry_run=False):
    runtime = mock.MagicMock(spec=Runtime)
    runtime.logger = mock.MagicMock()
    runtime.dry_run = dry_run
    return runtime


def _make_record(name="ose-foo", version="4.18.0", image_tag="foo-tag", embargoed=False):
    record = mock.MagicMock()
    record.name = name
    record.version = version
    record.image_tag = image_tag
    record.image_pullspec = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:deadbeef"
    record.embargoed = embargoed
    return record


class TestSyncImageToQuayHelper(unittest.IsolatedAsyncioTestCase):
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    async def test_dry_run_does_not_sync(self, mock_sync):
        await sync_image_to_quay("pullspec", KONFLUX_IMAGE_TEST_REPO, ["a", "b"], dry_run=True)
        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    async def test_filters_empty_tags(self, mock_sync):
        await sync_image_to_quay("pullspec", KONFLUX_IMAGE_TEST_REPO, ["", "b", None], dry_run=False)
        mock_sync.assert_awaited_once_with("pullspec", KONFLUX_IMAGE_TEST_REPO, ["b"])


class TestSyncNvrToQuayPipeline(unittest.IsolatedAsyncioTestCase):
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

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_happy_path_sync(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(name="ose-foo", version="4.18.0", image_tag="foo-tag")
        db = mock_db_cls.return_value
        db.get_build_record_by_nvr = mock.AsyncMock(return_value=record)

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        await pipeline.run()

        db.bind.assert_called_once()
        db.get_build_record_by_nvr.assert_awaited_once_with("ose-foo-4.18.0-1")
        mock_sync.assert_awaited_once_with(
            record.image_pullspec,
            KONFLUX_IMAGE_TEST_REPO,
            ["foo-tag", "ose-foo-4.18"],
        )

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_empty_image_tag_omitted(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(name="ose-foo", version="4.18.0", image_tag="")
        db = mock_db_cls.return_value
        db.get_build_record_by_nvr = mock.AsyncMock(return_value=record)

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        await pipeline.run()

        mock_sync.assert_awaited_once_with(
            record.image_pullspec,
            KONFLUX_IMAGE_TEST_REPO,
            ["ose-foo-4.18"],
        )

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=True)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_embargoed_nvr_skipped(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(embargoed=False)
        db = mock_db_cls.return_value
        db.get_build_record_by_nvr = mock.AsyncMock(return_value=record)

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1.p3"])
        await pipeline.run()

        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_record_embargoed_flag_skipped(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        record = _make_record(embargoed=True)
        db = mock_db_cls.return_value
        db.get_build_record_by_nvr = mock.AsyncMock(return_value=record)

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        await pipeline.run()

        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_dry_run_does_not_sync(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=True)
        record = _make_record()
        db = mock_db_cls.return_value
        db.get_build_record_by_nvr = mock.AsyncMock(return_value=record)

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1"])
        await pipeline.run()

        db.get_build_record_by_nvr.assert_awaited_once()
        mock_sync.assert_not_awaited()

    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.is_nvr_embargoed", return_value=False)
    @mock.patch("pyartcd.pipelines.sync_nvr_to_quay.KonfluxDb")
    async def test_multiple_nvrs_synced(self, mock_db_cls, mock_embargo, mock_sync):
        runtime = _make_runtime(dry_run=False)
        db = mock_db_cls.return_value
        db.get_build_record_by_nvr = mock.AsyncMock(
            side_effect=[
                _make_record(name="ose-foo", image_tag="foo-tag"),
                _make_record(name="ose-bar", image_tag="bar-tag"),
            ]
        )

        pipeline = SyncNvrToQuayPipeline(runtime, nvrs=["ose-foo-4.18.0-1", "ose-bar-4.18.0-1"])
        await pipeline.run()

        assert mock_sync.await_count == 2
