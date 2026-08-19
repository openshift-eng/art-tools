import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from pyartcd.pipelines.olm_bundle_konflux import _stage_release_related_images


class TestStageReleaseRelatedImages(unittest.IsolatedAsyncioTestCase):
    """A stage release failure must be attributed to individual operators, not the whole job.

    pyartcd drops only the failed operators from the FBC trigger list, so it needs the per-operator
    `stage_release_related_images` records doozer writes to record.log.
    """

    def setUp(self):
        self._working_dir = TemporaryDirectory()
        self.addCleanup(self._working_dir.cleanup)
        self.runtime = mock.MagicMock(dry_run=False, doozer_working=self._working_dir.name)

    def _write_record_log(self, *records):
        lines = []
        for record in records:
            fields = "|".join(f"{k}={v}" for k, v in record.items())
            lines.append(f"stage_release_related_images|{fields}|")
        Path(self._working_dir.name, 'record.log').write_text("\n".join(lines) + "\n")

    async def _run(self):
        return await _stage_release_related_images(
            runtime=self.runtime,
            doozer_base_cmd=['doozer', '--group=rhacm2-2.16'],
            namespace='ns',
            final_kubeconfig='/path/to/kubeconfig',
            operator_nvrs=['operator-a-1-1', 'operator-b-1-1'],
        )

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async")
    async def test_returns_empty_list_on_success(self, cmd_assert_async: mock.AsyncMock):
        self.assertEqual(await self._run(), [])
        cmd = cmd_assert_async.await_args.args[0]
        self.assertEqual(cmd[-2:], ['operator-a-1-1', 'operator-b-1-1'])
        self.assertIn('beta:bundle:stage-release-related-images', cmd)

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async")
    async def test_returns_failed_operator_nvrs(self, cmd_assert_async: mock.AsyncMock):
        cmd_assert_async.side_effect = ChildProcessError('doozer failed')
        self._write_record_log(
            {'status': '1', 'operator_nvr': 'operator-a-1-1', 'operator': 'operator-a', 'message': 'boom'},
            {'status': '0', 'operator_nvr': 'operator-b-1-1', 'operator': 'operator-b', 'message': ''},
        )

        self.assertEqual(await self._run(), ['operator-a-1-1'])

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async")
    async def test_reraises_when_no_per_operator_results(self, cmd_assert_async: mock.AsyncMock):
        """A crash before any operator ran (bad kubeconfig, etc.) is a job-wide failure."""
        cmd_assert_async.side_effect = ChildProcessError('doozer failed')

        with self.assertRaises(ChildProcessError):
            await self._run()

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async")
    async def test_reraises_when_all_records_succeeded(self, cmd_assert_async: mock.AsyncMock):
        cmd_assert_async.side_effect = ChildProcessError('doozer failed')
        self._write_record_log(
            {'status': '0', 'operator_nvr': 'operator-a-1-1', 'operator': 'operator-a', 'message': ''},
        )

        with self.assertRaises(ChildProcessError):
            await self._run()

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async")
    async def test_passes_dry_run_through(self, cmd_assert_async: mock.AsyncMock):
        self.runtime.dry_run = True
        await self._run()
        self.assertIn('--dry-run', cmd_assert_async.await_args.args[0])


if __name__ == '__main__':
    unittest.main()
