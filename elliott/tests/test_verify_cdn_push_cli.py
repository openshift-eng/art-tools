import json
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

from elliottlib.cli.verify_cdn_push_cli import (
    AdvisoryPushResult,
    PushJobInfo,
    VerifyCdnPushResult,
    check_advisory_push,
    parse_push_jobs,
    render_result,
    verify_cdn_push,
)


class TestPushJobInfo(TestCase):
    def test_complete(self):
        j = PushJobInfo(target="cdn_stage", job_id=100, status="COMPLETE")
        self.assertTrue(j.complete)
        self.assertFalse(j.failed)

    def test_failed(self):
        j = PushJobInfo(target="cdn_stage", job_id=100, status="FAILED")
        self.assertFalse(j.complete)
        self.assertTrue(j.failed)

    def test_running(self):
        j = PushJobInfo(target="cdn_stage", job_id=100, status="RUNNING")
        self.assertFalse(j.complete)
        self.assertFalse(j.failed)


class TestAdvisoryPushResult(TestCase):
    def test_complete(self):
        r = AdvisoryPushResult(
            advisory_id=12345,
            impetus="rpm",
            push_jobs=[
                PushJobInfo(target="cdn_stage", job_id=1, status="COMPLETE"),
                PushJobInfo(target="cdn_docker_stage", job_id=2, status="COMPLETE"),
            ],
        )
        self.assertTrue(r.complete)
        self.assertFalse(r.failed)
        self.assertFalse(r.pending)

    def test_failed(self):
        r = AdvisoryPushResult(
            advisory_id=12345,
            impetus="rpm",
            push_jobs=[
                PushJobInfo(target="cdn_stage", job_id=1, status="COMPLETE"),
                PushJobInfo(target="cdn_docker_stage", job_id=2, status="FAILED"),
            ],
        )
        self.assertFalse(r.complete)
        self.assertTrue(r.failed)

    def test_pending(self):
        r = AdvisoryPushResult(
            advisory_id=12345,
            impetus="rpm",
            push_jobs=[
                PushJobInfo(target="cdn_stage", job_id=1, status="RUNNING"),
            ],
        )
        self.assertFalse(r.complete)
        self.assertFalse(r.failed)
        self.assertTrue(r.pending)

    def test_error(self):
        r = AdvisoryPushResult(advisory_id=12345, impetus="rpm", error="something broke")
        self.assertFalse(r.complete)
        self.assertTrue(r.failed)

    def test_empty_jobs(self):
        r = AdvisoryPushResult(advisory_id=12345, impetus="rpm")
        self.assertFalse(r.complete)
        self.assertFalse(r.failed)
        self.assertTrue(r.pending)


class TestVerifyCdnPushResult(TestCase):
    def test_all_complete(self):
        r = VerifyCdnPushResult(
            advisories=[
                AdvisoryPushResult(
                    advisory_id=1,
                    impetus="rpm",
                    push_jobs=[PushJobInfo(target="cdn_stage", job_id=1, status="COMPLETE")],
                ),
                AdvisoryPushResult(
                    advisory_id=2,
                    impetus="rhcos",
                    push_jobs=[PushJobInfo(target="cdn_stage", job_id=2, status="COMPLETE")],
                ),
            ]
        )
        self.assertTrue(r.complete)
        self.assertFalse(r.failed)

    def test_one_failed(self):
        r = VerifyCdnPushResult(
            advisories=[
                AdvisoryPushResult(
                    advisory_id=1,
                    impetus="rpm",
                    push_jobs=[PushJobInfo(target="cdn_stage", job_id=1, status="COMPLETE")],
                ),
                AdvisoryPushResult(
                    advisory_id=2,
                    impetus="rhcos",
                    push_jobs=[PushJobInfo(target="cdn_stage", job_id=2, status="FAILED")],
                ),
            ]
        )
        self.assertFalse(r.complete)
        self.assertTrue(r.failed)


class TestParsePushJobs(TestCase):
    def test_picks_latest_per_target(self):
        raw = [
            {"id": 1, "status": "FAILED", "target": {"name": "cdn_stage"}},
            {"id": 5, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
            {"id": 3, "status": "RUNNING", "target": {"name": "cdn_stage"}},
            {"id": 2, "status": "COMPLETE", "target": {"name": "cdn_docker_stage"}},
        ]
        jobs = parse_push_jobs(raw)
        by_target = {j.target: j for j in jobs}
        self.assertEqual(by_target["cdn_stage"].job_id, 5)
        self.assertEqual(by_target["cdn_stage"].status, "COMPLETE")
        self.assertEqual(by_target["cdn_docker_stage"].job_id, 2)

    def test_empty_list(self):
        self.assertEqual(parse_push_jobs([]), [])

    def test_single_job(self):
        raw = [{"id": 10, "status": "RUNNING", "target": {"name": "cdn_stage"}}]
        jobs = parse_push_jobs(raw)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].status, "RUNNING")


class TestCheckAdvisoryPush(IsolatedAsyncioTestCase):
    async def test_all_complete(self):
        api = AsyncMock()
        api.get_push_jobs.return_value = [
            {"id": 1, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
            {"id": 2, "status": "COMPLETE", "target": {"name": "cdn_docker_stage"}},
        ]
        result = await check_advisory_push(api, 12345, "rpm", do_push=True)
        self.assertTrue(result.complete)
        self.assertFalse(result.push_triggered)
        api.push_cdn_stage.assert_not_called()

    async def test_failed_with_push(self):
        api = AsyncMock()
        api.get_push_jobs.side_effect = [
            [{"id": 1, "status": "FAILED", "target": {"name": "cdn_stage"}}],
            [{"id": 2, "status": "RUNNING", "target": {"name": "cdn_stage"}}],
        ]
        result = await check_advisory_push(api, 12345, "rpm", do_push=True)
        self.assertTrue(result.push_triggered)
        api.push_cdn_stage.assert_called_once_with(12345)

    async def test_failed_without_push(self):
        api = AsyncMock()
        api.get_push_jobs.return_value = [
            {"id": 1, "status": "FAILED", "target": {"name": "cdn_stage"}},
        ]
        result = await check_advisory_push(api, 12345, "rpm", do_push=False)
        self.assertFalse(result.push_triggered)
        api.push_cdn_stage.assert_not_called()

    async def test_no_jobs_with_push(self):
        api = AsyncMock()
        api.get_push_jobs.side_effect = [
            [],
            [{"id": 1, "status": "RUNNING", "target": {"name": "cdn_stage"}}],
        ]
        result = await check_advisory_push(api, 12345, "rpm", do_push=True)
        self.assertTrue(result.push_triggered)

    async def test_running_no_push(self):
        api = AsyncMock()
        api.get_push_jobs.return_value = [
            {"id": 1, "status": "RUNNING", "target": {"name": "cdn_stage"}},
        ]
        result = await check_advisory_push(api, 12345, "rpm", do_push=True)
        self.assertFalse(result.push_triggered)
        self.assertTrue(result.pending)

    async def test_api_error(self):
        api = AsyncMock()
        api.get_push_jobs.side_effect = RuntimeError("connection failed")
        result = await check_advisory_push(api, 12345, "rpm", do_push=True)
        self.assertTrue(result.failed)
        self.assertIn("connection failed", result.error)

    async def test_push_rejected_dependencies(self):
        api = AsyncMock()
        api.get_push_jobs.return_value = []
        api.push_cdn_stage.return_value = None
        result = await check_advisory_push(api, 12345, "rpm", do_push=True)
        self.assertFalse(result.push_triggered)
        self.assertTrue(result.failed)
        self.assertIn("unmet dependencies", result.error)


class TestVerifyCdnPush(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_cdn_push_cli.AsyncErrataAPI")
    async def test_all_complete(self, mock_api_cls):
        api = AsyncMock()
        mock_api_cls.return_value.__aenter__ = AsyncMock(return_value=api)
        mock_api_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        api.get_advisory.return_value = {"errata": {"rhba": {"blocking_advisories": []}}}
        api.get_push_jobs.return_value = [
            {"id": 1, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
        ]

        result = await verify_cdn_push({"rpm": 111}, do_push=True)
        self.assertTrue(result.complete)

    @patch("elliottlib.cli.verify_cdn_push_cli.AsyncErrataAPI")
    async def test_with_blocking_advisory(self, mock_api_cls):
        api = AsyncMock()
        mock_api_cls.return_value.__aenter__ = AsyncMock(return_value=api)
        mock_api_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        api.get_advisory.return_value = {"errata": {"rhba": {"blocking_advisories": [999]}}}
        api.get_push_jobs.return_value = [
            {"id": 1, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
        ]

        result = await verify_cdn_push({"rpm": 111}, do_push=True)
        self.assertTrue(result.complete)
        self.assertEqual(len(result.advisories), 2)

    @patch("elliottlib.cli.verify_cdn_push_cli.AsyncErrataAPI")
    async def test_blocking_incomplete_skips_main(self, mock_api_cls):
        api = AsyncMock()
        mock_api_cls.return_value.__aenter__ = AsyncMock(return_value=api)
        mock_api_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        api.get_advisory.return_value = {"errata": {"rhba": {"blocking_advisories": [999]}}}
        api.get_push_jobs.return_value = [
            {"id": 1, "status": "RUNNING", "target": {"name": "cdn_stage"}},
        ]

        result = await verify_cdn_push({"rpm": 111}, do_push=True)
        self.assertFalse(result.complete)

    @patch("elliottlib.cli.verify_cdn_push_cli.AsyncErrataAPI")
    async def test_blocking_lookup_failure(self, mock_api_cls):
        api = AsyncMock()
        mock_api_cls.return_value.__aenter__ = AsyncMock(return_value=api)
        mock_api_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        api.get_advisory.side_effect = RuntimeError("connection failed")

        result = await verify_cdn_push({"rpm": 111}, do_push=True)
        self.assertFalse(result.complete)
        self.assertTrue(result.failed)


class TestRenderResult(TestCase):
    def test_text_complete(self):
        r = VerifyCdnPushResult(
            advisories=[
                AdvisoryPushResult(
                    advisory_id=12345,
                    impetus="rpm",
                    push_jobs=[PushJobInfo(target="cdn_stage", job_id=1, status="COMPLETE")],
                ),
            ]
        )
        text = render_result(r, "text")
        self.assertIn("COMPLETE", text)
        self.assertIn("12345", text)

    def test_text_with_push_triggered(self):
        r = VerifyCdnPushResult(
            advisories=[
                AdvisoryPushResult(
                    advisory_id=12345,
                    impetus="rpm",
                    push_triggered=True,
                    push_jobs=[PushJobInfo(target="cdn_stage", job_id=2, status="RUNNING")],
                ),
            ]
        )
        text = render_result(r, "text")
        self.assertIn("re-triggered", text.lower())

    def test_json_output(self):
        r = VerifyCdnPushResult(
            advisories=[
                AdvisoryPushResult(
                    advisory_id=12345,
                    impetus="rpm",
                    push_jobs=[PushJobInfo(target="cdn_stage", job_id=1, status="COMPLETE")],
                ),
            ]
        )
        data = json.loads(render_result(r, "json"))
        self.assertTrue(data["complete"])
        self.assertEqual(len(data["advisories"]), 1)
        self.assertEqual(data["advisories"][0]["advisory_id"], 12345)

    def test_text_fail(self):
        r = VerifyCdnPushResult(
            advisories=[
                AdvisoryPushResult(advisory_id=12345, impetus="rpm", error="boom"),
            ]
        )
        text = render_result(r, "text")
        self.assertIn("FAIL", text)
        self.assertIn("boom", text)
