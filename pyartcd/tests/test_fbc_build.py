import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.fbc_build import build_fbc_catalogs, generate_fbc_lock_name


class TestFbcBuild(unittest.TestCase):
    def test_generate_fbc_lock_name(self):
        self.assertEqual(generate_fbc_lock_name("openshift-4.20", "4.20"), "lock:fbc-build:openshift-4.20")
        self.assertEqual(generate_fbc_lock_name("mtc-1.8", "4.20"), "lock:fbc-build:mtc-1.8-ocp-4.20")
        with self.assertRaisesRegex(ValueError, "--major-minor is required"):
            generate_fbc_lock_name("mtc-1.8")
        self.assertEqual(generate_fbc_lock_name("mtc-1.8", allow_missing_target=True), "lock:fbc-build:mtc-1.8")

    @patch("pyartcd.fbc_build.jenkins.get_build_path_or_random", return_value="job/build/1")
    @patch("pyartcd.fbc_build.locks.run_with_lock", new_callable=AsyncMock)
    @patch("pyartcd.fbc_build.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch.dict("os.environ", {"KONFLUX_OPERATOR_INDEX_AUTH_FILE": "/tmp/registry-auth.json"})
    def test_builds_all_targets_with_refresh_options(self, mock_cmd, mock_lock, _mock_lock_id):
        async def await_build(coro, **_kwargs):
            return await coro

        async def command_result(cmd, **_kwargs):
            target = next(arg.split("=", 1)[1] for arg in cmd if arg.startswith("--major-minor="))
            return (
                0,
                json.dumps(
                    {
                        "nvrs": [f"operator-fbc-1.8.18-20261011230515.ocp{target}"],
                        "pullspecs": [f"quay.io/example/fbc@sha256:{target.replace('.', '')}"],
                        "errors": [],
                        "failed_count": 0,
                        "success_count": 1,
                    }
                ),
                "",
            )

        mock_lock.side_effect = await_build
        mock_cmd.side_effect = command_result
        runtime = MagicMock(dry_run=False)
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime.working_dir = Path(temp_dir)
            results = asyncio.run(
                build_fbc_catalogs(
                    runtime=runtime,
                    group="mtc-1.8",
                    assembly="1.8.18",
                    version="1.8.18",
                    product="rhmtc",
                    operator_nvrs_by_target={
                        "4.19": ["operator-container-1.8.18-1"],
                        "4.20": ["operator-container-1.8.18-1"],
                    },
                    force=True,
                    reset_to_prod=True,
                )
            )

        self.assertEqual([result.target_version for result in results], ["4.19", "4.20"])
        self.assertEqual(mock_cmd.await_count, 2)
        self.assertTrue(
            all(call.kwargs == {"check": False, "stderr": None} for call in mock_cmd.await_args_list),
            "Doozer stderr should stream to the parent process while stdout remains captured for JSON parsing",
        )
        commands = [call.args[0] for call in mock_cmd.await_args_list]
        self.assertTrue(all("--force" in command for command in commands))
        self.assertTrue(all("--reset-to-prod" in command for command in commands))
        self.assertTrue(all("--variant=rhmtc" in command for command in commands))
        self.assertTrue(all("--prod-registry-auth=/tmp/registry-auth.json" in command for command in commands))
        self.assertNotEqual(
            next(arg for arg in commands[0] if arg.startswith("--working-dir=")),
            next(arg for arg in commands[1] if arg.startswith("--working-dir=")),
        )
        self.assertEqual(
            [call.kwargs["lock_name"] for call in mock_lock.await_args_list],
            ["lock:fbc-build:mtc-1.8-ocp-4.19", "lock:fbc-build:mtc-1.8-ocp-4.20"],
        )

    @patch("pyartcd.fbc_build.jenkins.get_build_path_or_random", return_value="job/build/1")
    @patch("pyartcd.fbc_build.locks.run_with_lock", new_callable=AsyncMock)
    @patch("pyartcd.fbc_build.exectools.cmd_gather_async", new_callable=AsyncMock)
    def test_rejects_partial_result(self, mock_cmd, mock_lock, _mock_lock_id):
        async def await_build(coro, **_kwargs):
            return await coro

        mock_lock.side_effect = await_build
        mock_cmd.return_value = (
            0,
            json.dumps({"nvrs": [], "pullspecs": [], "errors": [], "failed_count": 0, "success_count": 0}),
            "",
        )
        runtime = MagicMock(dry_run=False)
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime.working_dir = Path(temp_dir)
            with self.assertRaisesRegex(RuntimeError, "returned 0 NVRs and 0 pullspecs for 1 operators"):
                asyncio.run(
                    build_fbc_catalogs(
                        runtime=runtime,
                        group="mtc-1.8",
                        assembly="1.8.18",
                        version="1.8.18",
                        product="rhmtc",
                        operator_nvrs_by_target={"4.20": ["operator-container-1.8.18-1"]},
                        force=True,
                        reset_to_prod=True,
                    )
                )

    @patch("pyartcd.fbc_build.jenkins.get_build_path_or_random", return_value="job/build/1")
    @patch("pyartcd.fbc_build.locks.run_with_lock", new_callable=AsyncMock)
    @patch("pyartcd.fbc_build.exectools.cmd_gather_async", new_callable=AsyncMock)
    def test_allows_partial_results_for_normal_prepare_release(self, mock_cmd, mock_lock, _mock_lock_id):
        """Fail-open callers should retain successful catalogs and receive failure details."""

        async def await_build(coro, **_kwargs):
            return await coro

        mock_lock.side_effect = await_build
        mock_cmd.return_value = (
            1,
            json.dumps(
                {
                    "nvrs": ["operator-a-fbc-1.8.18-1.ocp4.20"],
                    "pullspecs": ["quay.io/example/fbc@sha256:operator-a"],
                    "errors": [{"operator": "operator-b", "error": "build failed"}],
                    "failed_count": 1,
                    "success_count": 1,
                }
            ),
            "",
        )
        failures = []
        runtime = MagicMock(dry_run=False)
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime.working_dir = Path(temp_dir)
            results = asyncio.run(
                build_fbc_catalogs(
                    runtime=runtime,
                    group="mtc-1.8",
                    assembly="1.8.18",
                    version="1.8.18",
                    product="rhmtc",
                    operator_nvrs_by_target={
                        "4.20": ["operator-a-container-1.8.18-1", "operator-b-container-1.8.18-1"]
                    },
                    reset_to_prod=True,
                    allow_partial=True,
                    failures_out=failures,
                )
            )

        self.assertEqual(results[0].nvrs, ("operator-a-fbc-1.8.18-1.ocp4.20",))
        self.assertEqual(len(failures), 2)
        self.assertTrue(any("operator-b" in failure for failure in failures))
        self.assertTrue(any("returned 1 NVRs and 1 pullspecs for 2 operators" in failure for failure in failures))

    @patch("pyartcd.fbc_build.jenkins.get_build_path_or_random", return_value="job/build/1")
    @patch("pyartcd.fbc_build.locks.run_with_lock", new_callable=AsyncMock)
    @patch("pyartcd.fbc_build.exectools.cmd_gather_async", new_callable=AsyncMock)
    def test_waits_for_all_targets_before_raising(self, mock_cmd, mock_lock, _mock_lock_id):
        """A failed target should not stop the coordinator from awaiting its peers."""

        async def await_build(coro, **_kwargs):
            return await coro

        async def command_result(cmd, **_kwargs):
            target = next(arg.split("=", 1)[1] for arg in cmd if arg.startswith("--major-minor="))
            if target == "4.19":
                return 1, json.dumps({"nvrs": [], "pullspecs": [], "errors": ["failed"], "failed_count": 1}), ""
            return (
                0,
                json.dumps(
                    {
                        "nvrs": ["operator-fbc-1.8.18-1.ocp4.20"],
                        "pullspecs": ["quay.io/example/fbc@sha256:420"],
                        "errors": [],
                        "failed_count": 0,
                    }
                ),
                "",
            )

        mock_lock.side_effect = await_build
        mock_cmd.side_effect = command_result
        runtime = MagicMock(dry_run=False)
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime.working_dir = Path(temp_dir)
            with self.assertRaisesRegex(RuntimeError, "FBC build for OCP 4.19 failed"):
                asyncio.run(
                    build_fbc_catalogs(
                        runtime=runtime,
                        group="mtc-1.8",
                        assembly="1.8.18",
                        version="1.8.18",
                        product="rhmtc",
                        operator_nvrs_by_target={
                            "4.19": ["operator-container-1.8.18-1"],
                            "4.20": ["operator-container-1.8.18-1"],
                        },
                        force=True,
                        reset_to_prod=True,
                    )
                )

        self.assertEqual(mock_cmd.await_count, 2)

    @patch("pyartcd.fbc_build.exectools.cmd_gather_async", new_callable=AsyncMock)
    def test_dry_run_does_not_start_builds(self, mock_cmd):
        runtime = MagicMock(dry_run=True)
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime.working_dir = Path(temp_dir)
            results = asyncio.run(
                build_fbc_catalogs(
                    runtime=runtime,
                    group="mtc-1.8",
                    assembly="1.8.18",
                    version="1.8.18",
                    product="rhmtc",
                    operator_nvrs_by_target={"4.20": ["operator-container-1.8.18-1"]},
                    force=True,
                    reset_to_prod=True,
                )
            )

        self.assertEqual(results[0].pullspecs, ())
        mock_cmd.assert_not_awaited()
