#!/usr/bin/env python
"""
Test functions related to controlled command execution
"""

import asyncio
import json
import logging
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import IsolatedAsyncioTestCase, mock

from artcommonlib import exectools


class RetryTestCase(IsolatedAsyncioTestCase):
    """
    Test the exectools.retry() method
    """

    ERROR_MSG = r"Giving up after {} failed attempt\(s\)"

    def test_success(self):
        """
        Given a function that passes, make sure it returns successfully with
        a single retry or greater.
        """

        def pass_function():
            return True

        self.assertTrue(exectools.retry(1, pass_function))
        self.assertTrue(exectools.retry(2, pass_function))

    def test_failure(self):
        """
        Given a function that fails, make sure that it raise an exception
        correctly with a single retry limit and greater.
        """

        def fail_function():
            return False

        assertRaisesRegex = self.assertRaisesRegex if hasattr(self, 'assertRaisesRegex') else self.assertRaisesRegexp
        assertRaisesRegex(Exception, self.ERROR_MSG.format(1), exectools.retry, 1, fail_function)
        assertRaisesRegex(Exception, self.ERROR_MSG.format(2), exectools.retry, 2, fail_function)

    def test_wait(self):
        """
        Verify that the retry fails and raises an exception as needed.
        Further, verify that the indicated wait loops occurred.
        """

        expected_calls = list("fw0fw1f")

        # initialize a collector for loop information
        calls = []

        # loop 3 times, writing into the collector each try and wait
        assertRaisesRegex = self.assertRaisesRegex if hasattr(self, 'assertRaisesRegex') else self.assertRaisesRegexp
        assertRaisesRegex(
            Exception,
            self.ERROR_MSG.format(3),
            exectools.retry,
            3,
            lambda: calls.append("f"),
            wait_f=lambda n: calls.extend(("w", str(n))),
        )

        # check that the test and wait loop operated as expected
        self.assertEqual(calls, expected_calls)

    def test_return(self):
        """
        Verify that the retry task return value is passed back out faithfully.
        """
        obj = {}

        def func():
            return obj

        self.assertIs(exectools.retry(1, func, check_f=lambda _: True), obj)


class TestExectools(IsolatedAsyncioTestCase):
    def test_gather_success(self):
        with mock.patch("subprocess.Popen") as MockPopen:
            mock_popen = MockPopen.return_value
            mock_popen.communicate.return_value = (b"hello there\n", b"")
            mock_popen.returncode = 0

            with self.assertLogs(level=logging.DEBUG) as cm:
                status, stdout, stderr = exectools.cmd_gather(["/usr/bin/echo", "hello", "there"], timeout=3000)
                self.assertEqual(status, 0)
                self.assertEqual(stdout, "hello there\n")
                self.assertEqual(stderr, "")
                MockPopen.assert_called_once_with(
                    ["/usr/bin/echo", "hello", "there"],
                    cwd=None,
                    env=mock.ANY,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                )
                mock_popen.communicate.assert_called_once_with(timeout=3000)
                self.assertTrue(
                    any(line for line in cm.output if "Executing:cmd_gather: /usr/bin/echo hello there" in line)
                )
                self.assertTrue(
                    any(line for line in cm.output if "Exited with: 0\nstdout>>hello there\n<<\nstderr>><<" in line)
                )

    def test_gather_timeout(self):
        with mock.patch("subprocess.Popen") as MockPopen:
            mock_popen = MockPopen.return_value
            mock_popen.communicate.side_effect = subprocess.TimeoutExpired("cmd", 3000)

            with self.assertLogs(level=logging.DEBUG) as cm:
                with self.assertRaises(subprocess.TimeoutExpired):
                    status, stdout, stderr = exectools.cmd_gather(["/usr/bin/sleep", "10"], timeout=3000)
                MockPopen.assert_called_once_with(
                    ["/usr/bin/sleep", "10"],
                    cwd=None,
                    env=mock.ANY,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                )
                mock_popen.communicate.assert_any_call(timeout=3000)
                mock_popen.communicate.assert_any_call()
                self.assertTrue(any(line for line in cm.output if "Executing:cmd_gather: /usr/bin/sleep 10" in line))

    def test_gather_fail(self):
        with mock.patch("subprocess.Popen") as MockPopen:
            mock_popen = MockPopen.return_value
            mock_popen.communicate.return_value = (b"", b"error")
            mock_popen.returncode = 1

            with self.assertLogs(level=logging.DEBUG) as cm:
                status, stdout, stderr = exectools.cmd_gather(["/usr/bin/false"], timeout=3000)
                self.assertEqual(status, 1)
                self.assertEqual(stdout, "")
                self.assertEqual(stderr, "error")
                MockPopen.assert_called_once_with(
                    ["/usr/bin/false"],
                    cwd=None,
                    env=mock.ANY,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                )
                mock_popen.communicate.assert_called_once_with(timeout=3000)
                self.assertTrue(any(line for line in cm.output if "Executing:cmd_gather: /usr/bin/false" in line))
                self.assertTrue(
                    any(line for line in cm.output if "Exited with error: 1\nstdout>><<\nstderr>>error<<\n" in line)
                )

    def test_cmd_assert_success(self):
        with mock.patch("artcommonlib.exectools.cmd_gather") as cmd_gather:
            cmd_gather.return_value = (0, "hello there", "")
            exectools.cmd_assert(["/usr/bin/echo", "hello", "there"])
            cmd_gather.assert_called_once_with(
                ["/usr/bin/echo", "hello", "there"],
                set_env=None,
                realtime=False,
                strip=False,
                log_stdout=False,
                log_stderr=True,
                timeout=None,
                cwd=None,
            )

    @mock.patch("artcommonlib.exectools.cmd_gather")
    @mock.patch("time.sleep")
    def test_cmd_assert_success_with_retries(self, mock_sleep: mock.MagicMock, cmd_gather: mock.MagicMock):
        cmd_gather.side_effect = [(1, "", "error"), (0, "hello there", "")]
        exectools.cmd_assert(["/usr/bin/echo", "hello", "there"], retries=2)
        self.assertEqual(cmd_gather.call_count, 2)
        mock_sleep.assert_called_once_with(60)

    @mock.patch("artcommonlib.exectools.cmd_gather")
    @mock.patch("time.sleep")
    def test_cmd_assert_fail_with_retries(self, mock_sleep: mock.MagicMock, cmd_gather: mock.MagicMock):
        cmd_gather.side_effect = [(1, "", "error"), (1, "", "error")]
        with self.assertRaises(ChildProcessError):
            exectools.cmd_assert(["/usr/bin/false"], retries=2)
        mock_sleep.assert_called_once_with(60)
        self.assertEqual(cmd_gather.call_count, 2)

    async def test_cmd_gather_async(self):
        cmd = ["uname", "-a"]
        fake_cwd = "/foo/bar"
        fake_stdout = b"fake_stdout"
        fake_stderr = b"fake_stderr"
        with mock.patch("asyncio.subprocess.create_subprocess_exec") as create_subprocess_exec:
            proc = create_subprocess_exec.return_value
            proc.returncode = 0
            proc.communicate.return_value = (fake_stdout, fake_stderr)

            rc, out, err = await exectools.cmd_gather_async(
                cmd,
                cwd=fake_cwd,
                env=None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
            )
            create_subprocess_exec.assert_awaited_once_with(
                *cmd,
                cwd=fake_cwd,
                env=None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
            )
            self.assertEqual(rc, 0)
            self.assertEqual(out, fake_stdout.decode("utf-8"))
            self.assertEqual(err, fake_stderr.decode("utf-8"))

    async def test_cmd_gather_async_timeout_kills_process(self):
        cmd = ["uname", "-a"]
        with mock.patch("asyncio.subprocess.create_subprocess_exec") as create_subprocess_exec:
            proc = create_subprocess_exec.return_value
            proc.communicate.side_effect = asyncio.TimeoutError()
            proc.kill = mock.MagicMock()
            proc.wait.return_value = 0

            with self.assertRaises(ChildProcessError):
                await exectools.cmd_gather_async(cmd, timeout=1)

            proc.kill.assert_called_once_with()
            proc.wait.assert_awaited()

    async def test_cmd_assert_async(self):
        cmd = ["uname", "-a"]
        fake_cwd = "/foo/bar"
        with mock.patch("asyncio.subprocess.create_subprocess_exec") as create_subprocess_exec:
            proc = create_subprocess_exec.return_value
            proc.wait.return_value = 0

            rc = await exectools.cmd_assert_async(cmd, cwd=fake_cwd)
            create_subprocess_exec.assert_awaited_once_with(*cmd, cwd=fake_cwd)
            self.assertEqual(rc, 0)

    @mock.patch("artcommonlib.exectools.cmd_assert_async")
    async def test_manifest_tool_uses_explicit_auth_file(self, cmd_assert_async: mock.AsyncMock):
        await exectools.manifest_tool(
            ["push", "from-spec", "--", "/tmp/manifest-list.yaml"],
            auth_file="/tmp/quay-auth.json",
        )

        cmd_assert_async.assert_awaited_once_with(
            ["manifest-tool", "--docker-cfg=/tmp/quay-auth.json", "push", "from-spec", "--", "/tmp/manifest-list.yaml"],
            stdout=sys.stderr,
            stderr=sys.stderr,
        )

    @mock.patch("artcommonlib.exectools.cmd_assert_async")
    async def test_manifest_tool_ignores_xdg_runtime_auth_file(self, cmd_assert_async: mock.AsyncMock):
        with mock.patch.dict(exectools.os.environ, {"XDG_RUNTIME_DIR": "/run/user/984"}, clear=True):
            await exectools.manifest_tool(["push", "from-spec", "--", "/tmp/manifest-list.yaml"])

        cmd_assert_async.assert_awaited_once_with(
            ["manifest-tool", "push", "from-spec", "--", "/tmp/manifest-list.yaml"],
            stdout=sys.stderr,
            stderr=sys.stderr,
        )

    @mock.patch("artcommonlib.exectools.cmd_assert_async")
    async def test_manifest_tool_uses_explicit_auth_file_over_env(self, cmd_assert_async: mock.AsyncMock):
        with mock.patch.dict(exectools.os.environ, {"QUAY_AUTH_FILE": "/tmp/env-auth.json"}, clear=True):
            await exectools.manifest_tool(
                ["push", "from-spec", "--", "/tmp/manifest-list.yaml"],
                auth_file="/tmp/explicit-auth.json",
            )

        cmd_assert_async.assert_awaited_once_with(
            [
                "manifest-tool",
                "--docker-cfg=/tmp/explicit-auth.json",
                "push",
                "from-spec",
                "--",
                "/tmp/manifest-list.yaml",
            ],
            stdout=sys.stderr,
            stderr=sys.stderr,
        )

    @mock.patch("artcommonlib.exectools.cmd_assert_async")
    async def test_manifest_tool_ignores_quay_auth_file_env(self, cmd_assert_async: mock.AsyncMock):
        with mock.patch.dict(exectools.os.environ, {"QUAY_AUTH_FILE": "/tmp/env-auth.json"}, clear=True):
            await exectools.manifest_tool(["push", "from-spec", "--", "/tmp/manifest-list.yaml"])

        cmd_assert_async.assert_awaited_once_with(
            ["manifest-tool", "push", "from-spec", "--", "/tmp/manifest-list.yaml"],
            stdout=sys.stderr,
            stderr=sys.stderr,
        )

    @mock.patch("artcommonlib.exectools.cmd_assert_async")
    async def test_manifest_tool_adds_quay_host_compat_entry(self, cmd_assert_async: mock.AsyncMock):
        captured_auth = {}

        async def _capture_auth(cmd, **_kwargs):
            self.assertEqual(cmd[0], "manifest-tool")
            self.assertTrue(cmd[1].startswith("--docker-cfg="))
            compat_auth_file = cmd[1].split("=", 1)[1]
            with open(compat_auth_file, encoding="utf-8") as f:
                captured_auth.update(json.load(f))

        cmd_assert_async.side_effect = _capture_auth

        with tempfile.TemporaryDirectory() as temp_dir:
            auth_file = f"{temp_dir}/auth.json"
            manifest_spec = f"{temp_dir}/manifest-list.yaml"
            with open(auth_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "auths": {
                            "quay.io/openshift-release-dev": {
                                "auth": "Zm9vOmJhcg==",
                            }
                        }
                    },
                    f,
                )
            with open(manifest_spec, "w", encoding="utf-8") as f:
                f.write("image: quay.io/openshift-release-dev/ocp-v4.0-art-dev:test-tag\n")

            await exectools.manifest_tool(
                ["push", "from-spec", "--", manifest_spec],
                auth_file=auth_file,
            )

        self.assertEqual(captured_auth["auths"]["quay.io"], {"auth": "Zm9vOmJhcg=="})
        self.assertEqual(captured_auth["auths"]["quay.io/openshift-release-dev"], {"auth": "Zm9vOmJhcg=="})

    @mock.patch("artcommonlib.exectools.cmd_assert_async")
    async def test_manifest_tool_selects_best_matching_repo_cred(self, cmd_assert_async: mock.AsyncMock):
        captured_auth = {}

        async def _capture_auth(cmd, **_kwargs):
            compat_auth_file = cmd[1].split("=", 1)[1]
            with open(compat_auth_file, encoding="utf-8") as f:
                captured_auth.update(json.load(f))

        cmd_assert_async.side_effect = _capture_auth

        with tempfile.TemporaryDirectory() as temp_dir:
            auth_file = f"{temp_dir}/auth.json"
            manifest_spec = f"{temp_dir}/manifest-list.yaml"
            with open(auth_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "auths": {
                            "quay.io/redhat-user-workloads/ocp-art-tenant/art-images": {
                                "auth": "YXJ0OnRva2Vu",
                            },
                            "quay.io/openshift-release-dev": {
                                "auth": "cmVsZWFzZTpkZXY=",
                            },
                        }
                    },
                    f,
                )
            with open(manifest_spec, "w", encoding="utf-8") as f:
                f.write("image: quay.io/redhat-user-workloads/ocp-art-tenant/art-images:test-tag\n")

            await exectools.manifest_tool(
                ["push", "from-spec", "--", manifest_spec],
                auth_file=auth_file,
            )

        self.assertEqual(captured_auth["auths"]["quay.io"], {"auth": "YXJ0OnRva2Vu"})

    @mock.patch("artcommonlib.exectools.cmd_assert_async")
    async def test_manifest_tool_reuses_cached_compat_auth_file(self, cmd_assert_async: mock.AsyncMock):
        compat_paths = []

        async def _capture_auth(cmd, **_kwargs):
            compat_paths.append(cmd[1].split("=", 1)[1])

        cmd_assert_async.side_effect = _capture_auth

        with tempfile.TemporaryDirectory() as temp_dir:
            auth_file = f"{temp_dir}/auth.json"
            manifest_spec = f"{temp_dir}/manifest-list.yaml"
            with open(auth_file, "w", encoding="utf-8") as f:
                json.dump({"auths": {"quay.io/openshift-release-dev": {"auth": "Zm9vOmJhcg=="}}}, f)
            with open(manifest_spec, "w", encoding="utf-8") as f:
                f.write("image: quay.io/openshift-release-dev/ocp-v4.0-art-dev:test-tag\n")

            await exectools.manifest_tool(["push", "from-spec", "--", manifest_spec], auth_file=auth_file)
            await exectools.manifest_tool(["push", "from-spec", "--", manifest_spec], auth_file=auth_file)

        self.assertEqual(len(compat_paths), 2)
        self.assertEqual(compat_paths[0], compat_paths[1])

    def test_parallel_exec(self):
        items = [1, 2, 3]
        results = exectools.parallel_exec(lambda k, v: k, items, n_threads=4)
        results = results.get()
        self.assertEqual(results, items)

    async def test_limit_concurrency(self):
        """Test that limit_concurrency actually limits concurrent execution"""
        concurrent_count = 0
        max_concurrent = 0

        @exectools.limit_concurrency(limit=2)
        async def test_func(name):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)

            await asyncio.sleep(0.1)  # Simulate work

            concurrent_count -= 1
            return name

        # Run 5 tasks with limit=2
        tasks = [test_func(f"task{i}") for i in range(5)]

        start_time = time.time()
        results = await asyncio.gather(*tasks)
        duration = time.time() - start_time

        # Should never have more than 2 concurrent executions
        self.assertEqual(max_concurrent, 2)

        # All tasks should complete
        self.assertEqual(len(results), 5)

        # With limit=2 and 5 tasks of 0.1s each, should take at least 0.25s
        self.assertGreaterEqual(duration, 0.25)


class TestRedactEnvForLogging(unittest.TestCase):
    def test_redacts_password_keys(self):
        env = {"GIT_PASSWORD": "ghs_secret123", "DB_PASSWORD": "hunter2"}
        result = exectools._redact_env_for_logging(env)
        self.assertEqual(result["GIT_PASSWORD"], "***REDACTED***")
        self.assertEqual(result["DB_PASSWORD"], "***REDACTED***")

    def test_redacts_token_keys(self):
        env = {"GITHUB_TOKEN": "ghp_abc", "SLACK_BOT_TOKEN": "xoxb-123"}
        result = exectools._redact_env_for_logging(env)
        self.assertEqual(result["GITHUB_TOKEN"], "***REDACTED***")
        self.assertEqual(result["SLACK_BOT_TOKEN"], "***REDACTED***")

    def test_redacts_secret_and_key_and_credential(self):
        env = {"API_SECRET": "s3cr3t", "PRIVATE_KEY": "pem-data", "SERVICE_CREDENTIAL": "cred"}
        result = exectools._redact_env_for_logging(env)
        for k in env:
            self.assertEqual(result[k], "***REDACTED***")

    def test_preserves_non_sensitive_keys(self):
        env = {"GIT_SSH_COMMAND": "ssh -oBatchMode=yes", "GIT_TERMINAL_PROMPT": "0", "GIT_ASKPASS": "/tmp/script.sh"}
        result = exectools._redact_env_for_logging(env)
        self.assertEqual(result, env)

    def test_case_insensitive(self):
        env = {"git_password": "secret", "Github_Token": "tok"}
        result = exectools._redact_env_for_logging(env)
        self.assertEqual(result["git_password"], "***REDACTED***")
        self.assertEqual(result["Github_Token"], "***REDACTED***")

    def test_empty_dict(self):
        self.assertEqual(exectools._redact_env_for_logging({}), {})

    def test_cmd_gather_does_not_log_secrets(self):
        secret_val = "ghs_SuperSecretToken123"
        env = {"GIT_PASSWORD": secret_val, "GIT_ASKPASS": "/tmp/askpass.sh"}
        with mock.patch("subprocess.Popen") as MockPopen:
            mock_popen = MockPopen.return_value
            mock_popen.communicate.return_value = (b"ok\n", b"")
            mock_popen.returncode = 0

            with self.assertLogs(level=logging.DEBUG) as cm:
                exectools.cmd_gather(["/usr/bin/echo", "hi"], set_env=env)
                log_text = "\n".join(cm.output)
                self.assertNotIn(secret_val, log_text)
                self.assertIn("***REDACTED***", log_text)
                self.assertIn("/tmp/askpass.sh", log_text)


if __name__ == "__main__":
    unittest.main()
