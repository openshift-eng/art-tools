#!/usr/bin/env python
"""
Test functions related to controlled command execution
"""

import asyncio
import logging
import subprocess
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

    async def test_cmd_assert_async(self):
        cmd = ["uname", "-a"]
        fake_cwd = "/foo/bar"
        with mock.patch("asyncio.subprocess.create_subprocess_exec") as create_subprocess_exec:
            proc = create_subprocess_exec.return_value
            proc.wait.return_value = 0

            rc = await exectools.cmd_assert_async(cmd, cwd=fake_cwd)
            create_subprocess_exec.assert_awaited_once_with(*cmd, cwd=fake_cwd)
            self.assertEqual(rc, 0)

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


if __name__ == "__main__":
    unittest.main()
