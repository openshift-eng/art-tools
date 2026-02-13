import subprocess
import unittest

from functional_tests import constants


class ReleaseTestCases(unittest.TestCase):
    def test_release_watch_fail(self):
        cmd = constants.ELLIOTT_CMD + ["release", "watch", "ose-4-18-202503131641"]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 1, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertRegex(result.stdout.decode(), "Release failed!")

    def test_release_watch_success(self):
        cmd = constants.ELLIOTT_CMD + ["release", "watch", "ose-4-18-prod-202503171852", "--timeout=5"]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 0, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertRegex(result.stdout.decode(), "Release successful!")


if __name__ == "__main__":
    unittest.main()
