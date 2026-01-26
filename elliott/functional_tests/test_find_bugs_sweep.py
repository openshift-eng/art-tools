import subprocess
import unittest

from functional_tests import constants


class FindBugsSweepTestCase(unittest.TestCase):
    def test_sweep_bugs(self):
        cmd = constants.ELLIOTT_CMD + [
            "--group=openshift-4.11",
            "--assembly=4.11.36",  # This assembly has a pinned bug
            "find-bugs:sweep",
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 0, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertRegex(result.stdout.decode(), "Found \\d+ bugs")


if __name__ == "__main__":
    unittest.main()
