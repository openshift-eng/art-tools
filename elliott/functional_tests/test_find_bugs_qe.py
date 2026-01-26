import subprocess
import unittest

from functional_tests import constants


class FindBugsQETestCase(unittest.TestCase):
    def test_find_bugs_qe(self):
        cmd = constants.ELLIOTT_CMD + [
            "--assembly=stream",
            "--group=openshift-4.6",
            "find-bugs:qe",
            "--noop",
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 0, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertRegex(result.stderr.decode(), "Found \\d+ bugs")


if __name__ == "__main__":
    unittest.main()
