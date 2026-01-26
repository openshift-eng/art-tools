import subprocess
import unittest

from functional_tests import constants


class GetTestCase(unittest.TestCase):
    def test_get_erratum(self):
        cmd = constants.ELLIOTT_CMD + ["get", "49982"]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 0, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertIn("49982", result.stdout.decode())

    def test_get_erratum_with_group(self):
        cmd = constants.ELLIOTT_CMD + [
            "--assembly=stream",
            "--group=openshift-4.2",
            "get",
            "--use-default-advisory",
            "rpm",
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 0, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertIn(constants.ERRATA_TOOL_URL, result.stdout.decode())


if __name__ == "__main__":
    unittest.main()
