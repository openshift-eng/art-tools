import subprocess
import unittest

from functional_tests import constants


class VerifyPayloadTestCase(unittest.TestCase):
    def test_verify_payload(self):
        cmd = constants.ELLIOTT_CMD + [
            "--group=openshift-4.2",
            "verify-payload",
            "quay.io/openshift-release-dev/ocp-release:4.2.12",
            "49645",
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode,
            0,
            msg=f"stdout: {result.stdout.decode('utf-8')}, stderr: {result.stderr.decode('utf-8')}",
        )
        self.assertIn("Summary results:", result.stdout.decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
