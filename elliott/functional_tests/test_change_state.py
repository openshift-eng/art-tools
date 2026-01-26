import subprocess
import unittest

from functional_tests import constants


class ChangeStateTestCase(unittest.TestCase):
    def test_change_state_fail(self):
        cmd = constants.ELLIOTT_CMD + [
            "--group=openshift-4.2",
            "change-state",
            "--state=QE",
            "--advisory=49645",
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Cannot change state from SHIPPED_LIVE to QE", result.stderr.decode())

    def test_change_state_from(self):
        cmd = constants.ELLIOTT_CMD + [
            "--group=openshift-4.2",
            "change-state",
            "--from=NEW_FILES",
            "--state=QE",
            "--advisory=49645",
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 0, msg=f"stdout: {result.stdout.decode()}, stderr: {result.stderr.decode()}"
        )
        self.assertIn("Current state is not NEW_FILES: SHIPPED_LIVE", result.stdout.decode())


if __name__ == "__main__":
    unittest.main()
