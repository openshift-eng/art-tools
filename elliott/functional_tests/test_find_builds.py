import subprocess
import unittest

from functional_tests import constants

# This test may start failing once this version is EOL and we either change the
# ocp-build-data bugzilla schema or all of the non-shipped builds are garbage-collected.
version = "4.12"


class FindBuildsTestCase(unittest.TestCase):
    def test_find_rpms(self):
        cmd = constants.ELLIOTT_CMD + [
            "--assembly=stream",
            f"--group=openshift-{version}",
            "find-builds",
            "--kind=rpm",
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 0, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertRegex(result.stderr.decode(), "Found \\d+ builds")

    def test_find_images(self):
        cmd = constants.ELLIOTT_CMD + [
            f"--group=openshift-{version}",
            "-i",
            "openshift-enterprise-cli",
            "find-builds",
            "--kind=image",
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 0, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertRegex(result.stderr.decode(), "Found \\d+ builds")

    def test_change_state(self):
        """To attach a build to an advisory, it will be attempted to set the
        advisory to NEW_FILES. This advisory is already SHIPPED_LIVE, and the
        attempted change should fail"""

        command = constants.ELLIOTT_CMD + [
            f"--group=openshift-{version}",
            "--images=openshift-enterprise-cli",
            "find-builds",
            "--kind=image",
            "--attach=57899",
        ]
        result = subprocess.run(command, capture_output=True)

        self.assertEqual(
            result.returncode, 1, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertIn("Cannot change state", result.stdout.decode())


if __name__ == "__main__":
    unittest.main()
