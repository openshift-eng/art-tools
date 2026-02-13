import subprocess
import unittest

from doozerlib.backend.konflux_client import API_VERSION, KIND_SNAPSHOT

from functional_tests import constants


class SnapshotTestCases(unittest.TestCase):
    def test_snapshot_new(self):
        cmd = constants.ELLIOTT_CMD + [
            "--assembly=stream",
            "--group=openshift-4.18",
            "snapshot",
            "new",
            "sriov-network-operator-v4.18.0-202502251712.p0.gf496851.assembly.stream.el9",
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 0, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertRegex(result.stderr.decode(), f"Would have created {API_VERSION}/{KIND_SNAPSHOT}")

    def test_snapshot_get(self):
        cmd = constants.ELLIOTT_CMD + [
            "--assembly=stream",
            "--group=openshift-4.18",
            "snapshot",
            "get",
            "ose-4-18-202503121723",
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(
            result.returncode, 0, msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        self.assertIn("microshift-bootc-v4.18.0-202503121435.p0.gc0eec47.assembly.4.18.2.el9", result.stdout.decode())


if __name__ == "__main__":
    unittest.main()
