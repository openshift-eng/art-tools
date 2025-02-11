import unittest
import subprocess

from doozerlib.backend.konflux_client import KIND_SNAPSHOT, API_VERSION
from functional_tests import constants


class NewSnapshotTestCase(unittest.TestCase):
    def test_new_snapshot(self):
        cmd = constants.ELLIOTT_CMD + [
            "--assembly=stream", "--group=openshift-4.18", "snapshot", "new",
            "sriov-network-operator-v4.18.0-202502251712.p0.gf496851.assembly.stream.el9"
        ]
        result = subprocess.run(cmd, capture_output=True)
        self.assertEqual(result.returncode, 0,
                         msg=f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}")
        self.assertRegex(result.stderr.decode(), f"Would have created {API_VERSION}/{KIND_SNAPSHOT}")


if __name__ == '__main__':
    unittest.main()
