import subprocess
import unittest

from tests_functional_elliott import constants


class GreateTestCase(unittest.TestCase):
    def test_create_rhba(self):
        out = subprocess.check_output(
            constants.ELLIOTT_CMD
            + [
                "--assembly=stream",
                "--group=openshift-4.6",
                "create",
                "--type=RHBA",
                "--art-advisory-key=rpm",
                "--date=2020-Jan-1",
                "--assigned-to=openshift-qe-errata@redhat.com",
                "--manager=vlaad@redhat.com",
                "--package-owner=jdelft@redhat.com",
            ],
        )
        self.assertIn("Would have created advisory:", out.decode("utf-8"))


if __name__ == '__main__':
    unittest.main()
