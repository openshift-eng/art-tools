import unittest

from flexmock import flexmock
from validator import cgit


class TestCgit(unittest.TestCase):
    def setUp(self):
        self.group_cfg = {
            "urls": {"cgit": "http://my.cgit.endpoint"},
            "branch": "rhaos-{MAJOR}.{MINOR}-rhel-999",
            "vars": {"MAJOR": 4, "MINOR": 2},
        }

        (flexmock(cgit.support).should_receive("resource_is_reachable").and_return(True))

        (flexmock(cgit.support).should_receive("resource_exists").and_return(True))

    def test_image_artifact(self):
        (url, err) = cgit.validate("images/foo.yml", {}, self.group_cfg)
        self.assertEqual(url, "http://my.cgit.endpoint/containers/foo")
        self.assertIsNone(err)

    def test_rpm_artifact(self):
        (url, err) = cgit.validate("rpms/bar.yml", {}, self.group_cfg)
        self.assertEqual(url, "http://my.cgit.endpoint/rpms/bar")
        self.assertIsNone(err)

    def test_unknown_artifact(self):
        (url, err) = cgit.validate("unknown/foo.yml", {}, self.group_cfg)
        self.assertEqual(url, "http://my.cgit.endpoint/???/foo")
        self.assertIsNone(err)

    def test_file_with_custom_namespace(self):
        data = {"distgit": {"namespace": "apbs"}}
        (url, err) = cgit.validate("images/foo.yml", data, self.group_cfg)
        self.assertEqual(url, "http://my.cgit.endpoint/apbs/foo")
        self.assertIsNone(err)

    def test_file_with_additional_extensions(self):
        (url, err) = cgit.validate("images/x.apb.y.yml", {}, self.group_cfg)
        self.assertEqual(url, "http://my.cgit.endpoint/containers/x")
        self.assertIsNone(err)

    def test_cgit_endpoint_not_reachable(self):
        (flexmock(cgit.support).should_receive("resource_is_reachable").and_return(False))

        (url, err) = cgit.validate("images/my-img.yml", {}, self.group_cfg)
        self.assertEqual(url, "http://my.cgit.endpoint/containers/my-img")
        self.assertEqual(err, ("This validation must run from a network with access to http://my.cgit.endpoint"))

    def test_cgit_repository_doesnt_exist(self):
        (flexmock(cgit.support).should_receive("resource_exists").and_return(False))

        (url, err) = cgit.validate("images/my-img.yml", {}, self.group_cfg)
        self.assertEqual(url, "http://my.cgit.endpoint/containers/my-img")
        self.assertEqual(
            err,
            (
                "Repo was not found in CGit cache.\n"
                "If you didn't request a DistGit repo yet, "
                "please check "
                "https://mojo.redhat.com/docs/DOC-1168290\n"
                "But if you already obtained one, make sure "
                "its name matches the YAML filename"
            ),
        )

    def test_use_branch_declared_on_file(self):
        expected_url = "http://my.cgit.endpoint/containers/img"

        (flexmock(cgit).should_receive("branch_exists").with_args("my-custom-branch", expected_url).and_return(True))

        data = {"distgit": {"branch": "my-custom-branch"}}
        (url, err) = cgit.validate("images/img.yml", data, self.group_cfg)
        self.assertEqual(url, expected_url)
        self.assertIsNone(err)

    def test_use_branch_declared_on_file_with_vars(self):
        expected_url = "http://my.cgit.endpoint/containers/img"

        (flexmock(cgit).should_receive("branch_exists").with_args("my-4-branch.2", expected_url).and_return(True))

        data = {"distgit": {"branch": "my-{MAJOR}-branch.{MINOR}"}}
        (url, err) = cgit.validate("images/img.yml", data, self.group_cfg)
        self.assertEqual(url, expected_url)
        self.assertIsNone(err)

    def test_use_branch_from_group_cfg(self):
        expected_url = "http://my.cgit.endpoint/containers/my-img"

        (
            flexmock(cgit)
            .should_receive("branch_exists")
            .with_args("rhaos-4.2-rhel-999", expected_url)  # defined on setUp
            .and_return(True)
        )

        (url, err) = cgit.validate("images/my-img.yml", {}, self.group_cfg)
        self.assertEqual(url, expected_url)
        self.assertIsNone(err)

    def test_branch_doesnt_exist(self):
        branch_url = "http://my.cgit.endpoint/containers/my-img/log?h=rhaos-4.2-rhel-999"  # defined on setUp

        (flexmock(cgit.support).should_receive("resource_exists").with_args(branch_url).and_return(False))

        (url, err) = cgit.validate("images/my-img.yml", {}, self.group_cfg)
        self.assertEqual(url, "http://my.cgit.endpoint/containers/my-img")
        self.assertEqual(err, "Branch rhaos-4.2-rhel-999 not found in CGit cache")
