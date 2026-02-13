import sys
import unittest

from flexmock import flexmock
from validator import exceptions, support

group_cfg = {
    "urls": {"cgit": "http://my.cgit.endpoint"},
    "branch": "rhaos-{MAJOR}.{MINOR}-rhel-999",
    "vars": {"MAJOR": 4, "MINOR": 10},
}


class TestSupport(unittest.TestCase):
    def test_fail_validation(self):
        self.assertRaises(exceptions.ValidationFailedWIP, support.fail_validation, "msg", {"mode": "wip"})

        self.assertRaises(exceptions.ValidationFailed, support.fail_validation, "msg", {"mode": "other"})

        self.assertRaises(exceptions.ValidationFailed, support.fail_validation, "msg", {})

    def test_is_disabled(self):
        self.assertTrue(support.is_disabled({"mode": "disabled"}))
        self.assertFalse(support.is_disabled({"mode": "anything-else"}))
        self.assertFalse(support.is_disabled({}))

    def test_load_group_config_for(self):
        fake_group_yaml = """
        key_1: value_1
        key_2: value_2
        """
        expected = {"key_1": "value_1", "key_2": "value_2"}

        mock_builtin_open("./group.yml", fake_group_yaml)
        actual = support.load_group_config_for("images/x.yml")
        self.assertEqual(actual, expected)

        mock_builtin_open("../../group.yml", fake_group_yaml)
        actual = support.load_group_config_for("../../rpms/x.yml")
        self.assertEqual(actual, expected)

        mock_builtin_open("/var/lib/group.yml", fake_group_yaml)
        actual = support.load_group_config_for("/var/lib/images/x.yml")
        self.assertEqual(actual, expected)

    def test_get_ocp_build_data_dir(self):
        _dir = support.get_ocp_build_data_dir("/a/b/ocp-build-data/rpms/x.yml")
        self.assertEqual(_dir, "/a/b/ocp-build-data")

        _dir = support.get_ocp_build_data_dir("../rpms/foo.yml")
        self.assertEqual(_dir, "..")

        _dir = support.get_ocp_build_data_dir("rpms/foo.yml")
        self.assertEqual(_dir, ".")

    def test_get_artifact_type_image(self):
        artifact_type = support.get_artifact_type("/path/to/images/x.yml")
        self.assertEqual(artifact_type, "image")

        artifact_type = support.get_artifact_type("../images/x.yml")
        self.assertEqual(artifact_type, "image")

        artifact_type = support.get_artifact_type("images/x.yml")
        self.assertEqual(artifact_type, "image")

    def test_get_artifact_type_rpm(self):
        artifact_type = support.get_artifact_type("/path/to/rpms/x.yml")
        self.assertEqual(artifact_type, "rpm")

        artifact_type = support.get_artifact_type("../rpms/x.yml")
        self.assertEqual(artifact_type, "rpm")

        artifact_type = support.get_artifact_type("rpms/x.yml")
        self.assertEqual(artifact_type, "rpm")

    def test_get_artifact_type_unknown(self):
        artifact_type = support.get_artifact_type("/path/to/unknown/x.yml")
        self.assertEqual(artifact_type, "???")

        artifact_type = support.get_artifact_type("../images-on-path/x.yml")
        self.assertEqual(artifact_type, "???")

        artifact_type = support.get_artifact_type("unknown/rpms-on-name.yml")
        self.assertEqual(artifact_type, "???")

    def test_get_valid_streams_for(self):
        fake_streams_yaml = """
        cobol:
          image: openshift/cobol-builder:latest
        fortran:
          image: openshift/fortran-builder:latest
        """
        mock_builtin_open("./streams.yml", fake_streams_yaml)

        streams = support.get_valid_streams_for("images/x.yml")
        self.assertEqual(streams, {"cobol", "fortran"})

    def test_get_valid_member_references_for(self):
        (
            flexmock(support.os)
            .should_receive("listdir")
            .with_args("./images")
            .and_return({"image-a", "image-b", "image-c"})
        )

        references = support.get_valid_member_references_for("images/x.yml")
        self.assertEqual(references, {"image-a", "image-b", "image-c"})

    def test_resource_exists(self):
        for status_code in [200, 204, 302, 304, 307, 308]:
            (flexmock(support.requests).should_receive("head").and_return(flexmock(status_code=status_code)))
            self.assertTrue(support.resource_exists("http://a.random/url"))

        for status_code in [400, 401, 403, 404, 405, 410]:
            (flexmock(support.requests).should_receive("head").and_return(flexmock(status_code=status_code)))
            self.assertFalse(support.resource_exists("http://a.random/url"))

    def test_resource_is_reachable(self):
        (flexmock(support.requests).should_receive("head").replace_with(lambda _: None))
        self.assertTrue(support.resource_is_reachable("http://a.random/url"))

        (flexmock(support.requests).should_receive("head").and_raise(support.requests.exceptions.ConnectionError))
        self.assertFalse(support.resource_is_reachable("http://a.random/url"))

    def test_namespace(self):
        self.assertEqual(support.get_namespace({}, "images/foo.yml"), "containers")
        self.assertEqual(support.get_namespace({}, "rpms/bar.yml"), "rpms")
        self.assertEqual(support.get_namespace({}, "group.yml"), "???")
        self.assertEqual(support.get_namespace({"distgit": {"namespace": "apbs"}}, "images/yadda.yml"), "apbs")

    def test_repository_name(self):
        self.assertEqual(support.get_repository_name("images/yadda.yml"), "yadda")
        self.assertEqual(support.get_repository_name("images/foo.bar.yml"), "foo")

    def test_distgit_branch(self):
        self.assertEqual(support.get_distgit_branch({}, group_cfg), "rhaos-4.10-rhel-999")
        self.assertEqual(support.get_distgit_branch({"distgit": {"branch": "brunchbar"}}, group_cfg), "brunchbar")


def mock_builtin_open(file, contents):
    mock = flexmock(get_builtin_module())
    mock.should_call("open")
    (mock.should_receive("open").with_args(file).and_return(flexmock(read=lambda: contents)))


def get_builtin_module():
    return sys.modules.get("__builtin__", sys.modules.get("builtins"))
