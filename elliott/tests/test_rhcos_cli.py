import unittest
from unittest.mock import MagicMock, patch

from click import ClickException
from elliottlib.cli.rhcos_cli import (
    _get_layered_tags,
    _get_non_layered_tags,
    _get_rhcos_pullspec_from_payload,
    _verify_tag,
    get_rhcos_tags,
)


class TestGetRhcosPullspecFromPayload(unittest.TestCase):
    @patch("elliottlib.cli.rhcos_cli.exectools.cmd_assert")
    def test_extracts_pullspec(self, mock_cmd):
        mock_cmd.return_value = ("quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123\n", "")
        result = _get_rhcos_pullspec_from_payload("payload:latest", "rhel-coreos")
        mock_cmd.assert_called_once_with(
            ["oc", "adm", "release", "info", "--image-for", "rhel-coreos", "--", "payload:latest"]
        )
        self.assertEqual(result, "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123")


class TestVerifyTag(unittest.TestCase):
    @patch("elliottlib.cli.rhcos_cli.oc_image_info_show_multiarch")
    def test_passes_when_live(self, mock_info):
        mock_info.return_value = {"some": "info"}
        _verify_tag("quay.io/repo:tag")
        mock_info.assert_called_once_with("quay.io/repo:tag", strict=False)

    @patch("elliottlib.cli.rhcos_cli.oc_image_info_show_multiarch")
    def test_raises_when_not_live(self, mock_info):
        mock_info.return_value = None
        with self.assertRaises(ClickException) as ctx:
            _verify_tag("quay.io/repo:tag")
        self.assertIn("not live", str(ctx.exception))


class TestGetRhcosTags(unittest.TestCase):
    @patch("elliottlib.cli.rhcos_cli._get_layered_tags")
    @patch("elliottlib.cli.rhcos_cli.oc_image_info")
    @patch("elliottlib.cli.rhcos_cli._get_rhcos_pullspec_from_payload")
    @patch("elliottlib.cli.rhcos_cli.get_container_configs")
    @patch("elliottlib.cli.rhcos_cli.get_art_prod_image_repo_for_version")
    def test_dispatches_layered(self, mock_art_repo, mock_configs, mock_get_pullspec, mock_info, mock_layered):
        mock_art_repo.return_value = "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
        primary = MagicMock()
        primary.primary = True
        primary.name = "rhel-coreos"
        mock_configs.return_value = [primary]
        mock_get_pullspec.return_value = "quay.io/art-dev@sha256:abc"
        mock_info.return_value = {
            "config": {
                "architecture": "amd64",
                "config": {"Labels": {"coreos.build.manifest-list-tag": "4.19-9.6-202505-node-image"}},
            }
        }
        mock_layered.return_value = {"rhel-coreos": "quay.io/art-dev:4.19-9.6-202505-node-image"}

        runtime = MagicMock()
        result = get_rhcos_tags(runtime, "payload:latest")

        mock_layered.assert_called_once()
        self.assertEqual(result, {"rhel-coreos": "quay.io/art-dev:4.19-9.6-202505-node-image"})

    @patch("elliottlib.cli.rhcos_cli._get_non_layered_tags")
    @patch("elliottlib.cli.rhcos_cli.oc_image_info")
    @patch("elliottlib.cli.rhcos_cli._get_rhcos_pullspec_from_payload")
    @patch("elliottlib.cli.rhcos_cli.get_container_configs")
    @patch("elliottlib.cli.rhcos_cli.get_art_prod_image_repo_for_version")
    def test_dispatches_non_layered(self, mock_art_repo, mock_configs, mock_get_pullspec, mock_info, mock_non_layered):
        mock_art_repo.return_value = "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
        primary = MagicMock()
        primary.primary = True
        primary.name = "machine-os-content"
        mock_configs.return_value = [primary]
        mock_get_pullspec.return_value = "quay.io/art-dev@sha256:abc"
        mock_info.return_value = {
            "config": {
                "architecture": "amd64",
                "config": {"Labels": {"org.opencontainers.image.version": "418.94.202605101521-0"}},
            }
        }
        mock_non_layered.return_value = {"machine-os-content": "quay.io/art-dev:418.94.202605101521-0-coreos"}

        runtime = MagicMock()
        result = get_rhcos_tags(runtime, "payload:latest")

        mock_non_layered.assert_called_once()
        self.assertEqual(result, {"machine-os-content": "quay.io/art-dev:418.94.202605101521-0-coreos"})

    @patch("elliottlib.cli.rhcos_cli.oc_image_info")
    @patch("elliottlib.cli.rhcos_cli._get_rhcos_pullspec_from_payload")
    @patch("elliottlib.cli.rhcos_cli.get_container_configs")
    @patch("elliottlib.cli.rhcos_cli.get_art_prod_image_repo_for_version")
    def test_raises_when_no_build_id(self, mock_art_repo, mock_configs, mock_get_pullspec, mock_info):
        mock_art_repo.return_value = "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
        primary = MagicMock()
        primary.primary = True
        primary.name = "machine-os-content"
        mock_configs.return_value = [primary]
        mock_get_pullspec.return_value = "quay.io/art-dev@sha256:abc"
        mock_info.return_value = {
            "config": {
                "architecture": "amd64",
                "config": {"Labels": {}},
            }
        }

        runtime = MagicMock()
        with self.assertRaises(ClickException) as ctx:
            get_rhcos_tags(runtime, "payload:latest")
        self.assertIn("Cannot determine RHCOS build ID", str(ctx.exception))


class TestGetLayeredTags(unittest.TestCase):
    @patch("elliottlib.cli.rhcos_cli._verify_tag")
    @patch("elliottlib.cli.rhcos_cli.oc_image_info")
    @patch("elliottlib.cli.rhcos_cli._get_rhcos_pullspec_from_payload")
    def test_constructs_tags(self, mock_get_pullspec, mock_info, mock_verify):
        art_repo = "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
        conf1 = MagicMock()
        conf1.name = "rhel-coreos"
        conf2 = MagicMock()
        conf2.name = "rhel-coreos-extensions"

        mock_get_pullspec.side_effect = ["quay.io/art-dev@sha256:aaa", "quay.io/art-dev@sha256:bbb"]
        mock_info.side_effect = [
            {"config": {"config": {"Labels": {"coreos.build.manifest-list-tag": "4.19-9.6-202505-node-image"}}}},
            {
                "config": {
                    "config": {"Labels": {"coreos.build.manifest-list-tag": "4.19-9.6-202505-node-image-extensions"}}
                }
            },
        ]

        runtime = MagicMock()
        result = _get_layered_tags(runtime, "payload:latest", [conf1, conf2], art_repo)

        self.assertEqual(result["rhel-coreos"], f"{art_repo}:4.19-9.6-202505-node-image")
        self.assertEqual(result["rhel-coreos-extensions"], f"{art_repo}:4.19-9.6-202505-node-image-extensions")
        self.assertEqual(mock_verify.call_count, 2)

    @patch("elliottlib.cli.rhcos_cli.oc_image_info")
    @patch("elliottlib.cli.rhcos_cli._get_rhcos_pullspec_from_payload")
    def test_raises_when_no_label(self, mock_get_pullspec, mock_info):
        conf = MagicMock()
        conf.name = "rhel-coreos"
        mock_get_pullspec.return_value = "quay.io/art-dev@sha256:aaa"
        mock_info.return_value = {"config": {"config": {"Labels": {}}}}

        runtime = MagicMock()
        with self.assertRaises(ClickException) as ctx:
            _get_layered_tags(runtime, "payload:latest", [conf], "art-repo")
        self.assertIn("No coreos.build.manifest-list-tag", str(ctx.exception))


class TestGetNonLayeredTags(unittest.TestCase):
    @patch("elliottlib.cli.rhcos_cli._verify_tag")
    @patch("elliottlib.cli.rhcos_cli.request.urlopen")
    def test_constructs_tags_from_meta(self, mock_urlopen, mock_verify):
        build_id = "418.94.202605101521-0"
        meta = {
            "base-oscontainer": {
                "image": "quay.io/openshift-release-dev/ocp-v4.0-art-dev",
                "digest": "sha256:aaa",
                "tags": ["4.18-9.4-coreos", f"{build_id}-coreos"],
            },
            "extensions-container": {
                "image": "quay.io/openshift-release-dev/ocp-v4.0-art-dev",
                "digest": "sha256:bbb",
                "tags": ["4.18-9.4-coreos-extensions", f"{build_id}-coreos-extensions"],
            },
        }
        import io
        import json

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(meta).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        conf1 = MagicMock()
        conf1.name = "rhel-coreos"
        conf1.build_metadata_key = "base-oscontainer"
        conf2 = MagicMock()
        conf2.name = "rhel-coreos-extensions"
        conf2.build_metadata_key = "extensions-container"

        runtime = MagicMock()
        runtime.get_major_minor.return_value = (4, 18)
        runtime.group_config.vars.RHCOS_EL_MAJOR = 9
        runtime.group_config.vars.RHCOS_EL_MINOR = 4

        art_repo = "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
        result = _get_non_layered_tags(runtime, build_id, "x86_64", [conf1, conf2], art_repo)

        self.assertEqual(result["rhel-coreos"], f"{art_repo}:{build_id}-coreos")
        self.assertEqual(result["rhel-coreos-extensions"], f"{art_repo}:{build_id}-coreos-extensions")
        self.assertEqual(mock_verify.call_count, 2)

    @patch("elliottlib.cli.rhcos_cli.request.urlopen")
    def test_raises_when_key_missing(self, mock_urlopen):
        import io
        import json

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        conf = MagicMock()
        conf.name = "rhel-coreos"
        conf.build_metadata_key = "base-oscontainer"

        runtime = MagicMock()
        runtime.get_major_minor.return_value = (4, 18)
        runtime.group_config.vars.RHCOS_EL_MAJOR = 9
        runtime.group_config.vars.RHCOS_EL_MINOR = 4

        with self.assertRaises(ClickException) as ctx:
            _get_non_layered_tags(runtime, "418.94.202605101521-0", "x86_64", [conf], "art-repo")
        self.assertIn("not found in RHCOS metadata", str(ctx.exception))

    @patch("elliottlib.cli.rhcos_cli.request.urlopen")
    def test_raises_when_no_build_tag(self, mock_urlopen):
        import io
        import json

        meta = {
            "base-oscontainer": {
                "image": "quay.io/art-dev",
                "digest": "sha256:aaa",
                "tags": ["4.18-9.4-coreos"],
            },
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(meta).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        conf = MagicMock()
        conf.name = "rhel-coreos"
        conf.build_metadata_key = "base-oscontainer"

        runtime = MagicMock()
        runtime.get_major_minor.return_value = (4, 18)
        runtime.group_config.vars.RHCOS_EL_MAJOR = 9
        runtime.group_config.vars.RHCOS_EL_MINOR = 4

        with self.assertRaises(ClickException) as ctx:
            _get_non_layered_tags(runtime, "418.94.202605101521-0", "x86_64", [conf], "art-repo")
        self.assertIn("No build-specific tag", str(ctx.exception))

    @patch("elliottlib.cli.rhcos_cli.request.urlopen")
    def test_url_uses_el_version_when_gt_8(self, mock_urlopen):
        import json

        build_id = "418.94.202605101521-0"
        meta = {
            "oscontainer": {
                "image": "quay.io/art-dev",
                "digest": "sha256:aaa",
                "tags": [f"{build_id}-coreos"],
            },
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(meta).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        conf = MagicMock()
        conf.name = "machine-os-content"
        conf.build_metadata_key = "oscontainer"

        runtime = MagicMock()
        runtime.get_major_minor.return_value = (4, 18)
        runtime.group_config.vars.RHCOS_EL_MAJOR = 9
        runtime.group_config.vars.RHCOS_EL_MINOR = 4

        with patch("elliottlib.cli.rhcos_cli._verify_tag"):
            _get_non_layered_tags(runtime, build_id, "x86_64", [conf], "art-repo")

        url_arg = mock_urlopen.call_args[0][0]
        self.assertIn("/4.18-9.4/", url_arg)

    @patch("elliottlib.cli.rhcos_cli.request.urlopen")
    def test_url_uses_major_minor_when_el8(self, mock_urlopen):
        import json

        build_id = "415.92.202301010101-0"
        meta = {
            "oscontainer": {
                "image": "quay.io/art-dev",
                "digest": "sha256:aaa",
                "tags": [f"{build_id}-coreos"],
            },
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(meta).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        conf = MagicMock()
        conf.name = "machine-os-content"
        conf.build_metadata_key = "oscontainer"

        runtime = MagicMock()
        runtime.get_major_minor.return_value = (4, 15)
        runtime.group_config.vars.RHCOS_EL_MAJOR = 8
        runtime.group_config.vars.RHCOS_EL_MINOR = 8

        with patch("elliottlib.cli.rhcos_cli._verify_tag"):
            _get_non_layered_tags(runtime, build_id, "x86_64", [conf], "art-repo")

        url_arg = mock_urlopen.call_args[0][0]
        self.assertIn("/4.15/", url_arg)
        self.assertNotIn("-8.", url_arg)
