from unittest import TestCase
from unittest.mock import MagicMock, patch

from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder


class TestKonfluxCachi2(TestCase):
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_1(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = False
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False

        self.assertEqual(builder._prefetch(metadata=metadata, group="test-group"), [])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_2(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = False
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.config.content.source.pkg_managers = ["unknown"]

        self.assertEqual(builder._prefetch(metadata=metadata, group="test-group"), [])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_3(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.config.content.source.pkg_managers = ["gomod"]

        self.assertEqual(builder._prefetch(metadata=metadata, group="test-group"), [{"type": "gomod", "path": "."}])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_4(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "api"}]}

        self.assertEqual(builder._prefetch(metadata=metadata, group="test-group"), [{"type": "gomod", "path": "api"}])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_5(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}, {"path": "api"}, {"path": "client/pkg"}]}

        self.assertEqual(
            builder._prefetch(metadata=metadata, group="test-group"),
            [{"type": "gomod", "path": "."}, {"type": "gomod", "path": "api"}, {"type": "gomod", "path": "client/pkg"}],
        )

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_6(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.config.content.source.pkg_managers = ["npm", "gomod"]
        metadata.config.cachito.packages = {"npm": [{"path": "web"}], "gomod": [{"path": "."}]}

        self.assertEqual(
            builder._prefetch(metadata=metadata, group="test-group"),
            [{"type": "gomod", "path": "."}, {"type": "npm", "path": "web"}],
        )

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_rpm_lockfile_enabled_non_hermetic(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}]}

        result = builder._prefetch(metadata=metadata, group="test-group")
        expected = [{"type": "gomod", "path": "."}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_rpm_lockfile_enabled_hermetic(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        metadata.runtime.group_config.software_lifecycle.phase = "release"

        result = builder._prefetch(metadata=metadata, group="test-group")
        expected = [{"type": "rpm", "path": "."}, {"type": "gomod", "path": "."}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_rpm_lockfile_custom_path(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["npm"]
        metadata.config.cachito.packages = {"npm": [{"path": "frontend"}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "custom/path"

        metadata.runtime.group_config.software_lifecycle.phase = "release"

        result = builder._prefetch(metadata=metadata, group="test-group")
        expected = [{"type": "rpm", "path": "custom/path"}, {"type": "npm", "path": "frontend"}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_artifact_lockfile_enabled_hermetic(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = True
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}]}
        metadata.config.konflux.cachi2.artifact_lockfile.get.return_value = "."

        result = builder._prefetch(metadata=metadata, group="test-group")
        expected = [{"type": "generic", "path": "."}, {"type": "gomod", "path": "."}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_artifact_lockfile_disabled_hermetic(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}]}

        result = builder._prefetch(metadata=metadata, group="test-group")
        expected = [{"type": "gomod", "path": "."}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_rpm_lockfile_prerelease_dnf_options(self, mock_konflux_client_init):
        """Test that prerelease phase adds DNF options with gpgcheck=0 for all repository IDs"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        metadata.runtime.group_config.software_lifecycle.phase = "pre-release"
        metadata.get_enabled_repos.return_value = {"repo1", "repo2"}
        metadata.get_arches.return_value = ["x86_64", "aarch64"]

        mock_repo1 = MagicMock()
        mock_repo1.content_set.side_effect = lambda arch: f"repo1-content-set-{arch}"
        mock_repo2 = MagicMock()
        mock_repo2.content_set.side_effect = lambda arch: f"repo2-content-set-{arch}"

        metadata.runtime.repos = {"repo1": mock_repo1, "repo2": mock_repo2}

        result = builder._prefetch(metadata=metadata, group="openshift-4.17")

        expected_rpm_data = {
            "type": "rpm",
            "path": ".",
            "options": {
                "dnf": {
                    "repo1-content-set-x86_64": {"gpgcheck": "0"},
                    "repo1-content-set-aarch64": {"gpgcheck": "0"},
                    "repo2-content-set-x86_64": {"gpgcheck": "0"},
                    "repo2-content-set-aarch64": {"gpgcheck": "0"},
                }
            },
        }
        expected = [expected_rpm_data, {"type": "gomod", "path": "."}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_rpm_lockfile_non_prerelease_no_dnf_options(self, mock_konflux_client_init):
        """Test that non-prerelease phase does NOT add DNF options"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        metadata.runtime.group_config.software_lifecycle.phase = "release"
        metadata.get_enabled_repos.return_value = {"repo1", "repo2"}
        metadata.get_arches.return_value = ["x86_64"]

        result = builder._prefetch(metadata=metadata, group="openshift-4.17")

        expected = [{"type": "rpm", "path": "."}, {"type": "gomod", "path": "."}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_prerelease_no_rpm_lockfile_no_dnf_options(self, mock_konflux_client_init):
        """Test that prerelease phase with no RPM lockfile does NOT add DNF options"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}]}

        result = builder._prefetch(metadata=metadata, group="test-group")

        expected = [{"type": "gomod", "path": "."}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_prerelease_empty_repos_no_dnf_options(self, mock_konflux_client_init):
        """Test that prerelease phase with empty repositories does NOT add DNF options"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        metadata.runtime.group_config.software_lifecycle.phase = "pre-release"
        metadata.get_enabled_repos.return_value = set()
        metadata.get_arches.return_value = ["x86_64"]

        result = builder._prefetch(metadata=metadata, group="openshift-4.17")

        expected = [{"type": "rpm", "path": "."}, {"type": "gomod", "path": "."}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_prerelease_content_set_fallback(self, mock_konflux_client_init):
        """Test fallback logic when content_set returns None"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        metadata.runtime.group_config.software_lifecycle.phase = "pre-release"
        metadata.get_enabled_repos.return_value = {"repo1"}
        metadata.get_arches.return_value = ["x86_64"]

        mock_repo1 = MagicMock()
        mock_repo1.content_set.return_value = None

        metadata.runtime.repos = {"repo1": mock_repo1}

        result = builder._prefetch(metadata=metadata, group="openshift-4.17")

        expected_rpm_data = {
            "type": "rpm",
            "path": ".",
            "options": {"dnf": {"repo1-x86_64": {"gpgcheck": "0"}}},
        }
        expected = [expected_rpm_data, {"type": "gomod", "path": "."}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_prerelease_all_repos_included(self, mock_konflux_client_init):
        """Test that prerelease phase includes ALL repositories regardless of URL"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        metadata.runtime.group_config.software_lifecycle.phase = "pre-release"
        metadata.get_enabled_repos.return_value = {"ocp-repo", "rhel-repo"}
        metadata.get_arches.return_value = ["x86_64"]

        mock_ocp_repo = MagicMock()
        mock_ocp_repo.content_set.side_effect = lambda arch: f"ocp-repo-content-set-{arch}"

        mock_rhel_repo = MagicMock()
        mock_rhel_repo.content_set.side_effect = lambda arch: f"rhel-repo-content-set-{arch}"

        metadata.runtime.repos = {"ocp-repo": mock_ocp_repo, "rhel-repo": mock_rhel_repo}

        result = builder._prefetch(metadata=metadata, group="openshift-4.17")

        expected_rpm_data = {
            "type": "rpm",
            "path": ".",
            "options": {
                "dnf": {
                    "ocp-repo-content-set-x86_64": {"gpgcheck": "0"},
                    "rhel-repo-content-set-x86_64": {"gpgcheck": "0"},
                }
            },
        }
        expected = [expected_rpm_data, {"type": "gomod", "path": "."}]
        self.assertEqual(result, expected)
