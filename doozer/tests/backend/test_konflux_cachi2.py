from unittest import TestCase
from unittest.mock import MagicMock, patch

from artcommonlib.model import Missing
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder


class TestKonfluxCachi2(TestCase):
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_1(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = False
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False

        self.assertEqual(builder._prefetch(metadata=metadata), [])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_2(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = False
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.config.content.source.pkg_managers = ["unknown"]

        self.assertEqual(builder._prefetch(metadata=metadata), [])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_3(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.config.content.source.pkg_managers = ["gomod"]

        self.assertEqual(builder._prefetch(metadata=metadata), [{"type": "gomod", "path": "."}])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_4(self, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = False
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {'gomod': [{'path': 'api'}]}

        self.assertEqual(builder._prefetch(metadata=metadata), [{"type": "gomod", "path": "api"}])

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
            builder._prefetch(metadata=metadata),
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
        metadata.config.cachito.packages = {'npm': [{'path': 'web'}], 'gomod': [{'path': '.'}]}

        self.assertEqual(
            builder._prefetch(metadata=metadata), [{'type': 'gomod', 'path': '.'}, {'type': 'npm', 'path': 'web'}]
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
        metadata.config.cachito.packages = {'gomod': [{'path': '.'}]}

        result = builder._prefetch(metadata=metadata)
        expected = [{'type': 'gomod', 'path': '.'}]
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
        metadata.config.cachito.packages = {'gomod': [{'path': '.'}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        result = builder._prefetch(metadata=metadata)
        expected = [{'type': 'rpm', 'path': '.'}, {'type': 'gomod', 'path': '.'}]
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
        metadata.config.cachito.packages = {'npm': [{'path': 'frontend'}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "custom/path"

        result = builder._prefetch(metadata=metadata)
        expected = [{'type': 'rpm', 'path': 'custom/path'}, {'type': 'npm', 'path': 'frontend'}]
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
        metadata.config.cachito.packages = {'gomod': [{'path': '.'}]}
        metadata.config.konflux.cachi2.artifact_lockfile.get.return_value = "."

        result = builder._prefetch(metadata=metadata)
        expected = [{'type': 'generic', 'path': '.'}, {'type': 'gomod', 'path': '.'}]
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
        metadata.config.cachito.packages = {'gomod': [{'path': '.'}]}

        result = builder._prefetch(metadata=metadata)
        expected = [{'type': 'gomod', 'path': '.'}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_rpm_lockfile_prerelease_dnf_options(self, mock_konflux_client_init):
        """Test that prerelease phase adds DNF options with gpgcheck=0 for all repository IDs"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        # Setup metadata for prerelease RPM lockfile scenario
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {'gomod': [{'path': '.'}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        # Setup prerelease phase
        metadata.runtime.group_config.software_lifecycle.phase = 'pre-release'
        metadata.get_enabled_repos.return_value = {'repo1', 'repo2'}
        metadata.get_arches.return_value = ['x86_64', 'aarch64']

        # Mock repository objects with plashets URLs
        mock_repo1 = MagicMock()
        mock_repo1.content_set.side_effect = lambda arch: f'repo1-content-set-{arch}'
        mock_repo1.baseurl.side_effect = (
            lambda repotype,
            arch: f'https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/4.21/test/el9/latest/{arch}/os/'
        )
        mock_repo2 = MagicMock()
        mock_repo2.content_set.side_effect = lambda arch: f'repo2-content-set-{arch}'
        mock_repo2.baseurl.side_effect = (
            lambda repotype,
            arch: f'https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/4.21/test/el9-embargoed/latest/{arch}/os/'
        )

        metadata.runtime.repos = {'repo1': mock_repo1, 'repo2': mock_repo2}

        result = builder._prefetch(metadata=metadata)

        expected_rpm_data = {
            'type': 'rpm',
            'path': '.',
            'options': {
                'dnf': {
                    'repo1-content-set-x86_64': {'gpgcheck': '0'},
                    'repo1-content-set-aarch64': {'gpgcheck': '0'},
                    'repo2-content-set-x86_64': {'gpgcheck': '0'},
                    'repo2-content-set-aarch64': {'gpgcheck': '0'},
                }
            },
        }
        expected = [expected_rpm_data, {'type': 'gomod', 'path': '.'}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_rpm_lockfile_non_prerelease_no_dnf_options(self, mock_konflux_client_init):
        """Test that non-prerelease phase does NOT add DNF options"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        # Setup metadata for non-prerelease RPM lockfile scenario
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {'gomod': [{'path': '.'}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        # Setup non-prerelease phase
        metadata.runtime.group_config.software_lifecycle.phase = 'production'
        metadata.get_enabled_repos.return_value = {'repo1', 'repo2'}
        metadata.get_arches.return_value = ['x86_64']

        result = builder._prefetch(metadata=metadata)

        # Should NOT have DNF options
        expected = [{'type': 'rpm', 'path': '.'}, {'type': 'gomod', 'path': '.'}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_prerelease_no_rpm_lockfile_no_dnf_options(self, mock_konflux_client_init):
        """Test that prerelease phase with no RPM lockfile does NOT add DNF options"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        # Setup metadata for prerelease without RPM lockfile
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = False  # No RPM lockfile
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {'gomod': [{'path': '.'}]}

        # Setup prerelease phase
        metadata.runtime.group_config.software_lifecycle.phase = 'pre-release'

        result = builder._prefetch(metadata=metadata)

        # Should NOT have RPM data at all
        expected = [{'type': 'gomod', 'path': '.'}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_prerelease_empty_repos_no_dnf_options(self, mock_konflux_client_init):
        """Test that prerelease phase with empty repositories does NOT add DNF options"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        # Setup metadata for prerelease RPM lockfile scenario with empty repos
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {'gomod': [{'path': '.'}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        # Setup prerelease phase with empty repos
        metadata.runtime.group_config.software_lifecycle.phase = 'pre-release'
        metadata.get_enabled_repos.return_value = set()  # Empty repositories
        metadata.get_arches.return_value = ['x86_64']

        result = builder._prefetch(metadata=metadata)

        # Should NOT have DNF options due to empty repos
        expected = [{'type': 'rpm', 'path': '.'}, {'type': 'gomod', 'path': '.'}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_prerelease_content_set_fallback(self, mock_konflux_client_init):
        """Test fallback logic when content_set returns None"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        # Setup metadata for prerelease RPM lockfile scenario
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {'gomod': [{'path': '.'}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        # Setup prerelease phase
        metadata.runtime.group_config.software_lifecycle.phase = 'pre-release'
        metadata.get_enabled_repos.return_value = {'repo1'}
        metadata.get_arches.return_value = ['x86_64']

        # Mock repository object that returns None for content_set
        mock_repo1 = MagicMock()
        mock_repo1.content_set.return_value = None  # Trigger fallback
        mock_repo1.baseurl.side_effect = (
            lambda repotype,
            arch: f'https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/4.21/test/el9/latest/{arch}/os/'
        )

        metadata.runtime.repos = {'repo1': mock_repo1}

        result = builder._prefetch(metadata=metadata)

        expected_rpm_data = {
            'type': 'rpm',
            'path': '.',
            'options': {
                'dnf': {
                    'repo1-x86_64': {'gpgcheck': '0'}  # Fallback format
                }
            },
        }
        expected = [expected_rpm_data, {'type': 'gomod', 'path': '.'}]
        self.assertEqual(result, expected)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_prerelease_non_plashets_repos_excluded(self, mock_konflux_client_init):
        """Test that prerelease phase excludes repositories without '/plashets/' in baseurl"""
        builder = KonfluxImageBuilder(MagicMock())
        metadata = MagicMock()

        # Setup metadata for prerelease RPM lockfile scenario
        metadata.is_cachi2_enabled.return_value = True
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {'gomod': [{'path': '.'}]}
        metadata.config.konflux.cachi2.lockfile.get.return_value = "."

        # Setup prerelease phase
        metadata.runtime.group_config.software_lifecycle.phase = 'pre-release'
        metadata.get_enabled_repos.return_value = {'ocp-repo', 'rhel-repo'}
        metadata.get_arches.return_value = ['x86_64']

        # Mock repository objects: one with plashets, one without
        mock_ocp_repo = MagicMock()
        mock_ocp_repo.content_set.side_effect = lambda arch: f'ocp-repo-content-set-{arch}'
        mock_ocp_repo.baseurl.side_effect = (
            lambda repotype,
            arch: f'https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/4.21/test/el9/latest/{arch}/os/'
        )

        mock_rhel_repo = MagicMock()
        mock_rhel_repo.content_set.side_effect = lambda arch: f'rhel-repo-content-set-{arch}'
        mock_rhel_repo.baseurl.side_effect = (
            lambda repotype, arch: f'https://cdn.redhat.com/content/dist/rhel9/{arch}/appstream/os/'
        )

        metadata.runtime.repos = {'ocp-repo': mock_ocp_repo, 'rhel-repo': mock_rhel_repo}

        result = builder._prefetch(metadata=metadata)

        # Should only have DNF options for OCP repo with plashets, not RHEL repo
        expected_rpm_data = {
            'type': 'rpm',
            'path': '.',
            'options': {
                'dnf': {
                    'ocp-repo-content-set-x86_64': {'gpgcheck': '0'},
                    # rhel-repo should NOT be present
                }
            },
        }
        expected = [expected_rpm_data, {'type': 'gomod', 'path': '.'}]
        self.assertEqual(result, expected)
