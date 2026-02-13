import unittest
from unittest.mock import MagicMock, patch

from artcommonlib.config.plashet import PlashetConfig
from artcommonlib.config.repo import BrewSource, BrewTag, PlashetRepo, Repo
from pyartcd.pipelines.scan_plashet_rpms import ScanPlashetRpmsPipeline


class TestScanPlashetRpmsPipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(dry_run=False)
        self.runtime.logger = MagicMock()

    def _create_test_repo(self, name, slug, tag_name="rhaos-4.17-rhel-9-candidate", arches=None):
        """Helper to create a test plashet repo"""
        return Repo(
            name=name,
            type="plashet",
            plashet=PlashetRepo(
                slug=slug,
                arches=arches,
                source=BrewSource(
                    type="brew",
                    from_tags=[
                        BrewTag(
                            name=tag_name,
                            product_version="OSE-4.17-RHEL-9",
                        )
                    ],
                ),
            ),
        )

    @patch("pyartcd.pipelines.scan_plashet_rpms.util.load_group_config")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.init_jenkins")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.start_build_plashets")
    async def test_no_plashet_repos(self, mock_start_build, mock_init_jenkins, mock_load_config):
        """Test when there are no plashet repos configured"""
        mock_load_config.return_value = {
            "all_repos": [],
            "plashet": {},
            "arches": ["x86_64"],
        }

        pipeline = ScanPlashetRpmsPipeline(
            runtime=self.runtime,
            group="openshift-4.17",
        )

        await pipeline.run()

        mock_start_build.assert_not_called()
        self.assertEqual(len(pipeline.repos_to_rebuild), 0)

    @patch("pyartcd.pipelines.scan_plashet_rpms.util.load_group_config")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.init_jenkins")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.start_build_plashets")
    @patch("pyartcd.pipelines.scan_plashet_rpms.koji.ClientSession")
    async def test_first_build_triggers_rebuild(
        self, mock_koji_session, mock_start_build, mock_init_jenkins, mock_load_config
    ):
        """Test that missing plashet.yml (404) triggers a rebuild"""
        test_repo = self._create_test_repo("test-repo", "el9")

        mock_load_config.return_value = {
            "all_repos": [test_repo.model_dump()],
            "plashet": {
                "base_dir": "rhocp-rhel-4.17/$runtime_assembly/$slug",
                "plashet_dir": "$yyyy-$MM/$revision",
                "create_symlinks": True,
                "symlink_name": "latest",
                "base_url": "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets",
            },
            "arches": ["x86_64"],
        }

        pipeline = ScanPlashetRpmsPipeline(
            runtime=self.runtime,
            group="openshift-4.17",
        )

        # Mock HTTP 404 response
        with patch("httpx.AsyncClient.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response

            # Mock koji client
            mock_koji = MagicMock()
            mock_koji.gssapi_login = MagicMock()
            mock_koji_session.return_value = mock_koji

            await pipeline.run()

        self.assertIn("test-repo", pipeline.repos_to_rebuild)
        self.assertIn("first build", pipeline.rebuild_reasons["test-repo"])
        mock_start_build.assert_called_once()

    @patch("pyartcd.pipelines.scan_plashet_rpms.util.load_group_config")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.init_jenkins")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.start_build_plashets")
    @patch("pyartcd.pipelines.scan_plashet_rpms.koji.ClientSession")
    async def test_arches_changed_triggers_rebuild(
        self, mock_koji_session, mock_start_build, mock_init_jenkins, mock_load_config
    ):
        """Test that changed arches configuration triggers a rebuild"""
        test_repo = self._create_test_repo("test-repo", "el9")

        mock_load_config.return_value = {
            "all_repos": [test_repo.model_dump()],
            "plashet": {
                "base_dir": "rhocp-rhel-4.17/$runtime_assembly/$slug",
                "plashet_dir": "$yyyy-$MM/$revision",
                "create_symlinks": True,
                "symlink_name": "latest",
                "base_url": "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets",
            },
            "arches": ["x86_64", "s390x"],  # Current config has 2 arches
        }

        pipeline = ScanPlashetRpmsPipeline(
            runtime=self.runtime,
            group="openshift-4.17",
        )

        # Mock plashet.yml with different arches
        plashet_yml_data = {
            "assemble": {
                "arches": ["x86_64"],  # Only 1 arch in existing plashet
                "brew_event": {"id": 12345},
            }
        }

        with patch("httpx.AsyncClient.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = "assemble:\n  arches:\n    - x86_64\n  brew_event:\n    id: 12345"
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            # Mock yaml.safe_load
            with patch("yaml.safe_load", return_value=plashet_yml_data):
                # Mock koji client
                mock_koji = MagicMock()
                mock_koji.gssapi_login = MagicMock()
                mock_koji_session.return_value = mock_koji

                await pipeline.run()

        self.assertIn("test-repo", pipeline.repos_to_rebuild)
        self.assertIn("arches changed", pipeline.rebuild_reasons["test-repo"])
        mock_start_build.assert_called_once()

    @patch("pyartcd.pipelines.scan_plashet_rpms.util.load_group_config")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.init_jenkins")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.start_build_plashets")
    @patch("pyartcd.pipelines.scan_plashet_rpms.koji.ClientSession")
    async def test_tag_changed_triggers_rebuild(
        self, mock_koji_session, mock_start_build, mock_init_jenkins, mock_load_config
    ):
        """Test that new tag events trigger a rebuild"""
        test_repo = self._create_test_repo("test-repo", "el9")

        mock_load_config.return_value = {
            "all_repos": [test_repo.model_dump()],
            "plashet": {
                "base_dir": "rhocp-rhel-4.17/$runtime_assembly/$slug",
                "plashet_dir": "$yyyy-$MM/$revision",
                "create_symlinks": True,
                "symlink_name": "latest",
                "base_url": "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets",
            },
            "arches": ["x86_64"],
        }

        pipeline = ScanPlashetRpmsPipeline(
            runtime=self.runtime,
            group="openshift-4.17",
        )

        # Mock plashet.yml with old event ID
        plashet_yml_data = {
            "assemble": {
                "arches": ["x86_64"],
                "brew_event": {"id": 10000},  # Old event
            }
        }

        # Mock koji multicall returning newer event
        mock_koji = MagicMock()
        mock_koji.gssapi_login = MagicMock()

        # Mock multicall context manager
        mock_multicall_ctx = MagicMock()
        mock_task = MagicMock()
        mock_task.result = {
            "tag_listing": [
                {"create_event": 10500},  # Newer event
            ]
        }
        mock_multicall_ctx.__enter__.return_value.queryHistory.return_value = mock_task
        mock_multicall_ctx.__exit__.return_value = None
        mock_koji.multicall.return_value = mock_multicall_ctx

        mock_koji_session.return_value = mock_koji

        with patch("httpx.AsyncClient.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = "assemble:\n  arches:\n    - x86_64\n  brew_event:\n    id: 10000"
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            with patch("yaml.safe_load", return_value=plashet_yml_data):
                await pipeline.run()

        self.assertIn("test-repo", pipeline.repos_to_rebuild)
        self.assertIn("tags changed", pipeline.rebuild_reasons["test-repo"])
        mock_start_build.assert_called_once()

    @patch("pyartcd.pipelines.scan_plashet_rpms.util.load_group_config")
    @patch("pyartcd.pipelines.scan_plashet_rpms.koji.ClientSession")
    async def test_no_changes_detected(self, mock_koji_session, mock_load_config):
        """Test that no rebuild is triggered when there are no changes"""
        test_repo = self._create_test_repo("test-repo", "el9")

        mock_load_config.return_value = {
            "all_repos": [test_repo.model_dump()],
            "plashet": {
                "base_dir": "rhocp-rhel-4.17/$runtime_assembly/$slug",
                "plashet_dir": "$yyyy-$MM/$revision",
                "create_symlinks": True,
                "symlink_name": "latest",
                "base_url": "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets",
            },
            "arches": ["x86_64"],
        }

        pipeline = ScanPlashetRpmsPipeline(
            runtime=self.runtime,
            group="openshift-4.17",
        )

        # Mock plashet.yml
        plashet_yml_data = {
            "assemble": {
                "arches": ["x86_64"],
                "brew_event": {"id": 10000},
            }
        }

        # Mock koji multicall returning no changes (empty tag_listing)
        mock_koji = MagicMock()
        mock_koji.gssapi_login = MagicMock()

        # Mock multicall context manager - no events after plashet_event_id
        mock_multicall_ctx = MagicMock()
        mock_task = MagicMock()
        mock_task.result = {
            "tag_listing": []  # No events after plashet_event_id
        }
        mock_multicall_ctx.__enter__.return_value.queryHistory.return_value = mock_task
        mock_multicall_ctx.__exit__.return_value = None
        mock_koji.multicall.return_value = mock_multicall_ctx

        mock_koji_session.return_value = mock_koji

        with patch("httpx.AsyncClient.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = ""
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            with patch("yaml.safe_load", return_value=plashet_yml_data):
                await pipeline.run()

        self.assertEqual(len(pipeline.repos_to_rebuild), 0)

    @patch("pyartcd.pipelines.scan_plashet_rpms.util.load_group_config")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.init_jenkins")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.start_build_plashets")
    @patch("pyartcd.pipelines.scan_plashet_rpms.koji.ClientSession")
    async def test_no_arches_field_continues_to_tag_check(
        self, mock_koji_session, mock_start_build, mock_init_jenkins, mock_load_config
    ):
        """Test that missing arches field doesn't skip tag check"""
        test_repo = self._create_test_repo("test-repo", "el9")

        mock_load_config.return_value = {
            "all_repos": [test_repo.model_dump()],
            "plashet": {
                "base_dir": "rhocp-rhel-4.17/$runtime_assembly/$slug",
                "plashet_dir": "$yyyy-$MM/$revision",
                "create_symlinks": True,
                "symlink_name": "latest",
                "base_url": "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets",
            },
            "arches": ["x86_64"],
        }

        pipeline = ScanPlashetRpmsPipeline(
            runtime=self.runtime,
            group="openshift-4.17",
        )

        # Mock plashet.yml WITHOUT arches field
        plashet_yml_data = {
            "assemble": {
                # No arches field
                "brew_event": {"id": 10000},
            }
        }

        # Mock koji multicall returning newer event
        mock_koji = MagicMock()
        mock_koji.gssapi_login = MagicMock()

        # Mock multicall context manager
        mock_multicall_ctx = MagicMock()
        mock_task = MagicMock()
        mock_task.result = {
            "tag_listing": [
                {"create_event": 10500},  # Newer event
            ]
        }
        mock_multicall_ctx.__enter__.return_value.queryHistory.return_value = mock_task
        mock_multicall_ctx.__exit__.return_value = None
        mock_koji.multicall.return_value = mock_multicall_ctx

        mock_koji_session.return_value = mock_koji

        with patch("httpx.AsyncClient.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = ""
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            with patch("yaml.safe_load", return_value=plashet_yml_data):
                await pipeline.run()

        # Should still trigger rebuild based on tag change
        self.assertIn("test-repo", pipeline.repos_to_rebuild)
        self.assertIn("tags changed", pipeline.rebuild_reasons["test-repo"])
        mock_start_build.assert_called_once()

    @patch("pyartcd.pipelines.scan_plashet_rpms.util.load_group_config")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.init_jenkins")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.start_build_plashets")
    @patch("pyartcd.pipelines.scan_plashet_rpms.koji.ClientSession")
    async def test_dry_run_doesnt_trigger_build(
        self, mock_koji_session, mock_start_build, mock_init_jenkins, mock_load_config
    ):
        """Test that dry run mode passes dry_run=True to jenkins"""
        test_repo = self._create_test_repo("test-repo", "el9")

        mock_load_config.return_value = {
            "all_repos": [test_repo.model_dump()],
            "plashet": {
                "base_dir": "rhocp-rhel-4.17/$runtime_assembly/$slug",
                "plashet_dir": "$yyyy-$MM/$revision",
                "create_symlinks": True,
                "symlink_name": "latest",
                "base_url": "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets",
            },
            "arches": ["x86_64"],
        }

        runtime = MagicMock(dry_run=True)
        runtime.logger = MagicMock()

        pipeline = ScanPlashetRpmsPipeline(
            runtime=runtime,
            group="openshift-4.17",
        )

        # Mock koji client
        mock_koji = MagicMock()
        mock_koji.gssapi_login = MagicMock()
        mock_koji_session.return_value = mock_koji

        # Mock HTTP 404 to trigger rebuild detection
        with patch("httpx.AsyncClient.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response

            await pipeline.run()

        # Should detect changes and call jenkins with dry_run=True
        self.assertIn("test-repo", pipeline.repos_to_rebuild)
        mock_start_build.assert_called_once()
        # Verify dry_run=True was passed
        call_kwargs = mock_start_build.call_args.kwargs
        self.assertEqual(call_kwargs["dry_run"], True)

    def test_arches_fallback_logic(self):
        """Test the arches fallback chain: repo -> plashet config -> group"""
        pipeline = ScanPlashetRpmsPipeline(
            runtime=self.runtime,
            group="openshift-4.17",
        )

        plashet_config = PlashetConfig(
            base_dir="",
            plashet_dir="",
            download_url="",
            arches=["x86_64", "s390x"],
        )

        # Test: repo has arches - use repo arches
        repo_with_arches = self._create_test_repo("test", "el9", arches=["x86_64"])
        arches = pipeline._get_configured_arches(repo_with_arches, plashet_config, ["aarch64"])
        self.assertEqual(arches, ["x86_64"])

        # Test: repo has no arches - use plashet config arches
        repo_no_arches = self._create_test_repo("test", "el9", arches=None)
        arches = pipeline._get_configured_arches(repo_no_arches, plashet_config, ["aarch64"])
        self.assertEqual(arches, ["x86_64", "s390x"])

        # Test: neither repo nor plashet config have arches - use group arches
        plashet_config_no_arches = PlashetConfig(
            base_dir="",
            plashet_dir="",
            download_url="",
            arches=None,
        )
        arches = pipeline._get_configured_arches(repo_no_arches, plashet_config_no_arches, ["aarch64", "ppc64le"])
        self.assertEqual(arches, ["aarch64", "ppc64le"])

    def test_repo_construct_download_url_plashet(self):
        """Test Repo.construct_download_url() for plashet repos"""
        # Test plashet repo with symlink
        test_repo = self._create_test_repo("test-repo", "el9")
        plashet_config = PlashetConfig(
            base_dir="rhocp-rhel-4.17/$runtime_assembly/$slug",
            plashet_dir="$yyyy-$MM/$revision",
            symlink_name="latest",
        )

        url = test_repo.construct_download_url(
            arch="x86_64",
            plashet_config=plashet_config,
            replace_vars={"runtime_assembly": "stream"},
        )

        expected = (
            "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/rhocp-rhel-4.17/stream/el9/latest/x86_64/"
        )
        self.assertEqual(url, expected)

        # Test with custom plashet_dir
        url_with_dir = test_repo.construct_download_url(
            arch="x86_64",
            plashet_config=plashet_config,
            replace_vars={"runtime_assembly": "stream"},
            plashet_dir="test-repo",
        )

        expected_with_dir = "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/rhocp-rhel-4.17/stream/el9/test-repo/x86_64/"
        self.assertEqual(url_with_dir, expected_with_dir)

    def test_repo_construct_download_url_external(self):
        """Test Repo.construct_download_url() for external repos"""
        external_repo = Repo(
            name="rhel-9-baseos",
            type="external",
            conf={
                "baseurl": {
                    "x86_64": "https://cdn.redhat.com/content/dist/rhel9/9/x86_64/baseos/os/",
                    "s390x": "https://cdn.redhat.com/content/dist/rhel9/9/s390x/baseos/os/",
                    "default": "https://cdn.redhat.com/content/dist/rhel9/9/default/baseos/os/",
                }
            },
        )

        # Test with specific arch
        url_x86 = external_repo.construct_download_url("x86_64")
        self.assertEqual(url_x86, "https://cdn.redhat.com/content/dist/rhel9/9/x86_64/baseos/os/")

        url_s390 = external_repo.construct_download_url("s390x")
        self.assertEqual(url_s390, "https://cdn.redhat.com/content/dist/rhel9/9/s390x/baseos/os/")

        # Test with default fallback when arch not found
        url_default = external_repo.construct_download_url("ppc64le")
        self.assertEqual(url_default, "https://cdn.redhat.com/content/dist/rhel9/9/default/baseos/os/")

    def test_repo_construct_download_url_deprecated(self):
        """Test that deprecated download_url field still works with warning"""
        import warnings

        test_repo = self._create_test_repo("test-repo", "el9")
        plashet_config = PlashetConfig(
            base_dir="rhocp-rhel-4.17/$runtime_assembly/$slug",
            plashet_dir="$yyyy-$MM/$revision",
            symlink_name="latest",
            download_url="https://example.com/$runtime_assembly/$slug/$arch/os/",
        )

        # Test that using download_url triggers deprecation warning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            url = test_repo.construct_download_url(
                arch="x86_64",
                plashet_config=plashet_config,
                replace_vars={"runtime_assembly": "stream"},
            )

            # Verify our specific deprecation warning was issued
            deprecation_warnings = [warning for warning in w if issubclass(warning.category, DeprecationWarning)]
            self.assertGreater(len(deprecation_warnings), 0, "Expected at least one DeprecationWarning")

            # Find our specific warning about download_url
            our_warning = next(
                (warning for warning in deprecation_warnings if "download_url is deprecated" in str(warning.message)),
                None,
            )
            self.assertIsNotNone(our_warning, "Expected deprecation warning about download_url")

            # Verify URL is constructed from deprecated download_url template
            expected = "https://example.com/stream/el9/x86_64/os/"
            self.assertEqual(url, expected)

    @patch("pyartcd.pipelines.scan_plashet_rpms.util.load_group_config")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.init_jenkins")
    @patch("pyartcd.pipelines.scan_plashet_rpms.jenkins.start_build_plashets")
    @patch("pyartcd.pipelines.scan_plashet_rpms.koji.ClientSession")
    async def test_repos_filter(self, mock_koji_session, mock_start_build, mock_init_jenkins, mock_load_config):
        """Test that repos parameter filters which repos are checked"""
        repo1 = self._create_test_repo("el9", "el9")
        repo2 = self._create_test_repo("el8", "el8")
        repo3 = self._create_test_repo("rhel-rhcos", "rhel-rhcos")

        mock_load_config.return_value = {
            "all_repos": [repo1.model_dump(), repo2.model_dump(), repo3.model_dump()],
            "plashet": {
                "base_dir": "rhocp-rhel-4.17/$runtime_assembly/$slug",
                "plashet_dir": "$yyyy-$MM/$revision",
                "create_symlinks": True,
                "symlink_name": "latest",
                "base_url": "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets",
            },
            "arches": ["x86_64"],
        }

        # Test with repos filter
        pipeline = ScanPlashetRpmsPipeline(
            runtime=self.runtime,
            group="openshift-4.17",
            repos=["el9", "rhel-rhcos"],  # Only check these two repos
        )

        # Mock koji client
        mock_koji = MagicMock()
        mock_koji.gssapi_login = MagicMock()
        mock_koji_session.return_value = mock_koji

        # Mock HTTP 404 to trigger rebuild for all repos
        with patch("httpx.AsyncClient.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response

            await pipeline.run()

        # Should only rebuild filtered repos (el9 and rhel-rhcos), not el8
        self.assertEqual(len(pipeline.repos_to_rebuild), 2)
        self.assertIn("el9", pipeline.repos_to_rebuild)
        self.assertIn("rhel-rhcos", pipeline.repos_to_rebuild)
        self.assertNotIn("el8", pipeline.repos_to_rebuild)


if __name__ == "__main__":
    unittest.main()
