import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

from artcommonlib.model import Missing
from dockerfile_parse import DockerfileParser
from doozerlib.backend.rebaser import KonfluxRebaser


class TestRebaser(TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def test_split_dockerfile_into_stages_1(self):
        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
            FROM base
            LABEL foo="bar baz"
            USER 1000
            FROM base2
            USER 2000
            RUN commands
        """

        actual = KonfluxRebaser.split_dockerfile_into_stages(dfp)
        expected = [
            [{'FROM': 'base'}, {'LABEL': 'foo="bar baz"'}, {'USER': '1000'}],
            [{'FROM': 'base2'}, {'USER': '2000'}, {'RUN': 'commands'}],
        ]

        self.assertEqual(len(actual), 2)
        self.assertEqual(actual, expected)

    def test_split_dockerfile_into_stages_2(self):
        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
            FROM base
            LABEL foo="bar baz"
            USER 1000
        """

        actual = KonfluxRebaser.split_dockerfile_into_stages(dfp)
        expected = [[{'FROM': 'base'}, {'LABEL': 'foo="bar baz"'}, {'USER': '1000'}]]

        self.assertEqual(len(actual), 1)
        self.assertEqual(actual, expected)

    def test_add_build_repos_1(self):
        """
        Test with defalut values, and has non-USER 0 lines
        """
        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.config.konflux.cachito.mode = Missing
        metadata.config.final_stage_user = Missing

        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
FROM base1
LABEL foo="bar baz"
USER 1000
FROM base2
USER 2000
RUN commands
               """
        expected = """
FROM base1

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
LABEL foo="bar baz"
USER 1000
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
USER 2000
RUN commands


# Start Konflux-specific steps
USER 0
RUN rm -f /etc/yum.repos.d/* && cp /tmp/art/yum_temp/* /etc/yum.repos.d/ || true
RUN rm -rf /tmp/art
USER 2000
# End Konflux-specific steps
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path("."))

        self.assertEqual(dfp.content, expected)

    def test_add_build_repos_2(self):
        """
        Test with default values, but with final_stage_user set
        """
        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.config.konflux.cachito.mode = Missing
        metadata.config.final_stage_user = "3000"

        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
FROM base1
LABEL foo="bar baz"
USER 1000
FROM base2
USER 2000
RUN commands
               """
        expected = """
FROM base1

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
LABEL foo="bar baz"
USER 1000
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
USER 2000
RUN commands


# Start Konflux-specific steps
USER 0
RUN rm -f /etc/yum.repos.d/* && cp /tmp/art/yum_temp/* /etc/yum.repos.d/ || true
RUN rm -rf /tmp/art
USER 3000
# End Konflux-specific steps
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path("."))

        self.assertEqual(dfp.content, expected)

    def test_add_build_repos_3(self):
        """
        Test with network_mode hermetic
        """
        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.config.konflux.cachito.mode = Missing
        metadata.config.final_stage_user = "3000"

        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
FROM base1
LABEL foo="bar baz"
USER 1000
FROM base2
USER 2000
RUN commands
               """
        expected = """
FROM base1

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=hermetic
ENV ART_BUILD_DEPS_MODE=default
# End Konflux-specific steps
LABEL foo="bar baz"
USER 1000
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=hermetic
ENV ART_BUILD_DEPS_MODE=default
# End Konflux-specific steps
USER 2000
RUN commands

USER 3000
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path(self.directory.name))
        dfp.content.strip()
        self.assertEqual(expected.strip(), dfp.content.strip())

    def test_add_build_repos_4(self):
        """
        Test with non-hermetic, but with final_stage_user
        """
        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.config.konflux.cachito.mode = Missing
        metadata.config.final_stage_user = "3000"

        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
FROM base1
LABEL foo="bar baz"
FROM base2
RUN commands
               """
        expected = """
FROM base1

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
LABEL foo="bar baz"
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
RUN commands


# Start Konflux-specific steps
USER 0
RUN rm -f /etc/yum.repos.d/* && cp /tmp/art/yum_temp/* /etc/yum.repos.d/ || true
RUN rm -rf /tmp/art
USER 3000
# End Konflux-specific steps
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path(self.directory.name))

        self.assertEqual(dfp.content.strip(), expected.strip())

    @patch("doozerlib.backend.rebaser.RPMLockfileGenerator")
    def test_write_rpms_lock_file_no_parents(self, mock_rpmlockfile_cls):
        mock_rpmlockfile = AsyncMock()
        mock_rpmlockfile_cls.return_value = mock_rpmlockfile

        metadata = MagicMock()
        metadata.distgit_key = "foo"
        metadata.get_arches.return_value = ["x86_64"]
        metadata.config.konflux.cachi2.lockfile.packages.get.side_effect = lambda k, default=None: []
        metadata.get_parent_members.return_value = {}
        metadata.config.get.side_effect = lambda k, default=None: []
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_lockfile_force_enabled.return_value = False

        runtime = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=MagicMock(installed_packages=["pkg1", "pkg2"]))

        rebaser = KonfluxRebaser(runtime, MagicMock(), MagicMock(), "unsigned")
        asyncio.run(rebaser._write_rpms_lock_file(metadata, "test-group", Path(".")))

        mock_rpmlockfile.generate_lockfile.assert_awaited_with(
            ["x86_64"], set(), {"pkg1", "pkg2"}, Path("."), distgit_key="foo", force=False
        )

    @patch("doozerlib.backend.rebaser.RPMLockfileGenerator")
    def test_write_rpms_lock_file_with_parents(self, mock_rpmlockfile_cls):
        mock_rpmlockfile = AsyncMock()
        mock_rpmlockfile_cls.return_value = mock_rpmlockfile

        metadata = MagicMock()
        metadata.distgit_key = "foo"
        metadata.get_arches.return_value = ["x86_64"]
        metadata.config.konflux.cachi2.lockfile.packages.get.side_effect = lambda k, default=None: []
        metadata.get_parent_members.return_value = {"bar": None}
        metadata.config.get.side_effect = lambda k, default=None: []
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_lockfile_force_enabled.return_value = False

        async def get_latest_build(name, group):
            if name == "foo":
                return MagicMock(installed_packages=["pkg1", "pkg2"])
            elif name == "bar":
                return MagicMock(installed_packages=["pkg2"])
            return None

        runtime = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(side_effect=get_latest_build)

        rebaser = KonfluxRebaser(runtime, MagicMock(), MagicMock(), "unsigned")
        asyncio.run(rebaser._write_rpms_lock_file(metadata, "test-group", Path(".")))

        mock_rpmlockfile.generate_lockfile.assert_awaited_with(
            ["x86_64"], set(), {"pkg1"}, Path("."), distgit_key="foo", force=False
        )

    def test_write_rpms_lock_file_disabled(self):
        metadata = MagicMock()
        metadata.image_name = "foo"
        metadata.is_lockfile_generation_enabled.return_value = False

        logger = MagicMock()

        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._logger = logger
        rebaser._rpm_lockfile_generator = MagicMock()

        asyncio.run(rebaser._write_rpms_lock_file(metadata, "test-group", Path(".")))

        # Assert that generate_lockfile was not called
        rebaser._rpm_lockfile_generator.generate_lockfile.assert_not_called()
        logger.info.assert_called_with("Skipping lockfile generation for foo")

    @patch("doozerlib.backend.rebaser.RPMLockfileGenerator")
    def test_write_rpms_lock_file_empty_rpms(self, mock_rpmlockfile_cls):
        mock_rpmlockfile = AsyncMock()
        mock_rpmlockfile_cls.return_value = mock_rpmlockfile

        metadata = MagicMock()
        metadata.distgit_key = "foo"
        metadata.get_arches.return_value = ["x86_64"]
        metadata.config.konflux.cachi2.lockfile.packages.get.side_effect = lambda k, default=None: []
        metadata.get_parent_members.return_value = {}
        metadata.config.get.side_effect = lambda k, default=None: []
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_lockfile_force_enabled.return_value = False

        runtime = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=None)

        rebaser = KonfluxRebaser(runtime, MagicMock(), MagicMock(), "unsigned")
        asyncio.run(rebaser._write_rpms_lock_file(metadata, "test-group", Path(".")))

        mock_rpmlockfile.generate_lockfile.assert_not_called()

    @patch("doozerlib.backend.rebaser.RPMLockfileGenerator")
    def test_write_rpms_lock_file_with_force_enabled(self, mock_rpmlockfile_cls):
        mock_rpmlockfile = AsyncMock()
        mock_rpmlockfile_cls.return_value = mock_rpmlockfile

        metadata = MagicMock()
        metadata.distgit_key = "foo"
        metadata.get_arches.return_value = ["x86_64"]
        metadata.config.konflux.cachi2.lockfile.packages.get.side_effect = lambda k, default=None: []
        metadata.get_parent_members.return_value = {}
        metadata.config.get.side_effect = lambda k, default=None: []
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_lockfile_force_enabled.return_value = True

        runtime = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=MagicMock(installed_packages=["pkg1", "pkg2"]))

        rebaser = KonfluxRebaser(runtime, MagicMock(), MagicMock(), "unsigned")
        asyncio.run(rebaser._write_rpms_lock_file(metadata, "test-group", Path(".")))

        # Should call generate_lockfile with force=True
        mock_rpmlockfile.generate_lockfile.assert_awaited_with(
            ["x86_64"], set(), {"pkg1", "pkg2"}, Path("."), distgit_key="foo", force=True
        )

    @patch("doozerlib.backend.rebaser.RPMLockfileGenerator")
    def test_write_rpms_lock_file_with_force_disabled(self, mock_rpmlockfile_cls):
        mock_rpmlockfile = AsyncMock()
        mock_rpmlockfile_cls.return_value = mock_rpmlockfile

        metadata = MagicMock()
        metadata.distgit_key = "foo"
        metadata.get_arches.return_value = ["x86_64"]
        metadata.config.konflux.cachi2.lockfile.packages.get.side_effect = lambda k, default=None: []
        metadata.get_parent_members.return_value = {}
        metadata.config.get.side_effect = lambda k, default=None: []
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.is_lockfile_force_enabled.return_value = False

        runtime = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=MagicMock(installed_packages=["pkg1", "pkg2"]))

        rebaser = KonfluxRebaser(runtime, MagicMock(), MagicMock(), "unsigned")
        asyncio.run(rebaser._write_rpms_lock_file(metadata, "test-group", Path(".")))

        # Should call generate_lockfile with force=False
        mock_rpmlockfile.generate_lockfile.assert_awaited_with(
            ["x86_64"], set(), {"pkg1", "pkg2"}, Path("."), distgit_key="foo", force=False
        )

    def test_collect_repo_arch_combinations_empty_metas(self):
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        result = rebaser._collect_repo_arch_combinations([])
        self.assertEqual(result, set())

    def test_collect_repo_arch_combinations_no_lockfile_enabled(self):
        metadata = MagicMock()
        metadata.is_lockfile_generation_enabled.return_value = False

        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        result = rebaser._collect_repo_arch_combinations([metadata])
        self.assertEqual(result, set())

    def test_collect_repo_arch_combinations_basic(self):
        metadata1 = MagicMock()
        metadata1.is_lockfile_generation_enabled.return_value = True
        metadata1.config.get.return_value = ["repo1", "repo2"]
        metadata1.get_arches.return_value = ["x86_64", "aarch64"]

        metadata2 = MagicMock()
        metadata2.is_lockfile_generation_enabled.return_value = True
        metadata2.config.get.return_value = ["repo2", "repo3"]
        metadata2.get_arches.return_value = ["x86_64"]

        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        result = rebaser._collect_repo_arch_combinations([metadata1, metadata2])

        expected = {
            ("repo1", "x86_64"),
            ("repo1", "aarch64"),
            ("repo2", "x86_64"),
            ("repo2", "aarch64"),
            ("repo3", "x86_64"),
        }
        self.assertEqual(result, expected)

        # Verify the correct config key was requested
        metadata1.config.get.assert_called_with("enabled_repos", [])
        metadata2.config.get.assert_called_with("enabled_repos", [])

    def test_collect_repo_arch_combinations_mixed_enabled_disabled(self):
        metadata1 = MagicMock()
        metadata1.is_lockfile_generation_enabled.return_value = True
        metadata1.config.get.return_value = ["repo1"]
        metadata1.get_arches.return_value = ["x86_64"]

        metadata2 = MagicMock()
        metadata2.is_lockfile_generation_enabled.return_value = False
        metadata2.config.get.return_value = ["repo2"]
        metadata2.get_arches.return_value = ["x86_64"]

        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        result = rebaser._collect_repo_arch_combinations([metadata1, metadata2])

        expected = {("repo1", "x86_64")}
        self.assertEqual(result, expected)

        # Verify only the enabled metadata had its config checked
        metadata1.config.get.assert_called_with("enabled_repos", [])
        # metadata2 should not have been called since lockfile generation is disabled
        metadata2.config.get.assert_not_called()

    @patch("doozerlib.backend.rebaser.asyncio")
    def test_warm_repository_cache_empty_pairs(self, mock_asyncio):
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._logger = MagicMock()

        asyncio.run(rebaser._warm_repository_cache(set()))

        rebaser._logger.debug.assert_called_with("No repository-architecture combinations to warm")
        mock_asyncio.gather.assert_not_called()

    def test_warm_repository_cache_success(self):
        mock_collector = AsyncMock()
        mock_rpm_lockfile_generator = MagicMock()
        mock_rpm_lockfile_generator.builder = mock_collector

        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._logger = MagicMock()
        rebaser._rpm_lockfile_generator = mock_rpm_lockfile_generator

        repo_arch_pairs = {("repo1", "x86_64"), ("repo2", "x86_64"), ("repo1", "aarch64")}

        asyncio.run(rebaser._warm_repository_cache(repo_arch_pairs))

        rebaser._logger.info.assert_has_calls(
            [
                call("Warming repository cache for 3 (repo, arch) combinations across 2 architectures"),
                call("Repository cache warming completed"),
            ]
        )

        # Verify debug calls for each architecture
        debug_calls = rebaser._logger.debug.call_args_list
        self.assertTrue(
            any("arch x86_64 with repos: ['repo1', 'repo2']" in str(call_args) for call_args in debug_calls)
        )
        self.assertTrue(any("arch aarch64 with repos: ['repo1']" in str(call_args) for call_args in debug_calls))

        # Verify collector.load_repos was called for each architecture
        expected_calls = [call({"repo1", "repo2"}, "x86_64"), call({"repo1"}, "aarch64")]
        mock_collector.load_repos.assert_has_calls(expected_calls, any_order=True)

    def test_warm_repository_cache_with_error(self):
        """Test that cache warming handles errors gracefully and continues operation."""
        mock_collector = AsyncMock()
        mock_collector.load_repos.side_effect = Exception("Cache warming failed")
        mock_rpm_lockfile_generator = MagicMock()
        mock_rpm_lockfile_generator.builder = mock_collector

        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._logger = MagicMock()
        rebaser._rpm_lockfile_generator = mock_rpm_lockfile_generator

        repo_arch_pairs = {("repo1", "x86_64")}

        # Should not raise exception - should handle gracefully
        asyncio.run(rebaser._warm_repository_cache(repo_arch_pairs))

        # Verify that process completed and logged completion despite errors
        rebaser._logger.info.assert_has_calls(
            [
                call("Warming repository cache for 1 (repo, arch) combinations across 1 architectures"),
                call("Repository cache warming completed"),
            ]
        )

        # Verify the load_repos was attempted
        mock_collector.load_repos.assert_called_once_with({"repo1"}, "x86_64")
