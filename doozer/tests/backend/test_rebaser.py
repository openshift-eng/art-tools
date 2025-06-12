import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

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
    @patch("doozerlib.image.ImageMetadata.is_lockfile_generation_enabled", return_value=True)
    def test_write_rpms_lock_file_no_parents(self, mock_is_enabled, mock_rpmlockfile_cls):
        mock_rpmlockfile = AsyncMock()
        mock_rpmlockfile_cls.return_value = mock_rpmlockfile

        metadata = MagicMock()
        metadata.distgit_key = "foo"
        metadata.get_arches.return_value = ["x86_64"]
        metadata.config.konflux.cachi2.lockfile.packages.get.side_effect = lambda k, default=None: []
        metadata.get_parent_members.return_value = {}
        metadata.config.get.side_effect = lambda k, default=None: []

        runtime = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=MagicMock(installed_packages=["pkg1", "pkg2"]))

        rebaser = KonfluxRebaser(runtime, MagicMock(), MagicMock(), "unsigned")
        asyncio.run(rebaser._write_rpms_lock_file(metadata, "test-group", Path(".")))

        mock_rpmlockfile.generate_lockfile.assert_awaited_with(["x86_64"], set(), {"pkg1", "pkg2"}, Path("."))

    @patch("doozerlib.backend.rebaser.RPMLockfileGenerator")
    @patch("doozerlib.image.ImageMetadata.is_lockfile_generation_enabled", return_value=True)
    def test_write_rpms_lock_file_with_parents(self, mock_is_enabled, mock_rpmlockfile_cls):
        mock_rpmlockfile = AsyncMock()
        mock_rpmlockfile_cls.return_value = mock_rpmlockfile

        metadata = MagicMock()
        metadata.distgit_key = "foo"
        metadata.get_arches.return_value = ["x86_64"]
        metadata.config.konflux.cachi2.lockfile.packages.get.side_effect = lambda k, default=None: []
        metadata.get_parent_members.return_value = {"bar": None}
        metadata.config.get.side_effect = lambda k, default=None: []

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

        mock_rpmlockfile.generate_lockfile.assert_awaited_with(["x86_64"], set(), {"pkg1"}, Path("."))

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
    @patch("doozerlib.image.ImageMetadata.is_lockfile_generation_enabled", return_value=True)
    def test_write_rpms_lock_file_empty_rpms(self, mock_is_enabled, mock_rpmlockfile_cls):
        mock_rpmlockfile = AsyncMock()
        mock_rpmlockfile_cls.return_value = mock_rpmlockfile

        metadata = MagicMock()
        metadata.distgit_key = "foo"
        metadata.get_arches.return_value = ["x86_64"]
        metadata.config.konflux.cachi2.lockfile.packages.get.side_effect = lambda k, default=None: []
        metadata.get_parent_members.return_value = {}
        metadata.config.get.side_effect = lambda k, default=None: []

        runtime = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=None)

        rebaser = KonfluxRebaser(runtime, MagicMock(), MagicMock(), "unsigned")
        asyncio.run(rebaser._write_rpms_lock_file(metadata, "test-group", Path(".")))

        mock_rpmlockfile.generate_lockfile.assert_not_called()
