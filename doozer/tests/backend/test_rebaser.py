import asyncio
import re
import stat
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock

import semver
from artcommonlib.model import Missing, Model
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
        from types import SimpleNamespace

        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "open"
        # Set up konflux config as Model (dict subclass) to support both .get() and attribute access
        metadata.config.konflux = Model({})
        metadata.config.konflux['cachito'] = SimpleNamespace(mode=Missing)
        metadata.config.final_stage_user = Missing
        metadata.is_lockfile_generation_enabled.return_value = False

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
RUN go clean -cache || true
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/art-unsigned.repo /etc/yum.repos.d/
RUN curl https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
LABEL foo="bar baz"
USER 1000
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
RUN go clean -cache || true
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/art-unsigned.repo /etc/yum.repos.d/
RUN curl https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
USER 2000
RUN commands


# Start Konflux-specific steps
USER 0
RUN rm -f /etc/yum.repos.d/art-* && mv /tmp/art/yum_temp/* /etc/yum.repos.d/ || true
RUN rm -rf /tmp/art
USER 2000
# End Konflux-specific steps
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned", "test-repo")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path("."))

        self.assertEqual(dfp.content, expected)

    def test_add_build_repos_2(self):
        """
        Test with default values, but with final_stage_user set
        """
        from types import SimpleNamespace

        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "open"
        # Set up konflux config as Model (dict subclass) to support both .get() and attribute access
        metadata.config.konflux = Model({})
        metadata.config.konflux['cachito'] = SimpleNamespace(mode=Missing)
        metadata.config.final_stage_user = "3000"
        metadata.is_lockfile_generation_enabled.return_value = False

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
RUN go clean -cache || true
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/art-unsigned.repo /etc/yum.repos.d/
RUN curl https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
LABEL foo="bar baz"
USER 1000
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
RUN go clean -cache || true
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/art-unsigned.repo /etc/yum.repos.d/
RUN curl https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
USER 2000
RUN commands


# Start Konflux-specific steps
USER 0
RUN rm -f /etc/yum.repos.d/art-* && mv /tmp/art/yum_temp/* /etc/yum.repos.d/ || true
RUN rm -rf /tmp/art
USER 3000
# End Konflux-specific steps
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned", "test-repo")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path("."))

        self.assertEqual(dfp.content, expected)

    def test_add_build_repos_3(self):
        """
        Test with network_mode hermetic but lockfile disabled
        """
        from types import SimpleNamespace

        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.is_lockfile_generation_enabled.return_value = False
        # Set up konflux config as Model (dict subclass) to support both .get() and attribute access
        metadata.config.konflux = Model({})
        metadata.config.konflux['cachito'] = SimpleNamespace(mode=Missing)
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
RUN go clean -cache || true
ENV ART_BUILD_DEPS_MODE=default
# End Konflux-specific steps
LABEL foo="bar baz"
USER 1000
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=hermetic
RUN go clean -cache || true
ENV ART_BUILD_DEPS_MODE=default
# End Konflux-specific steps
USER 2000
RUN commands

USER 3000
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned", "test-repo")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path(self.directory.name))
        dfp.content.strip()
        self.assertEqual(expected.strip(), dfp.content.strip())

    def test_add_build_repos_hermetic_with_lockfile(self):
        """
        Test with network_mode hermetic and lockfile generation enabled
        """
        from types import SimpleNamespace

        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.get_arches.return_value = ["x86_64", "aarch64"]
        # Set up konflux config as Model (dict subclass) to support both .get() and attribute access
        metadata.config.konflux = Model({})
        metadata.config.konflux['cachito'] = SimpleNamespace(mode=Missing)
        metadata.config.final_stage_user = "3000"
        metadata.branch_el_target.return_value = 9
        metadata.get_lockfile_modules_to_install.return_value = set()

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
RUN go clean -cache || true
ENV ART_BUILD_DEPS_MODE=default
# End Konflux-specific steps
LABEL foo="bar baz"
USER 1000
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=hermetic
RUN go clean -cache || true
ENV ART_BUILD_DEPS_MODE=default
# End Konflux-specific steps
USER 2000
RUN commands

USER 3000
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned", "test-repo")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path(self.directory.name))
        dfp.content.strip()
        self.maxDiff = None
        self.assertEqual(expected.strip(), dfp.content.strip())

    def test_add_build_repos_4(self):
        """
        Test with non-hermetic, but with final_stage_user
        """
        from types import SimpleNamespace

        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "open"
        # Set up konflux config as Model (dict subclass) to support both .get() and attribute access
        metadata.config.konflux = Model({})
        metadata.config.konflux['cachito'] = SimpleNamespace(mode=Missing)
        metadata.config.final_stage_user = "3000"
        metadata.is_lockfile_generation_enabled.return_value = False

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
RUN go clean -cache || true
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/art-unsigned.repo /etc/yum.repos.d/
RUN curl https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
LABEL foo="bar baz"
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
RUN go clean -cache || true
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true
COPY .oit/art-unsigned.repo /etc/yum.repos.d/
RUN curl https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp/art
# End Konflux-specific steps
RUN commands


# Start Konflux-specific steps
USER 0
RUN rm -f /etc/yum.repos.d/art-* && mv /tmp/art/yum_temp/* /etc/yum.repos.d/ || true
RUN rm -rf /tmp/art
USER 3000
# End Konflux-specific steps
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned", "test-repo")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path(self.directory.name))

        self.assertEqual(dfp.content.strip(), expected.strip())

    def test_write_rpms_lock_file_enabled(self):
        metadata = MagicMock()
        metadata.distgit_key = "foo"
        metadata.is_lockfile_generation_enabled.return_value = True

        mock_generator = AsyncMock()
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned", "test-repo")
        rebaser.rpm_lockfile_generator = mock_generator
        rebaser._logger = MagicMock()

        asyncio.run(rebaser._write_rpms_lock_file(metadata, Path(".")))

        mock_generator.generate_lockfile.assert_awaited_once_with(metadata, Path("."))
        rebaser._logger.info.assert_called_with('Generating RPM lockfile for foo')

    def test_write_rpms_lock_file_disabled(self):
        metadata = MagicMock()
        metadata.distgit_key = "foo"
        metadata.is_lockfile_generation_enabled.return_value = False

        mock_generator = AsyncMock()
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned", "test-repo")
        rebaser.rpm_lockfile_generator = mock_generator
        rebaser._logger = MagicMock()

        asyncio.run(rebaser._write_rpms_lock_file(metadata, Path(".")))

        mock_generator.generate_lockfile.assert_not_called()
        rebaser._logger.debug.assert_called_with('RPM lockfile generation is disabled for foo')

    def test_go_version_regex_pattern(self):
        """Test the regex pattern used to match go version lines"""
        pattern = r"(^go \d\.\d+$)"

        # Test cases that should match
        valid_cases = ["go 1.22", "go 1.23", "go 1.30"]

        for case in valid_cases:
            with self.subTest(case=case):
                match = re.match(pattern, case)
                self.assertIsNotNone(match, f"Should match: {case}")
                self.assertEqual(match.group(1), case)

        # Test cases that should NOT match
        invalid_cases = [
            "go 1.22.0",  # already has patch
            "go 1.invalid",  # non-numeric
            "go invalid.version",  # completely invalid
            "go 1",  # missing minor version
            " go 1.22",  # leading space
            "go 1.22 ",  # trailing space
            "golang 1.22",  # wrong prefix
        ]

        for case in invalid_cases:
            with self.subTest(case=case):
                match = re.match(pattern, case)
                self.assertIsNone(match, f"Should NOT match: {case}")

    def test_semver_version_comparison_valid_versions(self):
        """Test semver version comparison for valid go versions"""
        min_version = semver.VersionInfo.parse("1.22.0")

        # Test cases that should be >= 1.22.0
        valid_higher_versions = [
            ("1.22", True),  # exactly minimum
            ("1.23", True),  # above minimum
            ("1.30", True),  # higher minor
            ("2.0", True),  # higher major
            ("1.999", True),  # very high minor
        ]

        for version_str, should_be_higher in valid_higher_versions:
            with self.subTest(version=version_str):
                current_version = semver.VersionInfo.parse(f"{version_str}.0")
                result = current_version >= min_version
                self.assertEqual(result, should_be_higher, f"Version {version_str} comparison failed")

        # Test cases that should be < 1.22.0
        valid_lower_versions = [
            ("1.21", False),
            ("1.20", False),
            ("1.19", False),
            ("1.0", False),
            ("0.99", False),
        ]

        for version_str, should_be_higher in valid_lower_versions:
            with self.subTest(version=version_str):
                current_version = semver.VersionInfo.parse(f"{version_str}.0")
                result = current_version >= min_version
                self.assertEqual(result, should_be_higher, f"Version {version_str} comparison failed")

    def test_semver_version_comparison_invalid_versions(self):
        """Test semver version parsing with invalid version strings"""
        invalid_versions = [
            "1.invalid",
            "invalid.version",
            "1.22.3.4",  # too many parts
            "",  # empty string
            "abc",  # non-numeric
        ]

        for invalid_version in invalid_versions:
            with self.subTest(version=invalid_version):
                with self.assertRaises(ValueError):
                    semver.VersionInfo.parse(f"{invalid_version}.0")

    def test_go_version_string_replacement(self):
        """Test the string replacement logic for go version lines"""
        test_cases = [
            # (input_line, go_version_string, expected_output)
            ("go 1.22", "go 1.22", "go 1.22.0"),
            ("go 1.23", "go 1.23", "go 1.23.0"),
            ("go 1.30", "go 1.30", "go 1.30.0"),
        ]

        for input_line, go_version_string, expected_output in test_cases:
            with self.subTest(input=input_line):
                # Simulate the replacement logic from the actual code
                result = input_line.replace(go_version_string, f"{go_version_string}.0")
                self.assertEqual(result, expected_output)

    def test_go_version_processing_logic_integration(self):
        """Test the complete go version processing logic flow"""

        def process_go_version_line(line):
            """Simulate the exact logic from the rebaser"""
            stripped_line = line.strip()
            match = re.match(r"(^go \d\.\d+$)", stripped_line)

            if not match:
                return line, False, None  # line, modified, error

            go_version_string = match.group(1)
            version_part = go_version_string.split(" ")[-1]

            try:
                current_version = semver.VersionInfo.parse(f"{version_part}.0")
                min_version = semver.VersionInfo.parse("1.22.0")

                if current_version >= min_version:
                    new_line = stripped_line.replace(go_version_string, f"{go_version_string}.0")
                    return f"{new_line}\n", True, None
                else:
                    return line, False, None
            except ValueError as e:
                return line, False, str(e)

        # Test cases
        test_cases = [
            # (input, expected_output, should_be_modified, should_have_error)
            ("go 1.22\n", "go 1.22.0\n", True, False),
            ("go 1.23\n", "go 1.23.0\n", True, False),
            ("go 1.21\n", "go 1.21\n", False, False),
            ("go 1.20\n", "go 1.20\n", False, False),
            ("go 1.22.0\n", "go 1.22.0\n", False, False),  # doesn't match regex
            ("go 1.invalid\n", "go 1.invalid\n", False, False),  # doesn't match regex
            ("  go 1.22  \n", "go 1.22.0\n", True, False),  # with whitespace
        ]

        for input_line, expected_output, should_be_modified, should_have_error in test_cases:
            with self.subTest(input=repr(input_line)):
                result_line, was_modified, error = process_go_version_line(input_line)

                if should_have_error:
                    self.assertIsNotNone(error)
                else:
                    self.assertIsNone(error)

                self.assertEqual(was_modified, should_be_modified)

                if should_be_modified:
                    self.assertEqual(result_line, expected_output)

    def test_go_version_boundary_conditions(self):
        """Test boundary conditions for go version comparisons"""
        min_version = semver.VersionInfo.parse("1.22.0")

        boundary_cases = [
            # (version_string, expected_result, description)
            ("1.21.99", False, "just below minimum with high patch"),
            ("1.22.0", True, "exact minimum"),
            ("1.22.1", True, "just above minimum"),
            ("1.21", False, "one minor version below"),
            ("1.23", True, "one minor version above"),
        ]

        for version_str, expected_result, description in boundary_cases:
            with self.subTest(version=version_str, desc=description):
                try:
                    if "." in version_str and len(version_str.split(".")) == 2:
                        # For X.Y format, add .0 for comparison
                        test_version = semver.VersionInfo.parse(f"{version_str}.0")
                    else:
                        # For X.Y.Z format, use as-is
                        test_version = semver.VersionInfo.parse(version_str)

                    result = test_version >= min_version
                    self.assertEqual(result, expected_result, f"Boundary test failed for {version_str}: {description}")
                except ValueError:
                    # If version parsing fails, it should be handled gracefully
                    pass

    def test_get_module_enablement_commands_disabled_by_config(self):
        """Test _get_module_enablement_commands returns empty list when dnf_modules_enable is disabled"""
        rebaser = KonfluxRebaser(
            runtime=MagicMock(), base_dir=Path("/tmp"), source_resolver=MagicMock(), repo_type="test"
        )
        rebaser._logger = MagicMock()

        # Create mock metadata with dnf_modules_enable disabled
        mock_metadata = MagicMock()
        mock_metadata.distgit_key = "test-image"
        mock_metadata.is_lockfile_generation_enabled.return_value = True
        mock_metadata.is_dnf_modules_enable_enabled.return_value = False

        result = rebaser._get_module_enablement_commands(mock_metadata)

        # Should return empty list
        self.assertEqual(result, [])

        # Should log that module enablement is disabled
        rebaser._logger.info.assert_called_once_with("DNF module enablement disabled for test-image")

        # Should not call other metadata methods since we returned early
        mock_metadata.branch_el_target.assert_not_called()
        mock_metadata.get_lockfile_modules_to_install.assert_not_called()

    def test_get_module_enablement_commands_enabled_by_config(self):
        """Test _get_module_enablement_commands generates commands when dnf_modules_enable is enabled"""
        rebaser = KonfluxRebaser(
            runtime=MagicMock(), base_dir=Path("/tmp"), source_resolver=MagicMock(), repo_type="test"
        )
        rebaser._logger = MagicMock()

        # Create mock metadata with dnf_modules_enable enabled
        mock_metadata = MagicMock()
        mock_metadata.distgit_key = "test-image"
        mock_metadata.is_lockfile_generation_enabled.return_value = True
        mock_metadata.is_dnf_modules_enable_enabled.return_value = True
        mock_metadata.branch_el_target.return_value = 9  # RHEL 9
        mock_metadata.get_lockfile_modules_to_install.return_value = {"postgresql:15", "maven:3.8"}

        result = rebaser._get_module_enablement_commands(mock_metadata)

        # Should return command list with USER 0 + RUN command
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], "USER 0")
        self.assertIn("RUN dnf module enable -y", result[1])
        self.assertIn("maven:3.8", result[1])
        self.assertIn("postgresql:15", result[1])

        # Should log the modules being enabled
        rebaser._logger.info.assert_called_once()
        log_message = rebaser._logger.info.call_args[0][0]
        self.assertIn("Enabling modules for test-image", log_message)

        # Should have called all metadata methods
        mock_metadata.is_dnf_modules_enable_enabled.assert_called_once()
        mock_metadata.branch_el_target.assert_called_once()
        mock_metadata.get_lockfile_modules_to_install.assert_called_once()

    def test_get_module_enablement_commands_with_existing_user_zero(self):
        """Test _get_module_enablement_commands skips USER 0 when already present in previous lines"""
        rebaser = KonfluxRebaser(
            runtime=MagicMock(), base_dir=Path("/tmp"), source_resolver=MagicMock(), repo_type="test"
        )
        rebaser._logger = MagicMock()

        mock_metadata = MagicMock()
        mock_metadata.distgit_key = "test-image"
        mock_metadata.is_lockfile_generation_enabled.return_value = True
        mock_metadata.is_dnf_modules_enable_enabled.return_value = True
        mock_metadata.branch_el_target.return_value = 9
        mock_metadata.get_lockfile_modules_to_install.return_value = {"postgresql:15"}

        # Previous lines contain USER 0
        previous_lines = ["ENV TEST=1", "USER 0", "RUN something"]
        result = rebaser._get_module_enablement_commands(mock_metadata, previous_lines)

        # Should NOT include USER 0 again
        self.assertEqual(len(result), 1)
        self.assertIn("RUN dnf module enable -y", result[0])
        self.assertIn("postgresql:15", result[0])

    def test_get_module_enablement_commands_with_different_user(self):
        """Test _get_module_enablement_commands adds USER 0 when previous USER is different"""
        rebaser = KonfluxRebaser(
            runtime=MagicMock(), base_dir=Path("/tmp"), source_resolver=MagicMock(), repo_type="test"
        )
        rebaser._logger = MagicMock()

        mock_metadata = MagicMock()
        mock_metadata.distgit_key = "test-image"
        mock_metadata.is_lockfile_generation_enabled.return_value = True
        mock_metadata.is_dnf_modules_enable_enabled.return_value = True
        mock_metadata.branch_el_target.return_value = 9
        mock_metadata.get_lockfile_modules_to_install.return_value = {"postgresql:15"}

        # Previous lines contain different USER
        previous_lines = ["ENV TEST=1", "USER 1001", "RUN something"]
        result = rebaser._get_module_enablement_commands(mock_metadata, previous_lines)

        # Should include USER 0 since current user is not root
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], "USER 0")
        self.assertIn("RUN dnf module enable -y", result[1])

    def test_get_module_enablement_commands_user_zero_then_other_user(self):
        """Test _get_module_enablement_commands adds USER 0 when USER 0 is followed by another USER"""
        rebaser = KonfluxRebaser(
            runtime=MagicMock(), base_dir=Path("/tmp"), source_resolver=MagicMock(), repo_type="test"
        )
        rebaser._logger = MagicMock()

        mock_metadata = MagicMock()
        mock_metadata.distgit_key = "test-image"
        mock_metadata.is_lockfile_generation_enabled.return_value = True
        mock_metadata.is_dnf_modules_enable_enabled.return_value = True
        mock_metadata.branch_el_target.return_value = 9
        mock_metadata.get_lockfile_modules_to_install.return_value = {"postgresql:15"}

        # Previous lines contain USER 0 then USER 1001 (most recent)
        previous_lines = ["ENV TEST=1", "USER 0", "RUN something", "USER 1001"]
        result = rebaser._get_module_enablement_commands(mock_metadata, previous_lines)

        # Should include USER 0 since current user is 1001 (not root)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], "USER 0")
        self.assertIn("RUN dnf module enable -y", result[1])

    def test_get_module_enablement_commands_no_previous_lines(self):
        """Test _get_module_enablement_commands adds USER 0 when no previous lines provided"""
        rebaser = KonfluxRebaser(
            runtime=MagicMock(), base_dir=Path("/tmp"), source_resolver=MagicMock(), repo_type="test"
        )
        rebaser._logger = MagicMock()

        mock_metadata = MagicMock()
        mock_metadata.distgit_key = "test-image"
        mock_metadata.is_lockfile_generation_enabled.return_value = True
        mock_metadata.is_dnf_modules_enable_enabled.return_value = True
        mock_metadata.branch_el_target.return_value = 9
        mock_metadata.get_lockfile_modules_to_install.return_value = {"postgresql:15"}

        # No previous lines provided (None)
        result = rebaser._get_module_enablement_commands(mock_metadata, None)

        # Should include USER 0 by default
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], "USER 0")
        self.assertIn("RUN dnf module enable -y", result[1])

    def test_get_module_enablement_commands_empty_previous_lines(self):
        """Test _get_module_enablement_commands adds USER 0 when previous lines is empty list"""
        rebaser = KonfluxRebaser(
            runtime=MagicMock(), base_dir=Path("/tmp"), source_resolver=MagicMock(), repo_type="test"
        )
        rebaser._logger = MagicMock()

        mock_metadata = MagicMock()
        mock_metadata.distgit_key = "test-image"
        mock_metadata.is_lockfile_generation_enabled.return_value = True
        mock_metadata.is_dnf_modules_enable_enabled.return_value = True
        mock_metadata.branch_el_target.return_value = 9
        mock_metadata.get_lockfile_modules_to_install.return_value = {"postgresql:15"}

        # Empty previous lines list
        result = rebaser._get_module_enablement_commands(mock_metadata, [])

        # Should include USER 0 by default
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], "USER 0")
        self.assertIn("RUN dnf module enable -y", result[1])

    def test_make_actual_release_string_ocp_with_el_suffix(self):
        """Test _make_actual_release_string uses el# suffix for OCP builds"""
        from artcommonlib.variants import BuildVariant

        runtime = MagicMock()
        runtime.assembly = "stream"
        runtime.group_config.public_upstreams = []

        rebaser = KonfluxRebaser(
            runtime=runtime,
            base_dir=Path("/tmp"),
            source_resolver=MagicMock(),
            repo_type="test",
            variant=BuildVariant.OCP,
        )

        metadata = MagicMock()
        metadata.branch_el_target.return_value = 9

        source = MagicMock()
        source.commit_hash = "abc123def456"
        source.commit_hash_short = "abc123d"

        result = rebaser._make_actual_release_string(
            metadata=metadata,
            input_release="4.17.0-202407241200.p?",
            private_fix=False,
            source=source,
        )

        # Should use .el9 suffix for OCP
        self.assertIn(".el9", result)
        self.assertNotIn(".scos", result)
        self.assertIn(".gabc123d", result)
        self.assertIn(".assembly.stream", result)

    def test_make_actual_release_string_okd_with_scos_suffix(self):
        """Test _make_actual_release_string uses scos# suffix for OKD builds"""
        from artcommonlib.variants import BuildVariant

        runtime = MagicMock()
        runtime.assembly = "stream"
        runtime.group_config.public_upstreams = []
        runtime.get_major_minor_fields.return_value = (4, 17)
        runtime.repos = MagicMock()
        runtime.konflux_db = None

        rebaser = KonfluxRebaser(
            runtime=runtime,
            base_dir=Path("/tmp"),
            source_resolver=MagicMock(),
            repo_type="test",
            variant=BuildVariant.OKD,
        )

        metadata = MagicMock()
        metadata.branch_el_target.return_value = 9

        source = MagicMock()
        source.commit_hash = "abc123def456"
        source.commit_hash_short = "abc123d"

        result = rebaser._make_actual_release_string(
            metadata=metadata,
            input_release="4.17.0-202407241200.p?",
            private_fix=False,
            source=source,
        )

        # Should use .scos9 suffix for OKD
        self.assertIn(".scos9", result)
        self.assertNotIn(".el", result)
        self.assertIn(".gabc123d", result)
        self.assertIn(".assembly.stream", result)

    def test_make_actual_release_string_okd_different_versions(self):
        """Test _make_actual_release_string uses correct scos# for different RHEL versions"""
        from artcommonlib.variants import BuildVariant

        runtime = MagicMock()
        runtime.assembly = "stream"
        runtime.group_config.public_upstreams = []
        runtime.get_major_minor_fields.return_value = (4, 17)
        runtime.repos = MagicMock()
        runtime.konflux_db = None

        for el_version in [8, 9, 10]:
            with self.subTest(el_version=el_version):
                rebaser = KonfluxRebaser(
                    runtime=runtime,
                    base_dir=Path("/tmp"),
                    source_resolver=MagicMock(),
                    repo_type="test",
                    variant=BuildVariant.OKD,
                )

                metadata = MagicMock()
                metadata.branch_el_target.return_value = el_version

                source = MagicMock()
                source.commit_hash = "abc123def456"
                source.commit_hash_short = "abc123d"

                result = rebaser._make_actual_release_string(
                    metadata=metadata,
                    input_release="4.17.0-202407241200.p?",
                    private_fix=False,
                    source=source,
                )

                # Should use .scos{el_version} suffix for OKD
                expected_suffix = f".scos{el_version}"
                self.assertIn(expected_suffix, result)
                self.assertNotIn(".el", result)

    def test_get_el_target_string_ocp(self):
        """Test _get_el_target_string returns el# for OCP builds"""
        from artcommonlib.variants import BuildVariant

        runtime = MagicMock()
        runtime.repos = MagicMock()
        runtime.konflux_db = None

        rebaser = KonfluxRebaser(
            runtime=runtime,
            base_dir=Path("/tmp"),
            source_resolver=MagicMock(),
            repo_type="test",
            variant=BuildVariant.OCP,
        )

        self.assertEqual(rebaser._get_el_target_string(8), "el8")
        self.assertEqual(rebaser._get_el_target_string(9), "el9")
        self.assertEqual(rebaser._get_el_target_string(10), "el10")

    def test_get_el_target_string_okd(self):
        """Test _get_el_target_string returns scos# for OKD builds"""
        from artcommonlib.variants import BuildVariant

        runtime = MagicMock()
        runtime.get_major_minor_fields.return_value = (4, 17)
        runtime.repos = MagicMock()
        runtime.konflux_db = None

        rebaser = KonfluxRebaser(
            runtime=runtime,
            base_dir=Path("/tmp"),
            source_resolver=MagicMock(),
            repo_type="test",
            variant=BuildVariant.OKD,
        )

        self.assertEqual(rebaser._get_el_target_string(8), "scos8")
        self.assertEqual(rebaser._get_el_target_string(9), "scos9")
        self.assertEqual(rebaser._get_el_target_string(10), "scos10")

    def test_make_actual_release_string_okd_with_image_override(self):
        """Test OKD build with image-specific okd.distgit.branch override"""
        from artcommonlib.variants import BuildVariant

        runtime = MagicMock()
        runtime.assembly = "stream"
        runtime.group_config.public_upstreams = []
        runtime.get_major_minor_fields.return_value = (5, 0)
        runtime.branch = "rhaos-5.0-rhel-10"  # Group-level okd.branch
        runtime.repos = MagicMock()
        runtime.konflux_db = None

        rebaser = KonfluxRebaser(
            runtime=runtime,
            base_dir=Path("/tmp"),
            source_resolver=MagicMock(),
            repo_type="test",
            variant=BuildVariant.OKD,
        )

        # Image has okd.distgit.branch override to rhel-9
        metadata = MagicMock()
        metadata.config.okd.distgit.branch = "rhaos-5.0-rhel-9"
        metadata.branch_el_target.return_value = 9  # Should not be used

        source = MagicMock()
        source.commit_hash = "abc123def456"
        source.commit_hash_short = "abc123d"

        result = rebaser._make_actual_release_string(
            metadata=metadata,
            input_release="202601131053.p?",
            private_fix=False,
            source=source,
        )

        # Should use image override (rhel-9) not runtime.branch (rhel-10)
        self.assertIn(".scos9", result)
        self.assertNotIn(".scos10", result)

    def test_make_actual_release_string_okd_with_runtime_branch(self):
        """Test OKD build without image override, using runtime.branch from group okd.branch"""
        from artcommonlib.model import Missing
        from artcommonlib.variants import BuildVariant

        runtime = MagicMock()
        runtime.assembly = "stream"
        runtime.group_config.public_upstreams = []
        runtime.get_major_minor_fields.return_value = (5, 0)
        runtime.branch = "rhaos-5.0-rhel-10"  # Group-level okd.branch
        runtime.repos = MagicMock()
        runtime.konflux_db = None

        rebaser = KonfluxRebaser(
            runtime=runtime,
            base_dir=Path("/tmp"),
            source_resolver=MagicMock(),
            repo_type="test",
            variant=BuildVariant.OKD,
        )

        # Image has NO okd.distgit.branch override
        metadata = MagicMock()
        metadata.config.okd.distgit.branch = Missing
        metadata.branch_el_target.return_value = 9  # Should not be used

        source = MagicMock()
        source.commit_hash = "abc123def456"
        source.commit_hash_short = "abc123d"

        result = rebaser._make_actual_release_string(
            metadata=metadata,
            input_release="202601131053.p?",
            private_fix=False,
            source=source,
        )

        # Should use runtime.branch (rhel-10) not metadata.branch_el_target (9)
        self.assertIn(".scos10", result)
        self.assertNotIn(".scos9", result)

    def test_make_actual_release_string_okd_fallback_to_branch_el_target(self):
        """Test OKD build falls back to metadata.branch_el_target when no override or runtime.branch"""
        from artcommonlib.model import Missing
        from artcommonlib.variants import BuildVariant

        runtime = MagicMock()
        runtime.assembly = "stream"
        runtime.group_config.public_upstreams = []
        runtime.get_major_minor_fields.return_value = (5, 0)
        runtime.branch = None  # No runtime.branch set
        runtime.repos = MagicMock()
        runtime.konflux_db = None

        rebaser = KonfluxRebaser(
            runtime=runtime,
            base_dir=Path("/tmp"),
            source_resolver=MagicMock(),
            repo_type="test",
            variant=BuildVariant.OKD,
        )

        # Image has NO okd.distgit.branch override
        metadata = MagicMock()
        metadata.config.okd.distgit.branch = Missing
        metadata.branch_el_target.return_value = 9  # Should be used as fallback

        source = MagicMock()
        source.commit_hash = "abc123def456"
        source.commit_hash_short = "abc123d"

        result = rebaser._make_actual_release_string(
            metadata=metadata,
            input_release="202601131053.p?",
            private_fix=False,
            source=source,
        )

        # Should fall back to metadata.branch_el_target (9)
        self.assertIn(".scos9", result)

    def test_identify_stage_references_simple(self):
        """Test stage reference detection with simple multi-stage build"""
        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
            FROM registry.io/base:latest AS build
            RUN echo "building"

            FROM build AS metadata
            RUN echo "metadata"

            FROM build
            COPY --from=metadata /data /data
        """

        result = KonfluxRebaser._identify_stage_references(dfp)

        # Expected: [False, True, True]
        # - First FROM is an external image (base image)
        # - Second FROM references "build" stage
        # - Third FROM references "build" stage
        self.assertEqual(result, [False, True, True])
        self.assertEqual(len(result), 3)

    def test_identify_stage_references_no_stage_refs(self):
        """Test with Dockerfile that has no stage references"""
        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
            FROM registry.io/base1:latest
            RUN echo "step1"

            FROM registry.io/base2:latest
            RUN echo "step2"
        """

        result = KonfluxRebaser._identify_stage_references(dfp)

        # Expected: [False, False]
        # - Both FROM directives are external images
        self.assertEqual(result, [False, False])

    def test_identify_stage_references_all_stage_refs(self):
        """Test with Dockerfile where all but first FROM are stage references"""
        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
            FROM registry.io/base:v1 AS stage1
            RUN echo "stage1"

            FROM stage1 AS stage2
            RUN echo "stage2"

            FROM stage2 AS stage3
            RUN echo "stage3"

            FROM stage3
            RUN echo "final"
        """

        result = KonfluxRebaser._identify_stage_references(dfp)

        # Expected: [False, True, True, True]
        self.assertEqual(result, [False, True, True, True])

    def test_identify_stage_references_mixed(self):
        """Test with Dockerfile mixing external images and stage references"""
        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
            FROM registry.io/base:v1 AS builder
            RUN echo "building"

            FROM registry.io/tools:latest AS tools
            RUN echo "tools"

            FROM builder
            COPY --from=tools /bin/tool /bin/tool
        """

        result = KonfluxRebaser._identify_stage_references(dfp)

        # Expected: [False, False, True]
        # - First FROM: external (base image)
        # - Second FROM: external (tools image)
        # - Third FROM: references "builder" stage
        self.assertEqual(result, [False, False, True])

    def test_identify_stage_references_rhcos_node_image(self):
        """Test with actual rhcos-node-image Dockerfile pattern"""
        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
            FROM quay.io/openshift-release-dev/ocp-v4.0-art-dev:c9s-coreos AS build
            ARG OPENSHIFT_CI=0
            RUN --mount=type=bind,target=/run/src /run/src/build-node-image.sh

            FROM build AS metadata
            RUN --mount=type=bind,target=/run/src /run/src/scripts/generate-metadata

            FROM build
            COPY --from=metadata /usr/share/openshift /usr/share/openshift
            LABEL io.openshift.metalayer=true
        """

        result = KonfluxRebaser._identify_stage_references(dfp)

        # Expected: [False, True, True]
        # This is the exact pattern from rhcos-node-image
        self.assertEqual(result, [False, True, True])
        self.assertEqual(len(result), 3)

    def test_identify_stage_references_case_insensitive(self):
        """Test that stage name references are case-insensitive (Docker behavior)"""
        dfp = DockerfileParser(path=self.directory.name)
        dfp.content = """
            FROM registry.io/base:latest AS Build
            RUN echo "building"

            FROM build AS METADATA
            RUN echo "metadata"

            FROM BUILD
            COPY --from=metadata /data /data
        """

        result = KonfluxRebaser._identify_stage_references(dfp)

        # Expected: [False, True, True]
        # - First FROM: external image (base)
        # - Second FROM: references "Build" stage (case-insensitive: "build")
        # - Third FROM: references "Build" stage (case-insensitive: "BUILD" -> "build")
        # Note: COPY --from=metadata is not a FROM directive, so not counted
        self.assertEqual(result, [False, True, True])
        self.assertEqual(len(result), 3)


class TestResolveVendorSymlinks(TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.dest_dir = Path(self.directory.name)

        self.rebaser = KonfluxRebaser.__new__(KonfluxRebaser)
        self.rebaser._logger = MagicMock()

    def test_no_vendor_dir(self):
        asyncio.run(self.rebaser._resolve_vendor_symlinks(self.dest_dir))
        self.rebaser._logger.info.assert_not_called()

    def test_vendor_dir_no_symlinks(self):
        vendor = self.dest_dir / "vendor" / "example.com" / "pkg"
        vendor.mkdir(parents=True)
        (vendor / "main.go").write_text("package pkg")

        asyncio.run(self.rebaser._resolve_vendor_symlinks(self.dest_dir))
        self.rebaser._logger.info.assert_not_called()

    def test_resolves_directory_symlinks(self):
        staging = self.dest_dir / "staging" / "src" / "k8s.io" / "api"
        staging.mkdir(parents=True)
        (staging / "types.go").write_text("package api")
        (staging / "doc.go").write_text("package api // doc")

        vendor = self.dest_dir / "vendor" / "k8s.io"
        vendor.mkdir(parents=True)
        (vendor / "api").symlink_to(staging)

        asyncio.run(self.rebaser._resolve_vendor_symlinks(self.dest_dir))

        resolved = vendor / "api"
        self.assertTrue(resolved.is_dir())
        self.assertFalse(resolved.is_symlink())
        self.assertEqual((resolved / "types.go").read_text(), "package api")
        self.assertEqual((resolved / "doc.go").read_text(), "package api // doc")

    def test_resolves_file_symlinks(self):
        real_file = self.dest_dir / "real_module.go"
        real_file.write_text("package real")

        vendor = self.dest_dir / "vendor" / "example.com"
        vendor.mkdir(parents=True)
        (vendor / "module.go").symlink_to(real_file)

        asyncio.run(self.rebaser._resolve_vendor_symlinks(self.dest_dir))

        resolved = vendor / "module.go"
        self.assertTrue(resolved.is_file())
        self.assertFalse(resolved.is_symlink())
        self.assertEqual(resolved.read_text(), "package real")

    def test_skips_broken_symlinks(self):
        vendor = self.dest_dir / "vendor" / "k8s.io"
        vendor.mkdir(parents=True)
        (vendor / "missing").symlink_to("/nonexistent/path")

        asyncio.run(self.rebaser._resolve_vendor_symlinks(self.dest_dir))

        self.assertTrue((vendor / "missing").is_symlink())
        self.rebaser._logger.warning.assert_called_once()

    def test_resolves_multiple_symlinks(self):
        staging_base = self.dest_dir / "staging" / "src" / "k8s.io"

        for name in ["api", "client-go", "apimachinery"]:
            pkg = staging_base / name
            pkg.mkdir(parents=True)
            (pkg / "main.go").write_text(f"package {name}")

        vendor = self.dest_dir / "vendor" / "k8s.io"
        vendor.mkdir(parents=True)
        for name in ["api", "client-go", "apimachinery"]:
            (vendor / name).symlink_to(staging_base / name)

        asyncio.run(self.rebaser._resolve_vendor_symlinks(self.dest_dir))

        for name in ["api", "client-go", "apimachinery"]:
            resolved = vendor / name
            self.assertTrue(resolved.is_dir())
            self.assertFalse(resolved.is_symlink())
            self.assertEqual((resolved / "main.go").read_text(), f"package {name}")

    def test_strips_executable_permissions_from_resolved_dir(self):
        staging = self.dest_dir / "staging" / "src" / "k8s.io" / "apiserver"
        webhook_dir = staging / "pkg" / "util" / "webhook"
        webhook_dir.mkdir(parents=True)
        gencerts = webhook_dir / "gencerts.sh"
        gencerts.write_text("#!/bin/bash\necho hello")
        gencerts.chmod(0o755)
        (webhook_dir / "webhook.go").write_text("package webhook")

        vendor = self.dest_dir / "vendor" / "k8s.io"
        vendor.mkdir(parents=True)
        (vendor / "apiserver").symlink_to(staging)

        asyncio.run(self.rebaser._resolve_vendor_symlinks(self.dest_dir))

        resolved_sh = vendor / "apiserver" / "pkg" / "util" / "webhook" / "gencerts.sh"
        self.assertTrue(resolved_sh.is_file())
        self.assertFalse(resolved_sh.is_symlink())
        self.assertEqual(resolved_sh.read_text(), "#!/bin/bash\necho hello")
        mode = resolved_sh.stat().st_mode
        exec_bits = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        self.assertEqual(mode & exec_bits, 0, "Executable bits should be stripped")

    def test_strips_executable_permissions_from_resolved_file(self):
        real_file = self.dest_dir / "staging" / "run.sh"
        real_file.parent.mkdir(parents=True)
        real_file.write_text("#!/bin/bash")
        real_file.chmod(0o755)

        vendor = self.dest_dir / "vendor" / "example.com"
        vendor.mkdir(parents=True)
        (vendor / "run.sh").symlink_to(real_file)

        asyncio.run(self.rebaser._resolve_vendor_symlinks(self.dest_dir))

        resolved = vendor / "run.sh"
        mode = resolved.stat().st_mode
        exec_bits = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        self.assertEqual(mode & exec_bits, 0, "Executable bits should be stripped")


class TestCpeVersionExtraction(TestCase):
    """
    Tests for the CPE version extraction logic in _update_dockerfile.
    The logic must produce major.minor regardless of whether version
    has 2 segments (v4.20) or 3 segments (v4.20.0).
    """

    @staticmethod
    def _extract_cpe_version(version: str) -> str:
        """
        Replicate the CPE version extraction logic from rebaser.py.
        """
        version_parts = version.lstrip("v").split(".")
        return f"{version_parts[0]}.{version_parts[1]}" if len(version_parts) >= 2 else version_parts[0]

    def test_three_segment_version(self):
        self.assertEqual(self._extract_cpe_version("v4.20.0"), "4.20")

    def test_two_segment_version(self):
        self.assertEqual(self._extract_cpe_version("v4.20"), "4.20")

    def test_three_segment_nonzero_patch(self):
        self.assertEqual(self._extract_cpe_version("v4.18.3"), "4.18")

    def test_no_prefix(self):
        self.assertEqual(self._extract_cpe_version("4.20.0"), "4.20")

    def test_single_segment(self):
        self.assertEqual(self._extract_cpe_version("v4"), "4")
