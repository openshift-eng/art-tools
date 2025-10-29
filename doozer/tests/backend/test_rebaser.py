import asyncio
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

import semver
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
        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.is_lockfile_generation_enabled.return_value = False
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
        metadata = MagicMock()
        metadata.get_konflux_network_mode.return_value = "hermetic"
        metadata.is_lockfile_generation_enabled.return_value = True
        metadata.get_arches.return_value = ["x86_64", "aarch64"]
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
