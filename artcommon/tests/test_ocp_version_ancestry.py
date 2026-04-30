"""
Test the OCP version ancestry and build-suggestions models
"""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from artcommonlib.ocp_version_ancestry import (
    BuildSuggestions,
    SuggestionsSpec,
    calc_upgrade_sources_async,
    get_build_suggestions_async,
    get_cincinnati_channels,
)
from pydantic import ValidationError


class TestSuggestionsSpec(unittest.TestCase):
    """Test the SuggestionsSpec Pydantic model"""

    def test_valid_suggestions_spec(self):
        """Test that a valid SuggestionsSpec can be created"""
        spec = SuggestionsSpec(
            minor_min="4.22.0-rc.0",
            minor_max="4.22.9999",
            minor_block_list=[],
            z_min="5.0.0-ec.0",
            z_max="5.0.9999",
            z_block_list=[],
        )

        self.assertEqual(spec.minor_min, "4.22.0-rc.0")
        self.assertEqual(spec.minor_max, "4.22.9999")
        self.assertEqual(spec.minor_block_list, [])
        self.assertEqual(spec.z_min, "5.0.0-ec.0")
        self.assertEqual(spec.z_max, "5.0.9999")
        self.assertEqual(spec.z_block_list, [])

    def test_valid_suggestions_spec_with_block_lists(self):
        """Test SuggestionsSpec with populated block lists"""
        spec = SuggestionsSpec(
            minor_min="4.22.0",
            minor_max="4.22.9999",
            minor_block_list=["4.22.5", "4.22.6-rc.1"],
            z_min="5.0.0",
            z_max="5.0.9999",
            z_block_list=["5.0.1", "5.0.2-rc.0"],
        )

        self.assertEqual(len(spec.minor_block_list), 2)
        self.assertEqual(len(spec.z_block_list), 2)
        self.assertIn("4.22.5", spec.minor_block_list)
        self.assertIn("5.0.2-rc.0", spec.z_block_list)

    def test_invalid_semver_in_minor_min(self):
        """Test that invalid semver in minor_min raises ValidationError"""
        with self.assertRaises(ValidationError) as context:
            SuggestionsSpec(
                minor_min="not-a-version",  # Invalid semver
                minor_max="4.22.9999",
                minor_block_list=[],
                z_min="5.0.0",
                z_max="5.0.9999",
                z_block_list=[],
            )

        self.assertIn("Invalid semver format", str(context.exception))

    def test_invalid_semver_in_minor_max(self):
        """Test that invalid semver in minor_max raises ValidationError"""
        with self.assertRaises(ValidationError) as context:
            SuggestionsSpec(
                minor_min="4.22.0",
                minor_max="4.22.xyz",  # Invalid semver
                minor_block_list=[],
                z_min="5.0.0",
                z_max="5.0.9999",
                z_block_list=[],
            )

        self.assertIn("Invalid semver format", str(context.exception))

    def test_invalid_semver_in_block_list(self):
        """Test that invalid semver in block lists raises ValidationError"""
        with self.assertRaises(ValidationError) as context:
            SuggestionsSpec(
                minor_min="4.22.0",
                minor_max="4.22.9999",
                minor_block_list=["4.22.5", "invalid-version"],  # One invalid
                z_min="5.0.0",
                z_max="5.0.9999",
                z_block_list=[],
            )

        self.assertIn("Invalid semver format in block list", str(context.exception))

    def test_optional_max_fields(self):
        """Test that minor_max and z_max can be omitted"""
        spec = SuggestionsSpec(
            minor_min="4.22.0-rc.0",
            minor_block_list=[],
            z_min="5.0.0-ec.0",
            z_block_list=[],
        )

        self.assertEqual(spec.minor_min, "4.22.0-rc.0")
        self.assertIsNone(spec.minor_max)
        self.assertEqual(spec.z_min, "5.0.0-ec.0")
        self.assertIsNone(spec.z_max)

    def test_missing_required_fields(self):
        """Test that missing required fields raise ValidationError"""
        with self.assertRaises(ValidationError) as context:
            SuggestionsSpec(
                minor_min="4.22.0",
                minor_max="4.22.9999",
                # Missing minor_block_list (required)
                z_min="5.0.0",
                z_max="5.0.9999",
                z_block_list=[],
            )

        self.assertIn("Field required", str(context.exception))


class TestBuildSuggestions(unittest.TestCase):
    """Test the BuildSuggestions Pydantic model"""

    def test_valid_build_suggestions_default_only(self):
        """Test BuildSuggestions with only default section"""
        data = {
            "default": {
                "minor_min": "4.22.0-rc.0",
                "minor_max": "4.22.9999",
                "minor_block_list": [],
                "z_min": "5.0.0-ec.0",
                "z_max": "5.0.9999",
                "z_block_list": [],
            }
        }

        suggestions = BuildSuggestions.model_validate(data)

        self.assertEqual(suggestions.default.minor_min, "4.22.0-rc.0")
        self.assertEqual(suggestions.default.z_min, "5.0.0-ec.0")

    def test_valid_build_suggestions_with_arch_override(self):
        """Test BuildSuggestions with architecture-specific overrides"""
        data = {
            "default": {
                "minor_min": "4.22.0-rc.0",
                "minor_max": "4.22.9999",
                "minor_block_list": [],
                "z_min": "5.0.0-ec.0",
                "z_max": "5.0.9999",
                "z_block_list": [],
            },
            "s390x": {
                "minor_min": "4.22.1",  # Different min for s390x
                "minor_max": "4.22.9999",
                "minor_block_list": ["4.22.5"],
                "z_min": "5.0.0-ec.0",
                "z_max": "5.0.9999",
                "z_block_list": [],
            },
        }

        suggestions = BuildSuggestions.model_validate(data)

        # Check default
        self.assertEqual(suggestions.default.minor_min, "4.22.0-rc.0")

        # Check s390x override exists
        s390x_spec = suggestions.s390x
        self.assertIsNotNone(s390x_spec)
        self.assertEqual(s390x_spec.minor_min, "4.22.1")
        self.assertEqual(s390x_spec.minor_block_list, ["4.22.5"])

    def test_get_for_arch_default(self):
        """Test get_for_arch returns default when no override exists"""
        data = {
            "default": {
                "minor_min": "4.22.0",
                "minor_max": "4.22.9999",
                "minor_block_list": [],
                "z_min": "5.0.0",
                "z_max": "5.0.9999",
                "z_block_list": [],
            }
        }

        suggestions = BuildSuggestions.model_validate(data)

        # Request non-existent arch, should get default
        spec = suggestions.get_for_arch("aarch64")
        self.assertEqual(spec.minor_min, "4.22.0")

    def test_get_for_arch_with_override(self):
        """Test get_for_arch returns architecture-specific override"""
        data = {
            "default": {
                "minor_min": "4.22.0",
                "minor_max": "4.22.9999",
                "minor_block_list": [],
                "z_min": "5.0.0",
                "z_max": "5.0.9999",
                "z_block_list": [],
            },
            "s390x": {
                "minor_min": "4.22.1",
                "minor_max": "4.22.9999",
                "minor_block_list": [],
                "z_min": "5.0.0",
                "z_max": "5.0.9999",
                "z_block_list": [],
            },
        }

        suggestions = BuildSuggestions.model_validate(data)

        # Request s390x, should get override
        spec = suggestions.get_for_arch("s390x")
        self.assertEqual(spec.minor_min, "4.22.1")

        # Request default explicitly
        spec_default = suggestions.get_for_arch("default")
        self.assertEqual(spec_default.minor_min, "4.22.0")

    def test_malformed_arch_override_rejected(self):
        """Test that non-mapping architecture overrides are rejected"""
        data = {
            "default": {
                "minor_min": "4.22.0",
                "minor_max": "4.22.9999",
                "minor_block_list": [],
                "z_min": "5.0.0",
                "z_max": "5.0.9999",
                "z_block_list": [],
            },
            "s390x": "oops",
        }

        with self.assertRaises(ValidationError):
            BuildSuggestions.model_validate(data)

    def test_missing_default_section(self):
        """Test that missing default section raises ValidationError"""
        data = {
            "s390x": {
                "minor_min": "4.22.1",
                "minor_max": "4.22.9999",
                "minor_block_list": [],
                "z_min": "5.0.0",
                "z_max": "5.0.9999",
                "z_block_list": [],
            }
        }

        with self.assertRaises(ValidationError) as context:
            BuildSuggestions.model_validate(data)

        self.assertIn("Field required", str(context.exception))


class TestGetBuildSuggestionsAsync(unittest.IsolatedAsyncioTestCase):
    """Test the async get_build_suggestions_async function"""

    async def test_successful_fetch_and_parse(self):
        """Test successfully fetching and parsing build-suggestions"""
        yaml_content = """
default:
  minor_min: "4.22.0-rc.0"
  minor_max: "4.22.9999"
  minor_block_list: []
  z_min: "5.0.0-ec.0"
  z_max: "5.0.9999"
  z_block_list: []
"""

        mock_response = MagicMock()
        mock_response.text = yaml_content
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(return_value=mock_response)

            result = await get_build_suggestions_async(5, 0)

            self.assertIsInstance(result, BuildSuggestions)
            self.assertEqual(result.default.minor_min, "4.22.0-rc.0")
            self.assertEqual(result.default.z_min, "5.0.0-ec.0")

    async def test_invalid_yaml_syntax(self):
        """Test that invalid YAML syntax raises ValueError with OTA message"""
        invalid_yaml = """
default:
  minor_min: "4.22.0"
  - invalid yaml syntax here
"""

        mock_response = MagicMock()
        mock_response.text = invalid_yaml
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(return_value=mock_response)

            with self.assertRaises(ValueError) as context:
                await get_build_suggestions_async(5, 0)

            error_msg = str(context.exception)
            self.assertIn("Failed to parse YAML", error_msg)
            self.assertIn("OTA", error_msg)
            self.assertIn("5.0.yaml", error_msg)

    async def test_invalid_semver_in_yaml(self):
        """Test that invalid semver raises ValueError with OTA message"""
        yaml_content = """
default:
  minor_min: "not-a-version"
  minor_max: "4.22.9999"
  minor_block_list: []
  z_min: "5.0.0"
  z_max: "5.0.9999"
  z_block_list: []
"""

        mock_response = MagicMock()
        mock_response.text = yaml_content
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(return_value=mock_response)

            with self.assertRaises(ValueError) as context:
                await get_build_suggestions_async(5, 0)

            error_msg = str(context.exception)
            self.assertIn("Failed to validate", error_msg)
            self.assertIn("OTA", error_msg)
            self.assertIn("5.0", error_msg)

    async def test_missing_required_fields(self):
        """Test that missing required fields raises ValueError with OTA message"""
        yaml_content = """
default:
  minor_min: "4.22.0"
  minor_max: "4.22.9999"
  # Missing minor_block_list and other fields
"""

        mock_response = MagicMock()
        mock_response.text = yaml_content
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(return_value=mock_response)

            with self.assertRaises(ValueError) as context:
                await get_build_suggestions_async(5, 0)

            error_msg = str(context.exception)
            self.assertIn("Failed to validate", error_msg)
            self.assertIn("OTA", error_msg)

    async def test_http_error_propagates(self):
        """Test that HTTP errors (404, etc.) propagate naturally"""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 Not Found",
            request=MagicMock(),
            response=MagicMock(status_code=404),
        )

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(return_value=mock_response)

            with self.assertRaises(httpx.HTTPStatusError):
                await get_build_suggestions_async(99, 99)  # Non-existent version

    async def test_custom_url_and_timeout(self):
        """Test that custom URL and timeout parameters are used"""
        yaml_content = """
default:
  minor_min: "4.22.0"
  minor_max: "4.22.9999"
  minor_block_list: []
  z_min: "5.0.0"
  z_max: "5.0.9999"
  z_block_list: []
"""

        mock_response = MagicMock()
        mock_response.text = yaml_content
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_get = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__.return_value.get = mock_get

            custom_url = "https://example.com/build-suggestions/"
            custom_timeout = 60.0

            await get_build_suggestions_async(5, 0, suggestions_url=custom_url, timeout=custom_timeout)

            # Verify the URL was constructed correctly
            called_url = mock_get.call_args[0][0]
            self.assertEqual(called_url, "https://example.com/build-suggestions/5.0.yaml")

            # Verify timeout was passed
            called_timeout = mock_get.call_args[1]["timeout"]
            self.assertEqual(called_timeout, 60.0)


class TestGetCincinnatiChannels(unittest.TestCase):
    """Test the get_cincinnati_channels function"""

    def test_ocp_4_1(self):
        """OCP 4.1 uses special channel names (prerelease, stable)"""
        channels = get_cincinnati_channels(4, 1)
        self.assertEqual(channels, ['prerelease-4.1', 'stable-4.1'])

    def test_ocp_4_x(self):
        """OCP 4.2+ uses standard channel names"""
        channels = get_cincinnati_channels(4, 16)
        self.assertEqual(channels, ['candidate-4.16', 'fast-4.16', 'stable-4.16'])

        channels = get_cincinnati_channels(4, 22)
        self.assertEqual(channels, ['candidate-4.22', 'fast-4.22', 'stable-4.22'])

    def test_ocp_5_x(self):
        """OCP 5.x uses standard channel names"""
        channels = get_cincinnati_channels(5, 0)
        self.assertEqual(channels, ['candidate-5.0', 'fast-5.0', 'stable-5.0'])

        channels = get_cincinnati_channels(5, 5)
        self.assertEqual(channels, ['candidate-5.5', 'fast-5.5', 'stable-5.5'])

    def test_int_versions(self):
        """Function accepts int inputs"""
        channels = get_cincinnati_channels(5, 0)
        self.assertEqual(channels, ['candidate-5.0', 'fast-5.0', 'stable-5.0'])

    def test_rejects_ocp_3(self):
        """Cincinnati channels don't exist for OCP 3.x and earlier"""
        with self.assertRaises(ValueError) as ctx:
            get_cincinnati_channels(3, 11)
        self.assertIn('Cincinnati channels are only available for OCP 4.x and later', str(ctx.exception))
        self.assertIn('3.11', str(ctx.exception))


class TestCalcUpgradeSourcesAsync(unittest.IsolatedAsyncioTestCase):
    """Test the calc_upgrade_sources_async function"""

    def _make_suggestions(self, data=None):
        if data is None:
            data = {
                "default": {
                    "minor_min": "4.22.0-rc.0",
                    "minor_max": "4.22.9999",
                    "minor_block_list": [],
                    "z_min": "5.0.0-ec.0",
                    "z_max": "5.0.9999",
                    "z_block_list": [],
                },
            }
        return BuildSuggestions.model_validate(data)

    @patch("artcommonlib.ocp_version_ancestry.get_channel_versions_async")
    @patch("artcommonlib.ocp_version_ancestry.get_build_suggestions_async")
    async def test_5_0_queries_4_22_channel(self, mock_suggestions, mock_channel):
        """5.0 should query candidate-4.22 (not candidate-5.-1)"""
        mock_suggestions.return_value = self._make_suggestions()
        mock_channel.side_effect = [
            # First call: candidate-4.22
            (['4.22.2', '4.22.1', '4.22.0'], {}),
            # Second call: candidate-5.0
            (['5.0.0-rc.0', '5.0.0-ec.1', '5.0.0-ec.0'], {}),
        ]

        result = await calc_upgrade_sources_async("5.0.0-rc.0", "x86_64")

        # Verify correct channels were queried
        calls = mock_channel.call_args_list
        self.assertEqual(calls[0][0][0], 'candidate-4.22')
        self.assertEqual(calls[1][0][0], 'candidate-5.0')

        # All 4.22 versions >= minor_min should be included
        self.assertIn('4.22.0', result)
        self.assertIn('4.22.1', result)
        self.assertIn('4.22.2', result)

        # z-stream versions >= z_min should be included
        self.assertIn('5.0.0-ec.0', result)
        self.assertIn('5.0.0-ec.1', result)

    @patch("artcommonlib.ocp_version_ancestry.get_channel_versions_async")
    @patch("artcommonlib.ocp_version_ancestry.get_build_suggestions_async")
    async def test_standard_minor_bump_4_18(self, mock_suggestions, mock_channel):
        """Standard 4.18 should query candidate-4.17"""
        mock_suggestions.return_value = self._make_suggestions(
            {
                "default": {
                    "minor_min": "4.17.11",
                    "minor_max": "4.17.9999",
                    "minor_block_list": [],
                    "z_min": "4.18.0",
                    "z_max": "4.18.9999",
                    "z_block_list": [],
                },
            }
        )
        mock_channel.side_effect = [
            (['4.17.12', '4.17.11', '4.17.10'], {}),
            (['4.18.1', '4.18.0'], {}),
        ]

        result = await calc_upgrade_sources_async("4.18.2", "x86_64")

        calls = mock_channel.call_args_list
        self.assertEqual(calls[0][0][0], 'candidate-4.17')
        self.assertIn('4.17.11', result)
        self.assertIn('4.17.12', result)
        # 4.17.10 < minor_min (4.17.11), should be excluded
        self.assertNotIn('4.17.10', result)

    @patch("artcommonlib.ocp_version_ancestry.get_channel_versions_async")
    @patch("artcommonlib.ocp_version_ancestry.get_build_suggestions_async")
    async def test_block_list_excludes_versions(self, mock_suggestions, mock_channel):
        """Versions in block lists should be excluded"""
        mock_suggestions.return_value = self._make_suggestions(
            {
                "default": {
                    "minor_min": "4.22.0",
                    "minor_max": "4.22.9999",
                    "minor_block_list": ["4.22.1"],
                    "z_min": "5.0.0-ec.0",
                    "z_max": "5.0.9999",
                    "z_block_list": ["5.0.0-ec.1"],
                },
            }
        )
        mock_channel.side_effect = [
            (['4.22.2', '4.22.1', '4.22.0'], {}),
            (['5.0.0-ec.1', '5.0.0-ec.0'], {}),
        ]

        result = await calc_upgrade_sources_async("5.0.0-rc.0", "x86_64")

        self.assertIn('4.22.0', result)
        self.assertNotIn('4.22.1', result)  # blocked
        self.assertIn('4.22.2', result)
        self.assertIn('5.0.0-ec.0', result)
        self.assertNotIn('5.0.0-ec.1', result)  # blocked

    @patch("artcommonlib.ocp_version_ancestry.get_channel_versions_async")
    @patch("artcommonlib.ocp_version_ancestry.get_build_suggestions_async")
    async def test_hotfix_included_for_standard_release(self, mock_suggestions, mock_channel):
        """Hotfixes with < 2 outgoing edges should be included for standard releases"""
        mock_suggestions.return_value = self._make_suggestions()
        mock_channel.side_effect = [
            (['4.22.0'], {}),
            (
                ['5.0.0-ec.0', '5.0.0-0.hotfix-2024-09-30-133631'],
                {'5.0.0-ec.0': [], '5.0.0-0.hotfix-2024-09-30-133631': ['5.0.0-ec.0']},  # 1 edge < 2
            ),
        ]

        result = await calc_upgrade_sources_async("5.0.1", "x86_64")

        self.assertIn('5.0.0-0.hotfix-2024-09-30-133631', result)

    @patch("artcommonlib.ocp_version_ancestry.get_channel_versions_async")
    @patch("artcommonlib.ocp_version_ancestry.get_build_suggestions_async")
    async def test_hotfix_excluded_with_2_edges(self, mock_suggestions, mock_channel):
        """Hotfixes with >= 2 outgoing edges should NOT be added by step 7"""
        mock_suggestions.return_value = self._make_suggestions()
        mock_channel.side_effect = [
            (['4.22.0'], {}),
            (
                ['5.0.0-ec.0', '5.0.0-0.hotfix-2024-09-30-133631'],
                {
                    '5.0.0-ec.0': [],
                    '5.0.0-0.hotfix-2024-09-30-133631': ['5.0.0-ec.0', '5.0.1'],  # 2 edges
                },
            ),
        ]

        result = await calc_upgrade_sources_async("5.0.2", "x86_64")

        self.assertNotIn('5.0.0-0.hotfix-2024-09-30-133631', result)

    @patch("artcommonlib.ocp_version_ancestry.get_channel_versions_async")
    @patch("artcommonlib.ocp_version_ancestry.get_build_suggestions_async")
    async def test_hotfix_in_block_list_not_readded(self, mock_suggestions, mock_channel):
        """Step 7 must not re-add hotfixes excluded by z_block_list"""
        mock_suggestions.return_value = self._make_suggestions(
            {
                "default": {
                    "minor_min": "4.22.0",
                    "minor_max": "4.22.9999",
                    "minor_block_list": [],
                    "z_min": "5.0.0-ec.0",
                    "z_max": "5.0.9999",
                    "z_block_list": ["5.0.0-0.hotfix-2024-09-30-133631"],
                },
            }
        )
        mock_channel.side_effect = [
            (['4.22.0'], {}),
            (
                ['5.0.0-ec.0', '5.0.0-0.hotfix-2024-09-30-133631'],
                {'5.0.0-0.hotfix-2024-09-30-133631': ['5.0.0-ec.0']},
            ),
        ]

        result = await calc_upgrade_sources_async("5.0.1", "x86_64")

        self.assertNotIn('5.0.0-0.hotfix-2024-09-30-133631', result)

    @patch("artcommonlib.ocp_version_ancestry.get_channel_versions_async")
    @patch("artcommonlib.ocp_version_ancestry.get_build_suggestions_async")
    async def test_hotfix_edges_count_only_standard_targets(self, mock_suggestions, mock_channel):
        """Step 7 should count only edges to standard releases toward the 2-edge limit"""
        mock_suggestions.return_value = self._make_suggestions()
        mock_channel.side_effect = [
            (['4.22.0'], {}),
            (
                [
                    '5.0.0-ec.0',
                    '5.0.0-0.hotfix-2024-09-30-133631',
                    '5.0.0-0.hotfix-2024-09-29-120000',
                    '5.0.0-nightly-2024-09-28-010101',
                ],
                {
                    '5.0.0-0.hotfix-2024-09-30-133631': [
                        '5.0.0-ec.0',
                        '5.0.0-0.hotfix-2024-09-29-120000',
                        '5.0.0-nightly-2024-09-28-010101',
                    ],
                },
            ),
        ]

        result = await calc_upgrade_sources_async("5.0.1", "x86_64")

        self.assertIn('5.0.0-0.hotfix-2024-09-30-133631', result)

    @patch("artcommonlib.ocp_version_ancestry.get_channel_versions_async")
    @patch("artcommonlib.ocp_version_ancestry.get_build_suggestions_async")
    async def test_hotfix_not_included_for_hotfix_release(self, mock_suggestions, mock_channel):
        """When calculating for a hotfix release, don't include other hotfixes"""
        mock_suggestions.return_value = self._make_suggestions()
        mock_channel.side_effect = [
            (['4.22.0'], {}),
            (
                ['5.0.0-ec.0', '5.0.0-0.hotfix-2024-09-30-133631'],
                {'5.0.0-ec.0': [], '5.0.0-0.hotfix-2024-09-30-133631': []},
            ),
        ]

        result = await calc_upgrade_sources_async("5.0.0-0.hotfix-2024-10-01-120000", "x86_64")

        self.assertNotIn('5.0.0-0.hotfix-2024-09-30-133631', result)

    @patch("artcommonlib.ocp_version_ancestry.get_channel_versions_async")
    @patch("artcommonlib.ocp_version_ancestry.get_build_suggestions_async")
    async def test_result_sorted_descending(self, mock_suggestions, mock_channel):
        """Result should be sorted in descending semver order"""
        mock_suggestions.return_value = self._make_suggestions()
        mock_channel.side_effect = [
            (['4.22.0', '4.22.1', '4.22.2'], {}),
            (['5.0.0-ec.0', '5.0.0-ec.1'], {}),
        ]

        result = await calc_upgrade_sources_async("5.0.0-rc.0", "x86_64")

        self.assertEqual(result, ['5.0.0-ec.1', '5.0.0-ec.0', '4.22.2', '4.22.1', '4.22.0'])

    @patch("artcommonlib.ocp_version_ancestry.get_channel_versions_async")
    @patch("artcommonlib.ocp_version_ancestry.get_build_suggestions_async")
    async def test_no_max_includes_same_minor(self, mock_suggestions, mock_channel):
        """When minor_max/z_max are omitted, include all versions >= min with same major.minor"""
        mock_suggestions.return_value = self._make_suggestions(
            {
                "default": {
                    "minor_min": "4.22.0-rc.0",
                    # minor_max omitted
                    "minor_block_list": [],
                    "z_min": "5.0.0-ec.0",
                    # z_max omitted
                    "z_block_list": [],
                },
            }
        )
        mock_channel.side_effect = [
            # candidate-4.22: includes a mix of versions
            (['4.22.0', '4.22.1', '4.22.9999'], {}),
            # candidate-5.0
            (['5.0.0-ec.0', '5.0.0-rc.0', '5.0.9999'], {}),
        ]

        result = await calc_upgrade_sources_async("5.0.0-rc.0", "x86_64")

        # All 4.22 versions >= minor_min should be included (no upper bound)
        self.assertIn('4.22.0', result)
        self.assertIn('4.22.1', result)
        self.assertIn('4.22.9999', result)

        # All 5.0 versions >= z_min should be included (no upper bound)
        self.assertIn('5.0.0-ec.0', result)
        self.assertIn('5.0.9999', result)


if __name__ == "__main__":
    unittest.main()
