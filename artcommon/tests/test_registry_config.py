"""
Test the RegistryConfig class. Verify that it works as a context manager
for merging container registry credentials from multiple sources.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from artcommonlib.registry_config import RegistryConfig


class RegistryConfigTestCase(unittest.TestCase):
    """
    Test the features of the RegistryConfig class. This is a context manager
    for merging container registry credentials from multiple auth files.
    """

    def setUp(self):
        """
        Set up test fixtures
        """
        self.test_dir = tempfile.mkdtemp()
        self.default_auth = {
            'auths': {
                'quay.io/openshift-release-dev': {'auth': 'token1'},
                'quay.io/redhat-user-workloads/test': {'auth': 'token2'},
                'registry.ci.openshift.org': {'auth': 'token3'},
            }
        }
        self.additional_auth = {
            'auths': {
                'quay.io/redhat-user-workloads/konflux': {'auth': 'konflux-token'},
                'quay.io/openshift': {'auth': 'openshift-token'},
            }
        }

    def tearDown(self):
        """
        Clean up test fixtures
        """
        # Clean up any leftover temp files in current directory
        for file in Path('.').glob('registry-auth-*.json'):
            try:
                file.unlink()
            except:
                pass

    def test_init_default_values(self):
        """
        Verify default initialization values
        """
        with patch('os.path.exists', return_value=False):
            config = RegistryConfig()
            self.assertIsNone(config.additional_sources)
            self.assertIsNone(config.filter_patterns)
            self.assertIsNotNone(config.default_auth_file)
            self.assertIn('containers/auth.json', config.default_auth_file)

    def test_init_custom_auth_file(self):
        """
        Verify custom default_auth_file path is used
        """
        custom_path = '/custom/path/auth.json'
        with patch('os.path.exists', return_value=False):
            config = RegistryConfig(default_auth_file=custom_path)
            self.assertEqual(config.default_auth_file, custom_path)

    def test_init_custom_filter_patterns(self):
        """
        Verify custom filter_patterns are stored
        """
        patterns = ['quay.io/test/', 'registry.example.com/']
        with patch('os.path.exists', return_value=False):
            config = RegistryConfig(filter_patterns=patterns)
            self.assertEqual(config.filter_patterns, patterns)

    def test_context_manager_creates_temp_file(self):
        """
        Verify that context manager creates a temporary auth file
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        with RegistryConfig(default_auth_file=default_auth_file) as auth_file:
            # Verify temp file exists
            self.assertTrue(os.path.exists(auth_file))
            # Verify filename pattern (may or may not have ./ prefix)
            basename = os.path.basename(auth_file)
            self.assertTrue(basename.startswith('registry-auth-'))
            self.assertTrue(basename.endswith('.json'))

            # Verify temp file contains credentials
            with open(auth_file, 'r') as f:
                content = json.load(f)
                self.assertIn('auths', content)

        # Verify temp file is cleaned up after exit
        self.assertFalse(os.path.exists(auth_file))

    def test_context_manager_cleanup_on_exception(self):
        """
        Verify that temporary file is cleaned up even when exception occurs
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        temp_file_path = None
        try:
            with RegistryConfig(default_auth_file=default_auth_file) as auth_file:
                temp_file_path = auth_file
                self.assertTrue(os.path.exists(auth_file))
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Verify temp file is cleaned up despite exception
        self.assertFalse(os.path.exists(temp_file_path))

    def test_read_existing_credentials_no_filtering(self):
        """
        Verify that all credentials are read when no filter_patterns is provided
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        with RegistryConfig(default_auth_file=default_auth_file) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # All 3 credentials should be present
                self.assertEqual(len(content['auths']), 3)
                self.assertIn('quay.io/openshift-release-dev', content['auths'])
                self.assertIn('quay.io/redhat-user-workloads/test', content['auths'])
                self.assertIn('registry.ci.openshift.org', content['auths'])

    def test_read_existing_credentials_with_filtering(self):
        """
        Verify that credentials are filtered when filter_patterns is provided
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        filter_patterns = ['quay.io/redhat-user-workloads/']

        with RegistryConfig(default_auth_file=default_auth_file, filter_patterns=filter_patterns) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # Only 1 credential should match the filter
                self.assertEqual(len(content['auths']), 1)
                self.assertIn('quay.io/redhat-user-workloads/test', content['auths'])
                self.assertNotIn('quay.io/openshift-release-dev', content['auths'])
                self.assertNotIn('registry.ci.openshift.org', content['auths'])

    def test_read_existing_credentials_multiple_patterns(self):
        """
        Verify that credentials match any of multiple filter patterns
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        filter_patterns = ['quay.io/redhat-user-workloads/', 'registry.ci.openshift.org']

        with RegistryConfig(default_auth_file=default_auth_file, filter_patterns=filter_patterns) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # 2 credentials should match
                self.assertEqual(len(content['auths']), 2)
                self.assertIn('quay.io/redhat-user-workloads/test', content['auths'])
                self.assertIn('registry.ci.openshift.org', content['auths'])
                self.assertNotIn('quay.io/openshift-release-dev', content['auths'])

    def test_no_existing_credentials(self):
        """
        Verify behavior when default auth file does not exist
        """
        non_existent_file = os.path.join(self.test_dir, 'nonexistent.json')

        with RegistryConfig(default_auth_file=non_existent_file) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # Should have empty auths
                self.assertEqual(content['auths'], {})

    def test_add_additional_credentials_no_sources(self):
        """
        Verify behavior when additional_sources is None
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        with RegistryConfig(default_auth_file=default_auth_file, additional_sources=None) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # Should only have default credentials
                self.assertEqual(len(content['auths']), 3)

    @patch.dict(os.environ, {'KONFLUX_AUTH': '/path/to/konflux.json'})
    def test_add_additional_credentials_with_filtering(self):
        """
        Verify that additional credentials are added and filtered
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        additional_auth_file = os.path.join(self.test_dir, 'konflux.json')
        with open(additional_auth_file, 'w') as f:
            json.dump(self.additional_auth, f)

        with patch.dict(os.environ, {'KONFLUX_AUTH': additional_auth_file}):
            with RegistryConfig(
                default_auth_file=default_auth_file,
                additional_sources=['KONFLUX_AUTH'],
                filter_patterns=['quay.io/redhat-user-workloads/'],
            ) as auth_file:
                with open(auth_file, 'r') as f:
                    content = json.load(f)
                    # Should have 2 credentials: one from default, one from additional
                    self.assertEqual(len(content['auths']), 2)
                    self.assertIn('quay.io/redhat-user-workloads/test', content['auths'])
                    self.assertIn('quay.io/redhat-user-workloads/konflux', content['auths'])

    @patch.dict(os.environ, {'KONFLUX_AUTH': '/path/to/konflux.json'})
    def test_add_additional_credentials_no_filtering(self):
        """
        Verify that all additional credentials are added when no filtering
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        additional_auth_file = os.path.join(self.test_dir, 'konflux.json')
        with open(additional_auth_file, 'w') as f:
            json.dump(self.additional_auth, f)

        with patch.dict(os.environ, {'KONFLUX_AUTH': additional_auth_file}):
            with RegistryConfig(
                default_auth_file=default_auth_file,
                additional_sources=['KONFLUX_AUTH'],
                filter_patterns=None,  # No filtering
            ) as auth_file:
                with open(auth_file, 'r') as f:
                    content = json.load(f)
                    # Should have all 5 credentials (3 from default + 2 from additional)
                    self.assertEqual(len(content['auths']), 5)

    def test_add_additional_credentials_env_var_not_set(self):
        """
        Verify behavior when environment variable for additional source is not set
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        # Ensure env var is not set
        if 'NONEXISTENT_AUTH' in os.environ:
            del os.environ['NONEXISTENT_AUTH']

        with RegistryConfig(default_auth_file=default_auth_file, additional_sources=['NONEXISTENT_AUTH']) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # Should only have default credentials
                self.assertEqual(len(content['auths']), 3)

    @patch.dict(os.environ, {'KONFLUX_AUTH': '/nonexistent/file.json'})
    def test_add_additional_credentials_file_not_found(self):
        """
        Verify behavior when additional auth file does not exist
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        with RegistryConfig(default_auth_file=default_auth_file, additional_sources=['KONFLUX_AUTH']) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # Should only have default credentials
                self.assertEqual(len(content['auths']), 3)

    def test_get_registries(self):
        """
        Verify get_registries returns list of registry names
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        config = RegistryConfig(default_auth_file=default_auth_file)
        with config:
            registries = config.get_registries()
            self.assertEqual(len(registries), 3)
            self.assertIn('quay.io/openshift-release-dev', registries)
            self.assertIn('quay.io/redhat-user-workloads/test', registries)
            self.assertIn('registry.ci.openshift.org', registries)

    def test_has_credential_for_exact_match(self):
        """
        Verify has_credential_for returns True for exact match
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        config = RegistryConfig(default_auth_file=default_auth_file)
        with config:
            self.assertTrue(config.has_credential_for('quay.io/openshift-release-dev'))
            self.assertTrue(config.has_credential_for('quay.io/redhat-user-workloads/test'))
            self.assertTrue(config.has_credential_for('registry.ci.openshift.org'))

    def test_has_credential_for_prefix_match(self):
        """
        Verify has_credential_for returns True for prefix match
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        config = RegistryConfig(default_auth_file=default_auth_file)
        with config:
            # Should match because 'quay.io/redhat-user-workloads/test/image' starts with registry
            self.assertTrue(config.has_credential_for('quay.io/redhat-user-workloads/test/image'))
            # Should match because registry starts with search term
            self.assertTrue(config.has_credential_for('quay.io/openshift'))

    def test_has_credential_for_no_match(self):
        """
        Verify has_credential_for returns False when no match
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        config = RegistryConfig(default_auth_file=default_auth_file)
        with config:
            self.assertFalse(config.has_credential_for('docker.io'))
            self.assertFalse(config.has_credential_for('gcr.io'))

    def test_temp_file_created_in_current_directory(self):
        """
        Verify that temporary auth file is created in current directory
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        with RegistryConfig(default_auth_file=default_auth_file) as auth_file:
            # Verify filename pattern
            basename = os.path.basename(auth_file)
            self.assertTrue(basename.startswith('registry-auth-'))
            # Verify the path is in current directory
            abs_path = os.path.abspath(auth_file)
            current_dir = os.getcwd()
            self.assertTrue(abs_path.startswith(current_dir))
            # Verify it's directly in current directory (not in subdirectory)
            self.assertEqual(os.path.dirname(abs_path), current_dir)

    def test_merged_credentials_format(self):
        """
        Verify that merged credentials have correct JSON format
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        with RegistryConfig(default_auth_file=default_auth_file) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # Verify top-level structure
                self.assertIn('auths', content)
                self.assertIsInstance(content['auths'], dict)
                # Verify each auth entry has expected structure
                for registry, creds in content['auths'].items():
                    self.assertIsInstance(registry, str)
                    self.assertIsInstance(creds, dict)
                    self.assertIn('auth', creds)

    def test_invalid_json_in_default_auth_file(self):
        """
        Verify that invalid JSON in default auth file raises exception
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            f.write('invalid json content {]')

        with self.assertRaises(json.JSONDecodeError):
            with RegistryConfig(default_auth_file=default_auth_file):
                pass

    @patch.dict(os.environ, {'KONFLUX_AUTH': '/path/to/konflux.json'})
    def test_invalid_json_in_additional_auth_file(self):
        """
        Verify that invalid JSON in additional auth file raises exception
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        additional_auth_file = os.path.join(self.test_dir, 'konflux.json')
        with open(additional_auth_file, 'w') as f:
            f.write('invalid json {]')

        with patch.dict(os.environ, {'KONFLUX_AUTH': additional_auth_file}):
            with self.assertRaises(json.JSONDecodeError):
                with RegistryConfig(default_auth_file=default_auth_file, additional_sources=['KONFLUX_AUTH']):
                    pass

    def test_empty_default_auth_file(self):
        """
        Verify behavior with empty default auth file
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump({'auths': {}}, f)

        with RegistryConfig(default_auth_file=default_auth_file) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                self.assertEqual(content['auths'], {})

    def test_credential_override_from_additional_source(self):
        """
        Verify that credentials from additional sources override default ones
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump({'auths': {'quay.io/redhat-user-workloads/test': {'auth': 'old-token'}}}, f)

        additional_auth_file = os.path.join(self.test_dir, 'konflux.json')
        with open(additional_auth_file, 'w') as f:
            json.dump({'auths': {'quay.io/redhat-user-workloads/test': {'auth': 'new-token'}}}, f)

        with patch.dict(os.environ, {'KONFLUX_AUTH': additional_auth_file}):
            with RegistryConfig(default_auth_file=default_auth_file, additional_sources=['KONFLUX_AUTH']) as auth_file:
                with open(auth_file, 'r') as f:
                    content = json.load(f)
                    # Additional source should override default
                    self.assertEqual(content['auths']['quay.io/redhat-user-workloads/test']['auth'], 'new-token')

    def test_registries_parameter_all_present(self):
        """
        Verify that registries parameter validates successfully when all registries are present
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        required_registries = ['quay.io/openshift-release-dev', 'registry.ci.openshift.org']

        with RegistryConfig(default_auth_file=default_auth_file, registries=required_registries) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # Should have all default credentials (no filtering)
                self.assertEqual(len(content['auths']), 3)
                # Required registries should be present
                for registry in required_registries:
                    self.assertIn(registry, content['auths'])

    def test_registries_parameter_missing_registry(self):
        """
        Verify that registries parameter raises ValueError when required registry is missing
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        required_registries = ['quay.io/openshift-release-dev', 'registry.does-not-exist.com']

        with self.assertRaises(ValueError) as context:
            with RegistryConfig(default_auth_file=default_auth_file, registries=required_registries):
                pass

        # Verify error message contains missing registry
        self.assertIn('registry.does-not-exist.com', str(context.exception))
        self.assertIn('Required registries not found', str(context.exception))

    def test_registries_parameter_ignores_filter_patterns(self):
        """
        Verify that filter_patterns is ignored when registries parameter is provided
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        required_registries = ['quay.io/openshift-release-dev']
        filter_patterns = ['registry.ci.openshift.org']  # Different pattern

        with RegistryConfig(
            default_auth_file=default_auth_file, registries=required_registries, filter_patterns=filter_patterns
        ) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # Should have all 3 credentials (filter_patterns should be ignored)
                self.assertEqual(len(content['auths']), 3)
                self.assertIn('quay.io/openshift-release-dev', content['auths'])
                self.assertIn('quay.io/redhat-user-workloads/test', content['auths'])
                self.assertIn('registry.ci.openshift.org', content['auths'])

    @patch.dict(os.environ, {'KONFLUX_AUTH': '/path/to/konflux.json'})
    def test_registries_parameter_with_additional_sources(self):
        """
        Verify that registries parameter works with additional sources and validates across all sources
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        additional_auth_file = os.path.join(self.test_dir, 'konflux.json')
        with open(additional_auth_file, 'w') as f:
            json.dump(self.additional_auth, f)

        # Require registries from both sources
        required_registries = ['quay.io/openshift-release-dev', 'quay.io/redhat-user-workloads/konflux']

        with patch.dict(os.environ, {'KONFLUX_AUTH': additional_auth_file}):
            with RegistryConfig(
                default_auth_file=default_auth_file,
                additional_sources=['KONFLUX_AUTH'],
                registries=required_registries,
            ) as auth_file:
                with open(auth_file, 'r') as f:
                    content = json.load(f)
                    # Should have all credentials from both sources (no filtering)
                    self.assertEqual(len(content['auths']), 5)
                    # Required registries should be present
                    for registry in required_registries:
                        self.assertIn(registry, content['auths'])

    @patch.dict(os.environ, {'KONFLUX_AUTH': '/path/to/konflux.json'})
    def test_registries_parameter_missing_from_additional_source(self):
        """
        Verify that ValueError is raised when required registry is missing even with additional sources
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        additional_auth_file = os.path.join(self.test_dir, 'konflux.json')
        with open(additional_auth_file, 'w') as f:
            json.dump(self.additional_auth, f)

        # Require a registry that doesn't exist in either source
        required_registries = ['registry.does-not-exist.com']

        with patch.dict(os.environ, {'KONFLUX_AUTH': additional_auth_file}):
            with self.assertRaises(ValueError) as context:
                with RegistryConfig(
                    default_auth_file=default_auth_file,
                    additional_sources=['KONFLUX_AUTH'],
                    registries=required_registries,
                ):
                    pass

            self.assertIn('registry.does-not-exist.com', str(context.exception))

    def test_registries_parameter_empty_list(self):
        """
        Verify that empty registries list works (no validation)
        """
        default_auth_file = os.path.join(self.test_dir, 'auth.json')
        with open(default_auth_file, 'w') as f:
            json.dump(self.default_auth, f)

        with RegistryConfig(default_auth_file=default_auth_file, registries=[]) as auth_file:
            with open(auth_file, 'r') as f:
                content = json.load(f)
                # Should have all credentials (no filtering, no validation)
                self.assertEqual(len(content['auths']), 3)


if __name__ == "__main__":
    unittest.main()
