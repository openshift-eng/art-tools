#!/usr/bin/env python3

"""
Tests for centralized JIRA configuration module.
"""

import os
import unittest
from unittest.mock import Mock

from artcommonlib import jira_config


class TestJiraConfig(unittest.TestCase):
    """
    Test the centralized JIRA configuration module.
    """

    def test_default_jira_server_url(self):
        """
        Test that the default JIRA server URL is set correctly.
        """
        # When JIRA_SERVER_URL env var is not set, should use default (hardcoded, not from bug.yml)
        self.assertEqual(jira_config.DEFAULT_JIRA_SERVER_URL, "https://redhat.atlassian.net")

    def test_jira_api_field_url(self):
        """
        Test that JIRA API field URL is constructed correctly.
        """
        # Should be based on JIRA_SERVER_URL
        self.assertTrue(jira_config.JIRA_API_FIELD.endswith("/rest/api/2/field"))

    def test_jira_domain_name(self):
        """
        Test that JIRA domain name is extracted correctly.
        """
        # Should strip protocol and trailing slash
        self.assertNotIn("https://", jira_config.JIRA_DOMAIN_NAME)
        self.assertNotIn("http://", jira_config.JIRA_DOMAIN_NAME)
        self.assertFalse(jira_config.JIRA_DOMAIN_NAME.endswith("/"))

    def test_get_jira_browse_url(self):
        """
        Test browse URL generation.
        """
        issue_key = "OCPBUGS-12345"
        browse_url = jira_config.get_jira_browse_url(issue_key)

        # Should contain the issue key
        self.assertIn(issue_key, browse_url)
        # Should contain /browse/
        self.assertIn("/browse/", browse_url)
        # Should start with the server URL
        self.assertTrue(browse_url.startswith(jira_config.JIRA_SERVER_URL))

    def test_get_jira_api_url(self):
        """
        Test API URL generation.
        """
        endpoint = "issue/OCPBUGS-12345"
        api_url = jira_config.get_jira_api_url(endpoint)

        # Should contain the endpoint
        self.assertIn("OCPBUGS-12345", api_url)
        # Should contain /rest/api/2/
        self.assertIn("/rest/api/2/", api_url)
        # Should start with the server URL
        self.assertTrue(api_url.startswith(jira_config.JIRA_SERVER_URL))

    def test_get_jira_api_url_strips_leading_slash(self):
        """
        Test that API URL generation handles leading slashes correctly.
        """
        # Both forms should produce the same result
        url1 = jira_config.get_jira_api_url("issue/OCPBUGS-123")
        url2 = jira_config.get_jira_api_url("/issue/OCPBUGS-123")

        self.assertEqual(url1, url2)

    def test_jira_security_allowlist(self):
        """
        Test that security allowlist contains expected values.
        """
        expected_security_levels = {
            "Red Hat Employee",
            "Restricted",
            "Red Hat Partner",
            "Red Hat Engineering Authorized",
            "Embargoed Security Issue",
        }

        self.assertEqual(jira_config.JIRA_SECURITY_ALLOWLIST, expected_security_levels)

    def test_env_var_override(self):
        """
        Test that JIRA_SERVER_URL environment variable can override the default.
        """
        # Note: This test verifies the module respects environment variables
        # The actual override happens at module import time, so we can only
        # check if the environment variable is being read

        # Save original value
        original_env = os.environ.get("JIRA_SERVER_URL")

        try:
            # Set a custom value
            test_url = "https://test.atlassian.net"
            os.environ["JIRA_SERVER_URL"] = test_url

            # Reload the module to pick up the env var
            import importlib

            importlib.reload(jira_config)

            # Should now use the custom URL
            self.assertEqual(jira_config.JIRA_SERVER_URL, test_url)

        finally:
            # Restore original value
            if original_env is not None:
                os.environ["JIRA_SERVER_URL"] = original_env
            elif "JIRA_SERVER_URL" in os.environ:
                del os.environ["JIRA_SERVER_URL"]

            # Reload module to restore original state
            import importlib

            importlib.reload(jira_config)

    def test_verify_jira_client_success(self):
        """
        Test that verify_jira_client returns username on success.
        """
        # Create a mock JIRA client that succeeds
        mock_client = Mock()
        mock_client.current_user.return_value = "test-user@redhat.com"

        result = jira_config.verify_jira_client(mock_client)

        self.assertEqual(result, "test-user@redhat.com")
        mock_client.current_user.assert_called_once()

    def test_verify_jira_client_auth_failure(self):
        """
        Test that verify_jira_client raises ValueError on 401 error.
        """
        from jira import JIRAError

        # Create a mock JIRA client that returns 401
        mock_client = Mock()
        mock_error = JIRAError(status_code=401, text="Unauthorized")
        mock_client.current_user.side_effect = mock_error

        with self.assertRaises(ValueError) as context:
            jira_config.verify_jira_client(mock_client)

        error_message = str(context.exception)
        self.assertIn("matches your JIRA_TOKEN", error_message)

    def test_verify_jira_client_other_error(self):
        """
        Test that verify_jira_client re-raises non-401 JIRA errors.
        """
        from jira import JIRAError

        # Create a mock JIRA client that returns 500
        mock_client = Mock()
        mock_error = JIRAError(status_code=500, text="Internal Server Error")
        mock_client.current_user.side_effect = mock_error

        with self.assertRaises(JIRAError) as context:
            jira_config.verify_jira_client(mock_client)

        # Should re-raise the original error
        self.assertEqual(context.exception.status_code, 500)

    def test_verify_jira_client_chains_exception(self):
        """
        Test that ValueError chains the original JIRAError as __cause__.
        """
        from jira import JIRAError

        mock_client = Mock()
        mock_error = JIRAError(status_code=401, text="Unauthorized")
        mock_client.current_user.side_effect = mock_error

        with self.assertRaises(ValueError) as context:
            jira_config.verify_jira_client(mock_client)

        # Should chain the original exception
        self.assertIsInstance(context.exception.__cause__, JIRAError)
        self.assertEqual(context.exception.__cause__.status_code, 401)


if __name__ == "__main__":
    unittest.main()
