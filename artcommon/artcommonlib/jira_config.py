#!/usr/bin/env python3

"""
Centralized JIRA configuration for all ART tools (doozer, elliott, pyartcd).

This module provides a single source of truth for JIRA-related URLs and settings.
When migrating between JIRA instances (e.g., from issues.redhat.com to Atlassian Cloud),
only this file needs to be updated.

Environment Variables:
    JIRA_SERVER_URL: Override the default JIRA server URL
    JIRA_TOKEN: Authentication token for JIRA API access (required)

Example usage:
    from artcommonlib.jira_config import JIRA_SERVER_URL, get_jira_browse_url

    # Get the base server URL
    server_url = JIRA_SERVER_URL

    # Get a browse URL for an issue
    browse_url = get_jira_browse_url("OCPBUGS-12345")
"""

import os

# Default JIRA server URL
# This can be overridden by setting the JIRA_SERVER_URL environment variable
DEFAULT_JIRA_SERVER_URL = "https://issues.redhat.com"

# Get the JIRA server URL (check environment variable first, then use default)
JIRA_SERVER_URL = os.environ.get("JIRA_SERVER_URL", DEFAULT_JIRA_SERVER_URL)

# JIRA API endpoints
JIRA_API_BASE = f"{JIRA_SERVER_URL}/rest/api/2"
JIRA_API_FIELD = f"{JIRA_API_BASE}/field"

# Legacy domain name (without https://) for backward compatibility
# Used in some places for domain comparisons
JIRA_DOMAIN_NAME = JIRA_SERVER_URL.replace("https://", "").replace("http://", "").rstrip("/")

# JIRA security levels allowlist
JIRA_SECURITY_ALLOWLIST = {
    "Red Hat Employee",
    "Restricted",
    "Red Hat Partner",
    "Red Hat Engineering Authorized",
    "Embargoed Security Issue",
}


def get_jira_browse_url(issue_key: str) -> str:
    """
    Get the full browse URL for a JIRA issue.

    Arg(s):
        issue_key (str): The JIRA issue key (e.g., "OCPBUGS-12345")

    Return Value(s):
        str: The full URL to browse the issue
    """
    return f"{JIRA_SERVER_URL}/browse/{issue_key}"


def get_jira_api_url(endpoint: str) -> str:
    """
    Get the full API URL for a JIRA endpoint.

    Arg(s):
        endpoint (str): The API endpoint path (e.g., "issue/OCPBUGS-12345")

    Return Value(s):
        str: The full API URL
    """
    endpoint = endpoint.lstrip("/")
    return f"{JIRA_API_BASE}/{endpoint}"
