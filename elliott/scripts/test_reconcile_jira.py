#!/usr/bin/env python3

"""
Standalone script to invoke reconcile_jira_issues() from images_streams.py
with mock runtime and PR objects but a real JIRA connection.

This lets you exercise the customfield code path without running the full
doozer images:streams prs open pipeline.

Requirements:
    - JIRA_TOKEN environment variable
    - JIRA_USER environment variable (for Atlassian Cloud)
    - JIRA_SERVER_URL environment variable (optional, defaults to jira_config)

Examples:
    # Dry run — shows what JIRA issue would be created (no customfield updates)
    uv run python ./elliott/scripts/test_reconcile_jira.py --dry-run

    # Live run — creates a real JIRA issue and updates customfields
    uv run python ./elliott/scripts/test_reconcile_jira.py

    # Specify project/component/version
    uv run python ./elliott/scripts/test_reconcile_jira.py \
        --project OCPBUGS --component Release --major 4 --minor 18

    # Clean up — delete the JIRA issue created by a previous run
    uv run python ./elliott/scripts/test_reconcile_jira.py --cleanup OCPBUGS-99999
"""

import argparse
import logging
import os
import sys
from unittest.mock import MagicMock

from artcommonlib.jira_config import JIRA_SERVER_URL
from jira import JIRA

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def _build_jira_client() -> JIRA:
    """
    Build an authenticated JIRA client using environment variables.

    Return Value(s):
        JIRA: An authenticated JIRA client instance.
    """
    server = os.environ.get("JIRA_SERVER_URL", JIRA_SERVER_URL)
    token = os.environ.get("JIRA_TOKEN")
    if not token:
        print("ERROR: JIRA_TOKEN environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    if 'atlassian' in server:
        username = os.environ.get("JIRA_USER")
        if not username:
            print("ERROR: JIRA_USER environment variable is required for Atlassian Cloud.", file=sys.stderr)
            sys.exit(1)
        client = JIRA(server=server, basic_auth=(username, token))
    else:
        client = JIRA(server, token_auth=token)

    logger.info(f"Connected to JIRA server: {server}")
    return client


def _build_mock_runtime(jira_client: JIRA, major: int, minor: int,
                        project: str, component: str,
                        target_release: str) -> MagicMock:
    """
    Build a mock runtime object with enough structure for reconcile_jira_issues().

    Arg(s):
        jira_client (JIRA): A real authenticated JIRA client.
        major (int): OCP major version.
        minor (int): OCP minor version.
        project (str): JIRA project key.
        component (str): JIRA component name.
        target_release (str): Target version string (e.g., "4.18.z").

    Return Value(s):
        MagicMock: A mock runtime object.
    """
    runtime = MagicMock()
    runtime.get_major_minor_fields.return_value = (major, minor)
    runtime.logger = logger

    # build_jira_client returns the real JIRA client
    runtime.build_jira_client.return_value = jira_client

    # gitdata.load_data for bug.yml target_release
    bug_data = MagicMock()
    bug_data.data = {'target_release': [f'{major}.{minor}.0', target_release]}
    runtime.gitdata.load_data.return_value = bug_data

    # Image metadata mock
    image_meta = MagicMock()
    image_meta.get_jira_info.return_value = (project, component)
    image_meta.get_component_name.return_value = 'test-reconcile-component'
    image_meta.name = 'test-reconcile-image'
    runtime.image_map = {'test-reconcile-image': image_meta}

    return runtime


def _build_mock_pr() -> MagicMock:
    """
    Build a mock GitHub PR object with enough structure for reconcile_jira_issues().

    Return Value(s):
        MagicMock: A mock PR object.
    """
    mock_pr = MagicMock()
    mock_pr.html_url = 'https://github.com/openshift/test-repo/pull/99999'
    mock_pr.title = 'ART test reconciliation PR (test_reconcile_jira.py)'
    mock_pr.get_issue_comments.return_value = []
    return mock_pr


def _run_reconcile(args: argparse.Namespace) -> None:
    """
    Invoke reconcile_jira_issues() with mock objects and a real JIRA connection.

    Arg(s):
        args (argparse.Namespace): Parsed command line arguments.
    """
    # Import here so the script fails fast on env var issues before loading doozer modules
    from doozerlib.cli.images_streams import reconcile_jira_issues

    jira_client = _build_jira_client()

    runtime = _build_mock_runtime(
        jira_client=jira_client,
        major=args.major,
        minor=args.minor,
        project=args.project,
        component=args.component,
        target_release=args.target_release,
    )

    mock_pr = _build_mock_pr()
    pr_map = {'test-reconcile-image': (mock_pr, None)}

    dry_run = args.dry_run

    logger.info(f"Invoking reconcile_jira_issues(dry_run={dry_run})")
    logger.info(f"  Project: {args.project}")
    logger.info(f"  Component: {args.component}")
    logger.info(f"  Version: {args.major}.{args.minor}")
    logger.info(f"  Target Release: {args.target_release}")

    if not dry_run:
        logger.warning("THIS WILL CREATE A REAL JIRA ISSUE. Use --dry-run to preview.")
        response = input("Continue? [y/N] ")
        if response.lower() != 'y':
            logger.info("Aborted.")
            return

    reconcile_jira_issues(runtime, pr_map, dry_run)

    logger.info("reconcile_jira_issues() completed.")

    if not dry_run:
        logger.info("Check your JIRA project for the newly created issue.")
        logger.info("Use --cleanup <ISSUE_KEY> to delete it when done testing.")


def _cleanup(args: argparse.Namespace) -> None:
    """
    Delete a JIRA issue created by a previous test run.

    Arg(s):
        args (argparse.Namespace): Parsed command line arguments.
    """
    issue_key = args.cleanup
    jira_client = _build_jira_client()

    logger.info(f"Fetching issue {issue_key}...")
    issue = jira_client.issue(issue_key)
    logger.info(f"  Summary: {issue.fields.summary}")
    logger.info(f"  Status: {issue.fields.status.name}")

    response = input(f"Delete {issue_key}? [y/N] ")
    if response.lower() != 'y':
        logger.info("Aborted.")
        return

    issue.delete()
    logger.info(f"Deleted {issue_key}.")


def _build_parser() -> argparse.ArgumentParser:
    """
    Build the argument parser.

    Return Value(s):
        argparse.ArgumentParser: The configured parser.
    """
    parser = argparse.ArgumentParser(
        description='Invoke reconcile_jira_issues() to test customfield updates.',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Preview what would be created without making changes',
    )
    parser.add_argument(
        '--project', default='OCPBUGS',
        help='JIRA project key (default: OCPBUGS)',
    )
    parser.add_argument(
        '--component', default='Release',
        help='JIRA component name (default: Release)',
    )
    parser.add_argument(
        '--major', type=int, default=4,
        help='OCP major version (default: 4)',
    )
    parser.add_argument(
        '--minor', type=int, default=18,
        help='OCP minor version (default: 18)',
    )
    parser.add_argument(
        '--target-release', default='4.18.z',
        help='Target version string for the issue (default: 4.18.z)',
    )
    parser.add_argument(
        '--cleanup', metavar='ISSUE_KEY',
        help='Delete a JIRA issue created by a previous test run',
    )
    return parser


def main() -> None:
    """
    Entry point.
    """
    parser = _build_parser()
    args = parser.parse_args()

    if args.cleanup:
        _cleanup(args)
    else:
        _run_reconcile(args)


if __name__ == '__main__':
    main()
