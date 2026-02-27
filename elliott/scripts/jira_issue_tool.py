#!/usr/bin/env python3

"""
A standalone tool for reading and updating JIRA issues using the existing
JIRABugTracker infrastructure from bzutil.py.

This is useful for testing JIRA field access (especially custom fields)
without running the full elliott/doozer pipeline.

Requirements:
    - JIRA_TOKEN environment variable must be set
    - JIRA_SERVER_URL environment variable (optional, defaults to jira_config setting)

Examples:
    # Read a JIRA issue and display its fields
    ./jira_issue_tool.py read OCPBUGS-12345

    # Read and show all custom field IDs and values
    ./jira_issue_tool.py read OCPBUGS-12345 --show-custom-fields

    # Update the Target Version field
    ./jira_issue_tool.py update OCPBUGS-12345 --target-version "4.18.z"

    # Update Release Notes fields
    ./jira_issue_tool.py update OCPBUGS-12345 --release-notes-text "N/A" --release-notes-type "Release Note Not Required"

    # Dry run (show what would be updated)
    ./jira_issue_tool.py update OCPBUGS-12345 --target-version "4.18.z" --dry-run

    # Dump all JIRA field name-to-ID mappings (useful for migration)
    ./jira_issue_tool.py dump-fields
"""

import argparse
import json
import os
import sys

from artcommonlib.jira_config import JIRA_SERVER_URL
from jira import JIRA


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

    print(f"Connected to JIRA server: {server}")
    return client


def _resolve_field_names(client: JIRA) -> dict:
    """
    Build a mapping of field ID to field name from the JIRA server.

    Arg(s):
        client (JIRA): An authenticated JIRA client.

    Return Value(s):
        dict: Mapping of field ID (str) -> field name (str).
    """
    return {f['id']: f['name'] for f in client.fields()}


def _cmd_read(client: JIRA, args: argparse.Namespace) -> None:
    """
    Read a JIRA issue and display its fields.

    Arg(s):
        client (JIRA): An authenticated JIRA client.
        args (argparse.Namespace): Parsed command line arguments.
    """
    issue_key = args.issue_key
    print(f"\nFetching issue: {issue_key}")
    issue = client.issue(issue_key)

    print(f"\n{'=' * 60}")
    print(f"Key:        {issue.key}")
    print(f"Summary:    {issue.fields.summary}")
    print(f"Status:     {issue.fields.status.name}")
    print(f"Project:    {issue.fields.project.key}")
    print(f"Issue Type: {issue.fields.issuetype.name}")

    # Components
    components = [c.name for c in issue.fields.components] if issue.fields.components else []
    print(f"Components: {', '.join(components) if components else '(none)'}")

    # Labels
    labels = issue.fields.labels or []
    print(f"Labels:     {', '.join(labels) if labels else '(none)'}")

    # Standard custom fields used by ART
    field_map = _resolve_field_names(client)

    # Known ART custom fields to display
    art_fields = {
        'Target Version': None,
        'Release Blocker': None,
        'Blocked Reason': None,
        'Severity': None,
        'CVE ID': None,
        'Downstream Component Name': None,
        'Embargo Status': None,
        'Release Notes Text': None,
        'Release Notes Type': None,
    }

    # Resolve field IDs
    id_to_name = field_map
    name_to_id = {v: k for k, v in id_to_name.items()}

    print("\n--- ART-relevant Custom Fields ---")
    for field_name in art_fields:
        field_id = name_to_id.get(field_name)
        if field_id:
            value = getattr(issue.fields, field_id, None)
            # Format complex values
            display_value = _format_field_value(value)
            print(f"  {field_name} ({field_id}): {display_value}")
        else:
            print(f"  {field_name}: (field not found on server)")

    if args.show_custom_fields:
        print("\n--- All Custom Fields ---")
        for field_id, field_name in sorted(id_to_name.items()):
            if field_id.startswith('customfield_'):
                value = getattr(issue.fields, field_id, None)
                if value is not None:
                    display_value = _format_field_value(value)
                    print(f"  {field_name} ({field_id}): {display_value}")


def _format_field_value(value: object) -> str:
    """
    Format a JIRA field value for display.

    Arg(s):
        value (object): The field value from JIRA.

    Return Value(s):
        str: A human-readable string representation.
    """
    if value is None:
        return "(empty)"
    if isinstance(value, list):
        items = []
        for item in value:
            if hasattr(item, 'name'):
                items.append(item.name)
            elif isinstance(item, dict) and 'name' in item:
                items.append(item['name'])
            else:
                items.append(str(item))
        return ', '.join(items) if items else "(empty list)"
    if hasattr(value, 'value'):
        return value.value
    if hasattr(value, 'name'):
        return value.name
    return str(value)


def _cmd_update(client: JIRA, args: argparse.Namespace) -> None:
    """
    Update custom fields on a JIRA issue.

    Arg(s):
        client (JIRA): An authenticated JIRA client.
        args (argparse.Namespace): Parsed command line arguments.
    """
    issue_key = args.issue_key
    dry_run = args.dry_run
    # Resolve field name -> ID mapping
    name_to_id = {f['name']: f['id'] for f in client.fields()}

    # Build update payload
    update_fields = {}

    if args.target_version:
        field_id = name_to_id.get('Target Version')
        if not field_id:
            print("ERROR: 'Target Version' field not found on server.", file=sys.stderr)
            sys.exit(1)
        update_fields[field_id] = [{'name': args.target_version}]

    if args.release_notes_text is not None:
        field_id = name_to_id.get('Release Note Text')
        if not field_id:
            print("ERROR: 'Release Note Text' field not found on server.", file=sys.stderr)
            sys.exit(1)
        update_fields[field_id] = args.release_notes_text

    if args.release_notes_type is not None:
        field_id = name_to_id.get('Release Note Type')
        if not field_id:
            print("ERROR: 'Release Note Type' field not found on server.", file=sys.stderr)
            sys.exit(1)
        update_fields[field_id] = {'value': args.release_notes_type}

    if not update_fields:
        print("No fields to update. Use --target-version, --release-notes-text, or --release-notes-type.")
        sys.exit(0)

    print(f"\nIssue: {issue_key}")
    print("Fields to update:")
    for field_id, value in update_fields.items():
        # Reverse lookup for display
        field_name = next((n for n, i in name_to_id.items() if i == field_id), field_id)
        print(f"  {field_name} ({field_id}): {value}")

    if dry_run:
        print("\n[DRY RUN] No changes made.")
        return

    issue = client.issue(issue_key)
    issue.update(fields=update_fields)
    print(f"\nSuccessfully updated {issue_key}.")

    # Verify the update by re-reading
    issue = client.issue(issue_key)
    print("\nVerification — current values after update:")
    for field_id in update_fields:
        field_name = next((n for n, i in name_to_id.items() if i == field_id), field_id)
        value = getattr(issue.fields, field_id, None)
        print(f"  {field_name} ({field_id}): {_format_field_value(value)}")


def _cmd_dump_fields(client: JIRA, args: argparse.Namespace) -> None:
    """
    Dump all JIRA field name-to-ID mappings. Useful for identifying
    field ID changes during a JIRA migration.

    Arg(s):
        client (JIRA): An authenticated JIRA client.
        args (argparse.Namespace): Parsed command line arguments.
    """
    fields = client.fields()

    if args.json:
        print(json.dumps(fields, indent=2, default=str))
        return

    # Sort by field name
    fields_sorted = sorted(fields, key=lambda f: f['name'].lower())

    if args.custom_only:
        fields_sorted = [f for f in fields_sorted if f['id'].startswith('customfield_')]

    print(f"\n{'Field Name':<45} {'Field ID':<30} {'Custom'}")
    print(f"{'-' * 45} {'-' * 30} {'-' * 6}")
    for f in fields_sorted:
        is_custom = 'Yes' if f['id'].startswith('customfield_') else ''
        print(f"{f['name']:<45} {f['id']:<30} {is_custom}")

    print(f"\nTotal fields: {len(fields_sorted)}")


def _build_parser() -> argparse.ArgumentParser:
    """
    Build the argument parser for this tool.

    Return Value(s):
        argparse.ArgumentParser: The configured parser.
    """
    parser = argparse.ArgumentParser(
        description='Read and update JIRA issues using art-tools infrastructure.',
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # read command
    read_parser = subparsers.add_parser('read', help='Read a JIRA issue and display its fields')
    read_parser.add_argument('issue_key', help='JIRA issue key (e.g., OCPBUGS-12345)')
    read_parser.add_argument(
        '--show-custom-fields',
        action='store_true',
        help='Show all custom fields that have values',
    )

    # update command
    update_parser = subparsers.add_parser('update', help='Update custom fields on a JIRA issue')
    update_parser.add_argument('issue_key', help='JIRA issue key (e.g., OCPBUGS-12345)')
    update_parser.add_argument('--target-version', help='Set Target Version (e.g., "4.18.z")')
    update_parser.add_argument('--release-notes-text', help='Set Release Notes Text (e.g., "N/A")')
    update_parser.add_argument(
        '--release-notes-type', help='Set Release Notes Type (e.g., "Release Note Not Required")'
    )
    update_parser.add_argument(
        '--dry-run', action='store_true', help='Show what would be updated without making changes'
    )

    # dump-fields command
    dump_parser = subparsers.add_parser('dump-fields', help='Dump all JIRA field name-to-ID mappings')
    dump_parser.add_argument('--custom-only', action='store_true', help='Only show custom fields')
    dump_parser.add_argument('--json', action='store_true', help='Output in JSON format')

    return parser


def main() -> None:
    """
    Entry point for the JIRA issue tool.
    """
    parser = _build_parser()
    args = parser.parse_args()

    client = _build_jira_client()

    if args.command == 'read':
        _cmd_read(client, args)
    elif args.command == 'update':
        _cmd_update(client, args)
    elif args.command == 'dump-fields':
        _cmd_dump_fields(client, args)


if __name__ == '__main__':
    main()
