 # Contains helper scripts for JIRA operations
 - jira_issue_tool.py: A tool to read and update custom fields on JIRA issues
 - test_reconcile_jira.py: A script to test the reconcile_jira_issues function



# jira_issue_tool.py
Read an issue

  ## Basic read
  `./elliott/scripts/jira_issue_tool.py read OCPBUGS-12345`

  ## Show all custom fields that have values
  `./elliott/scripts/jira_issue_tool.py read OCPBUGS-12345 --show-custom-fields`

  Update custom fields

  ## Dry run — show what would change
  `./elliott/scripts/jira_issue_tool.py update OCPBUGS-12345 --target-version "4.18.z" --dry-run`

  ## Actually update Target Version
  `./elliott/scripts/jira_issue_tool.py update OCPBUGS-12345 --target-version "4.18.z"`

  ## Update Release Notes fields
  ```
  ./elliott/scripts/jira_issue_tool.py update OCPBUGS-12345 \
    --release-notes-text "N/A" \
    --release-notes-type "Release Note Not Required"
  ```
## Dump all field mappings (useful for migration)

  ## All fields
  `./elliott/scripts/jira_issue_tool.py dump-fields`

  ## Custom fields only
  ./elliott/scripts/jira_issue_tool.py dump-fields --custom-only

  ## JSON output
  `./elliott/scripts/jira_issue_tool.py dump-fields --custom-only --json`

  Key design choices

  - Field IDs are resolved dynamically via client.fields() — the script looks up 'Target Version', 'Release Notes Text', and 'Release Notes
  Type' by name, not by hardcoded ID. This means it works on both the old JIRA Server and new Atlassian Cloud regardless of field ID changes.
  - Supports both JIRA Server and Cloud — detects atlassian in the server URL and uses basic_auth (requires JIRA_USER env var) vs token_auth.
  - Verify-after-update — after an update, it re-reads the issue to confirm the values took effect.

  Required env vars

  - JIRA_TOKEN — always required
  - JIRA_USER — only needed for Atlassian Cloud
  - JIRA_SERVER_URL — optional, defaults to the value in artcommonlib/jira_config.py
