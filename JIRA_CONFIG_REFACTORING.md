# JIRA Configuration Refactoring Summary

## Overview

All JIRA-related settings have been centralized into a single configuration module in `artcommon`. This makes it easy to migrate between JIRA instances (e.g., from issues.redhat.com to Atlassian Cloud) by updating a single file or setting an environment variable.

## What Changed

### New Central Configuration Module

**Created:** `artcommon/artcommonlib/jira_config.py`

This module provides:
- `JIRA_SERVER_URL` - The JIRA server URL (default: https://issues.redhat.com)
- `DEFAULT_JIRA_SERVER_URL` - The default server URL constant
- `JIRA_API_BASE` - Base API URL (constructed from server URL)
- `JIRA_API_FIELD` - Field API endpoint URL
- `JIRA_DOMAIN_NAME` - Domain name without protocol (for comparisons)
- `JIRA_SECURITY_ALLOWLIST` - Set of allowed security levels
- `get_jira_browse_url(issue_key)` - Helper to generate browse URLs
- `get_jira_api_url(endpoint)` - Helper to generate API URLs

### Files Updated

#### Source Code Files

1. **pyartcd/pyartcd/pipelines/scan_fips.py**
   - Removed hard-coded `JIRA_DOMAIN = "https://issues.redhat.com/"`
   - Now imports `JIRA_SERVER_URL` from `artcommonlib.jira_config`

2. **elliott/elliottlib/constants.py**
   - Removed hard-coded `JIRA_API_FIELD` and `JIRA_SECURITY_ALLOWLIST`
   - Now imports both from `artcommonlib.jira_config`

3. **elliott/elliottlib/shipment_utils.py**
   - Removed hard-coded domain checks (`"issues.redhat.com"`)
   - Now imports and uses `JIRA_DOMAIN_NAME` from `artcommonlib.jira_config`

4. **doozer/doozerlib/runtime.py**
   - Updated `build_jira_client()` method
   - Changed fallback from hard-coded `'https://issues.redhat.com'` to `JIRA_SERVER_URL`

5. **doozer/doozerlib/cli/images_streams.py**
   - Removed hard-coded URL in PR comment templates
   - Now imports and uses `get_jira_browse_url()` function

6. **pyartcd/config.example.toml**
   - Added comments explaining the centralized configuration
   - Documented that JIRA_SERVER_URL env var can override the setting

#### Test Files

1. **elliott/tests/test_bzutil.py**
   - Replaced all hard-coded `'https://issues.redhat.com'` with `JIRA_SERVER_URL`
   - Updated permalink lambdas to use `get_jira_browse_url()`

2. **pyartcd/tests/pipelines/test_promote.py**
   - Replaced all hard-coded `"url": "https://issues.redhat.com/"` with `JIRA_SERVER_URL`

3. **pyartcd/tests/pipelines/test_prepare_release_konflux.py**
   - Replaced hard-coded `source="issues.redhat.com"` with `JIRA_DOMAIN_NAME`

4. **Created:** `artcommon/tests/test_jira_config.py`
   - Comprehensive tests for the new configuration module
   - Tests environment variable override functionality
   - Tests URL generation helpers

## How to Use

### Method 1: Update the Default (for permanent changes)

Edit `artcommon/artcommonlib/jira_config.py`:

```python
DEFAULT_JIRA_SERVER_URL = "https://redhat.atlassian.net"
```

### Method 2: Use Environment Variable (for runtime override)

```bash
export JIRA_SERVER_URL="https://redhat.atlassian.net"
# Run your commands
doozer --group openshift-4.20 ...
```

### Method 3: Keep ocp-build-data Configuration

The code already respects JIRA server settings from ocp-build-data's `bug.yml`:

```yaml
jira_config:
  server: "https://redhat.atlassian.net"
  project: "OCPBUGS"
```

The centralized `JIRA_SERVER_URL` is only used as a fallback when `bug.yml` doesn't specify a server.

## Benefits

1. **Single Source of Truth**: All JIRA URLs come from one place
2. **Easy Migration**: Change JIRA instance by updating one file or env var
3. **Consistent Behavior**: All tools use the same configuration
4. **Environment Override**: Can override per-environment without code changes
5. **Better Testing**: Tests use the same configuration, making them easier to maintain

## Migration to JIRA Cloud

When you're ready to migrate to JIRA Cloud:

### Option A: Environment Variable (Quick Test)
```bash
export JIRA_SERVER_URL="https://redhat.atlassian.net"
```

### Option B: Update Default (Permanent)
1. Edit `artcommon/artcommonlib/jira_config.py`
2. Change `DEFAULT_JIRA_SERVER_URL = "https://redhat.atlassian.net"`
3. Commit and deploy

### Option C: Update ocp-build-data (Per-Version)
Update each version's `bug.yml` in ocp-build-data repository.

## Files Summary

### Created
- `artcommon/artcommonlib/jira_config.py` - Central JIRA configuration
- `artcommon/tests/test_jira_config.py` - Tests for JIRA configuration

### Modified (Source Code)
- `pyartcd/pyartcd/pipelines/scan_fips.py`
- `elliott/elliottlib/constants.py`
- `elliott/elliottlib/shipment_utils.py`
- `doozer/doozerlib/runtime.py`
- `doozer/doozerlib/cli/images_streams.py`
- `pyartcd/config.example.toml`

### Modified (Tests)
- `elliott/tests/test_bzutil.py`
- `pyartcd/tests/pipelines/test_promote.py`
- `pyartcd/tests/pipelines/test_prepare_release_konflux.py`

## Testing

All tests pass successfully:

```bash
# Test the new configuration module
uv run pytest artcommon/tests/test_jira_config.py

# Test elliott JIRA functionality
uv run pytest elliott/tests/test_bzutil.py::TestJIRABugTracker

# Test environment variable override
JIRA_SERVER_URL="https://test.atlassian.net" uv run python -c \
  "from artcommonlib.jira_config import JIRA_SERVER_URL; print(JIRA_SERVER_URL)"
```

## Next Steps

1. Review the changes in this refactoring
2. Run tests to ensure everything works: `make test`
3. When ready to migrate to JIRA Cloud:
   - Update custom field IDs in `elliott/elliottlib/bzutil.py` (see JIRA_CLOUD_MIGRATION.md on jira_cloud_migration branch)
   - Choose one of the migration options above
   - Test with a non-production environment first
   - Update authentication tokens (JIRA_TOKEN env var)

## Notes

- The refactoring maintains backward compatibility
- All hard-coded JIRA URLs have been removed from the codebase
- The configuration respects the priority: ocp-build-data > env var > default
- Comments and references in code (like "# See https://issues.redhat.com/browse/ART-1234") are intentionally left unchanged as they're documentation, not configuration
