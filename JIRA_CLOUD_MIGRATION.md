# Jira Cloud Migration - Files to Update

This document outlines the files that need to be updated when migrating from Jira Server (issues.redhat.com) to Atlassian Cloud.

## Configuration Files

### 1. `pyartcd/config.example.toml`
**Location**: Line 24-28
**Changes needed**:
- Update `[jira]` section `url` field
- Current: `url = "https://issues.redhat.com"`
- New: `url = "https://<instance>.atlassian.net"` (replace `<instance>` with your Atlassian Cloud subdomain)

Example:
```toml
[jira]
url = "https://redhat.atlassian.net"  # Update this
[jira.templates]
ocp4 = "OCPPLAN-4756"
ocp3 = "OCPPLAN-1373"
```

## Source Code Files

### 2. `elliott/elliottlib/bzutil.py`
**Location**: Multiple locations

#### JIRABugTracker Class (lines 696-799+)
**Authentication Changes**:
- Line 727-733: `login()` method uses `JIRA_TOKEN` environment variable
- **Action**: Verify token authentication works with Atlassian Cloud (should be compatible)
- **Note**: Atlassian Cloud requires API tokens instead of password. Ensure `JIRA_TOKEN` contains a valid API token.

#### Custom Field IDs (lines 707-714)
**CRITICAL - These will likely change**:
```python
field_target_version = 'customfield_12319940'  # "Target Version"
field_release_blocker = 'customfield_12319743'  # "Release Blocker"
field_blocked_reason = 'customfield_12316544'  # "Blocked Reason"
field_severity = 'customfield_12316142'  # "Severity"
field_cve_id = 'customfield_12324749'  # "CVE ID"
field_cve_component = 'customfield_12324752'  # "Downstream Component Name"
field_cve_is_embargo = 'customfield_12324750'  # "Embargo Status"
field_security_levels = 'level'  # "Security Levels"
```

**Action**:
- The `_init_fields()` method (line 736-745) dynamically looks up some field IDs at runtime
- Verify all custom fields exist in Atlassian Cloud and update default values
- Run the initialization and check if field discovery works correctly
- Field IDs in Atlassian Cloud are different from on-premise Jira

#### Server Configuration (line 539)
```python
self._server = self.config.get('server', '')
```
**Action**: Ensure this pulls from ocp-build-data configuration correctly

### 3. `doozer/doozerlib/runtime.py`
**Location**: Line 806
**Changes needed**:
```python
server = bug_config.jira_config.server or 'https://issues.redhat.com'
```
**Action**: Update default URL if not configured in ocp-build-data
```python
server = bug_config.jira_config.server or 'https://<instance>.atlassian.net'
```

### 4. `pyartcd/pyartcd/jira_client.py`
**Location**: Line 14, 24-25
**Authentication**:
```python
client = JIRA(server_url, token_auth=token_auth)
```
**Action**:
- Verify token_auth parameter works with Atlassian Cloud
- Test visibility settings on line 25: `visibility={'type': 'group', 'value': 'Red Hat Employee'}`
  - Group visibility syntax may differ in Cloud vs Server

**Bot User Assignment** (line 28):
```python
self._client.assign_issue(key, 'openshift-art-jira-bot')
```
**Action**: Verify bot user account exists and has correct permissions in Atlassian Cloud

### 5. `elliott/elliottlib/shipment_utils.py`
**Location**: Lines 110, 112
**Hard-coded domain**:
```python
[b for b in release_notes.issues.fixed if b.source != "issues.redhat.com"]
...
Issue(id=str(issue_id), source="issues.redhat.com")
```
**Action**: Update source domain to match new Atlassian Cloud URL

### 6. `pyartcd/pyartcd/pipelines/scan_fips.py`
**Location**: Line 22
**Hard-coded domain**:
```python
JIRA_DOMAIN = "https://issues.redhat.com/"
```
**Action**: Update to new Atlassian Cloud URL
```python
JIRA_DOMAIN = "https://<instance>.atlassian.net/"
```

### 7. `doozer/doozerlib/cli/images_streams.py`
**Location**: Line 886
**Hard-coded URL in comment**:
```python
f"ART wants to connect issue [{issue}](https://issues.redhat.com/browse/{issue}) to this PR, ..."
```
**Action**: Update URL format for Atlassian Cloud
```python
f"ART wants to connect issue [{issue}](https://<instance>.atlassian.net/browse/{issue}) to this PR, ..."
```

## External Data Repository

### 8. ocp-build-data (External Repository)
**Repository**: https://github.com/openshift-eng/ocp-build-data
**Files to check**:
- Group configuration files (e.g., `openshift-4.17/group.yml`, `openshift-4.18/group.yml`, etc.)
- `bug.yml` or similar bug tracker configuration files

**Expected structure** (based on code analysis):
```yaml
jira_config:
  server: "https://<instance>.atlassian.net"
  project: "OCPBUGS"
  # ... other configuration
```

**Action**: Update server URL in all relevant group configurations

## Authentication Changes

### Environment Variables
**Current**: `JIRA_TOKEN` environment variable

**Actions**:
1. Generate new API tokens from Atlassian Cloud (tokens from on-premise Jira won't work)
2. Update CI/CD secrets and local development environments
3. Documentation at: https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/

### API Token Requirements
- Atlassian Cloud uses API tokens (not passwords)
- Tokens are user-specific
- Bot accounts need their own API tokens
- Verify `openshift-art-jira-bot` has been migrated and has valid token

## Testing Requirements

### Unit Tests to Update
1. `elliott/tests/test_bzutil.py` - Mock server URLs
2. `elliott/tests/test_bzutil_jira.py` - Jira-specific tests
3. `pyartcd/tests/test_jira_client.py` - Jira client tests

### Integration Testing Checklist
After making changes, test:
- [ ] Authentication with API token works
- [ ] Issue creation and updates work
- [ ] Custom field discovery (`_init_fields()`) works correctly
- [ ] Comment visibility settings work (`'Red Hat Employee'` group)
- [ ] Issue assignment to bot account works
- [ ] Issue linking and cloning works
- [ ] JQL queries return expected results
- [ ] Security level filtering works (if applicable)
- [ ] Transition workflows (e.g., 'Closed', 'In Progress') match new system
- [ ] Remote links functionality works

## API Compatibility Concerns

### Known Differences Between Jira Server and Cloud
1. **Custom Field IDs**: Will be different - must re-map
2. **API Endpoints**: Some endpoints may have changed
3. **Authentication**: Cloud uses API tokens exclusively
4. **Rate Limiting**: Cloud has different rate limits than on-premise
5. **Webhooks/Permissions**: May need reconfiguration
6. **JQL Syntax**: Generally compatible but verify complex queries
7. **Security Levels**: Implementation may differ

### Python Library Compatibility
The code uses the `jira` Python library:
```python
from jira import JIRA, Issue, JIRAError
```
**Action**: Verify library version supports Atlassian Cloud
- Current usage appears compatible
- May need to update to latest version

## Rollback Plan
1. Keep copy of all original configuration
2. Test in staging/development environment first
3. Have ability to quickly revert `ocp-build-data` changes
4. Document all API token locations for easy rollback

## Summary of File Changes

| File | Change Type | Priority |
|------|-------------|----------|
| `pyartcd/config.example.toml` | URL update | High |
| `elliott/elliottlib/bzutil.py` | Custom field IDs, server defaults | Critical |
| `doozer/doozerlib/runtime.py` | Server default URL | High |
| `pyartcd/pyartcd/jira_client.py` | Verify compatibility | Medium |
| `elliott/elliottlib/shipment_utils.py` | Hard-coded domain | Medium |
| `pyartcd/pyartcd/pipelines/scan_fips.py` | Hard-coded domain | Medium |
| `doozer/doozerlib/cli/images_streams.py` | URL in messages | Low |
| `ocp-build-data/*` (external) | Server configuration | Critical |

## Migration Steps

1. **Preparation**
   - Set up bot account (`openshift-art-jira-bot`) (if the existing one
     does not work)
   - Generate API tokens
   - Document custom field mappings (old ID → new ID)

2. **Code Updates**
   - Update all files listed above
   - Update unit test mocks
   - Update documentation

3. **Configuration Updates**
   - Update `ocp-build-data` repository
   - Update CI/CD environment variables
   - Update deployment configurations

4. **Testing**
   - Run unit tests
   - Run integration tests
   - Manual verification of key workflows
   - Verify all functionality
