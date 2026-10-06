"""Pre-mock system-dependent modules for local test execution.

The CI environment has all dependencies installed; this conftest enables
running the config_plashet tests locally when heavy deps (krb5, etc.)
are absent.  When a real package *is* installed, no mocking occurs so
that other tests that rely on the real modules are unaffected.
"""

import importlib
import sys
import types


def _ensure_mock(name, attrs=None):
    """Register a mock module only when the real module cannot be imported."""
    try:
        importlib.import_module(name)
        return  # real module available, nothing to mock
    except ImportError:
        pass

    m = types.ModuleType(name)
    m.__path__ = []
    m.__package__ = name
    sys.modules[name] = m

    # Bind the mock submodule to its parent so ``from parent.child import X``
    # works even when the parent is also a mock.
    if "." in name:
        parent_name, child_name = name.rsplit(".", 1)
        parent = sys.modules.get(parent_name)
        if parent is not None:
            setattr(parent, child_name, m)

    for k, v in (attrs or {}).items():
        setattr(m, k, v)


# ---- gssapi / kerberos / requests_gssapi chain ----
_ensure_mock('gssapi')
_ensure_mock('gssapi.raw')
_ensure_mock('gssapi.raw.misc')
_ensure_mock('gssapi.raw.named_tuples')
_ensure_mock('requests_gssapi', {'HTTPSPNEGOAuth': type('X', (), {})})
_ensure_mock('requests_gssapi.compat', {'HTTPKerberosAuth': type('X', (), {}), 'NullHandler': type('X', (), {})})
_ensure_mock('requests_gssapi.gssapi_')
_ensure_mock('requests_kerberos', {'HTTPKerberosAuth': type('X', (), {})})

# ---- errata_tool tree ----
_ErrataException = type('ErrataException', (Exception,), {})
_Erratum = type('Erratum', (), {})
_ErrataConnector = type('ErrataConnector', (), {})
_ensure_mock(
    'errata_tool',
    {
        'Erratum': _Erratum,
        'ErrataException': _ErrataException,
        'ErrataConnector': _ErrataConnector,
    },
)
_ensure_mock('errata_tool.bug', {'Bug': type('Bug', (), {})})
_ensure_mock('errata_tool.erratum', {'Erratum': _Erratum})
_ensure_mock('errata_tool.jira_issue', {'JiraIssue': type('JiraIssue', (), {})})
_ensure_mock('errata_tool.build', {'Build': type('Build', (), {})})
_ensure_mock('errata_tool.connector', {'ErrataConnector': _ErrataConnector})

# ---- bugzilla ----
_ensure_mock('bugzilla', {'Bugzilla': type('Bugzilla', (), {})})

# ---- jira ----
_JIRAError = type('JIRAError', (Exception,), {})
_JIRA = type('JIRA', (), {})
_Issue = type('Issue', (), {})
_ensure_mock('jira', {'JIRA': _JIRA, 'JIRAError': _JIRAError, 'Issue': _Issue})
_ensure_mock('jira.client', {'JIRA': _JIRA})
_ensure_mock('jira.resources')
_ensure_mock('jira.exceptions', {'JIRAError': _JIRAError})
