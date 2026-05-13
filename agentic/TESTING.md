# Testing Strategy

## Test Organization

```
artcommon/tests/                    -- artcommon unit tests
doozer/tests/                       -- doozer unit tests
  tests/cli/                        -- mirrors CLI command structure
  tests/backend/                    -- Konflux backend tests
  tests/test_distgit/               -- distgit-specific tests
  tests/resources/                  -- test fixtures
elliott/tests/                      -- elliott unit tests
pyartcd/tests/                      -- pyartcd tests
ocp-build-data-validator/tests/     -- validator tests
doozer/tests_functional/            -- doozer functional tests (6 test files)
elliott/functional_tests/           -- elliott functional tests
```

## Running Tests

All commands below are defined in `Makefile`.

| Target | Command | What it does |
|--------|---------|-------------|
| `make test` | `make lint` then `make unit` | Full CI check (lint + all unit tests) |
| `make unit` | `./run-tests-parallel.sh` | Runs all 5 package test suites in parallel |
| `make unit-artcommon` | `uv run pytest --verbose --color=yes artcommon/tests/` | artcommon tests only |
| `make unit-doozer` | `uv run pytest --verbose --color=yes doozer/tests/` | doozer tests only |
| `make unit-elliott` | `uv run pytest --verbose --color=yes elliott/tests/` | elliott tests only |
| `make unit-pyartcd` | `uv run pytest --verbose --color=yes pyartcd/tests/` | pyartcd tests only |
| `make unit-ocp-build-data-validator` | `uv run pytest --verbose --color=yes ocp-build-data-validator/tests/` | validator tests only |
| `make functional-doozer` | `uv run pytest --verbose --color=yes doozer/tests_functional` | doozer functional tests |
| `make functional-elliott` | `uv run pytest --verbose --color=yes elliott/functional_tests/` | elliott functional tests |

### Running specific tests

```bash
# Single test file
uv run pytest --verbose --color=yes doozer/tests/test_distgit.py

# Single test function
uv run pytest --verbose --color=yes doozer/tests/test_distgit.py::TestClass::test_method
```

## Test Frameworks

- **pytest** -- test runner and assertions
- **flexmock** -- mocking library (primary mock tool used throughout the codebase)
- **parameterized** -- parametrized test cases
- **pytest-mock** -- pytest plugin for mock fixtures
- **coverage** -- code coverage tracking

## Writing Tests

- Place tests in the same component's test directory, mirroring the source module structure.
- For bug fixes: write a failing test that reproduces the bug first, then fix.
- Mock all external systems (Brew, Errata Tool, GitHub, Jira, Slack). Never make real API calls in unit tests.
- Use flexmock for most mocking needs -- it is the established pattern in this codebase.
- Tests run with `uv run pytest`, so all dependencies must be installed via `make venv` first.

## Functional Tests

- Require Red Hat internal network access (Brew, Errata Tool, distgit).
- Run separately from unit tests; NOT run in CI.
- Developers run them locally before submitting changes that affect Brew/Errata integrations.
- Doozer functional tests (in `doozer/tests_functional/`):
  - `test_basic_rebase.py`
  - `test_golang_rebase.py`
  - `test_koji_wrapper.py`
  - `test_metadata.py`
  - `test_sanity.py`
  - `test_scan_sources.py`

## CI

- `.github/workflows/unit-tests.yaml` runs `make test` on all PRs.
- Runs in a Fedora container (`registry.fedoraproject.org/fedora:latest`) with Python 3.11.
- Uses `uv` (v0.9.18) for dependency management with lock file caching.
- System dependencies installed in CI: `git clang gcc krb5-devel make glibc`.

## Linting

Linting is part of `make test` (runs before unit tests via `make lint`).

| Target | Command | What it does |
|--------|---------|-------------|
| `make format-check` | `uv run ruff check --output-format concise` and `uv run ruff format --check` | Check formatting without changes |
| `make format` | `uv run ruff check --fix` and `uv run ruff format` | Auto-format code |
| `make lint` | `make format-check` then `uv run ruff check` | Lint checks |
| `make pylint` | `uv run pylint --errors-only .` | Pylint errors-only check |

Line length is 120 characters, configured in `pyproject.toml`.
