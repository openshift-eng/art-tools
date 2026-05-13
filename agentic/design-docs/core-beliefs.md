# Core Beliefs and Operating Principles

This document captures the foundational design decisions and constraints that govern the art-tools codebase. Every contributor and automation agent should internalize these before making changes.

## Operating Principles

### 1. ocp-build-data is the single source of truth

All build metadata -- image configs, RPM configs, group settings, assembly definitions, errata tool config, streams -- lives in the [ocp-build-data](https://github.com/openshift-eng/ocp-build-data) Git repository. Tools clone it at startup via `GitData` (`artcommon/artcommonlib/gitdata.py`) and `BuildDataLoader` (`artcommon/artcommonlib/config/__init__.py`).

Never hardcode build metadata in tool code. If you need a new configuration knob, add it to the appropriate YAML file in ocp-build-data and read it through the existing loading infrastructure.

The data path can be overridden via:
- `--data-path` CLI flag
- `DOOZER_DATA_PATH` or `ELLIOTT_DATA_PATH` environment variables

### 2. Runtime encapsulates session state

Every CLI session creates a `Runtime` object initialized with `--group` (e.g., `openshift-4.17`). The Runtime owns:

- The Koji/Brew client session (`shared_koji_client_session`)
- The working directory and all subdirectories (distgits, sources, brew-logs, flags)
- Loaded metadata maps (`image_map`, `rpm_map`, `component_map`)
- Assembly configuration and type
- The group config loaded from ocp-build-data

The inheritance chain is:

```
GroupRuntime (ABC)                  # artcommon/artcommonlib/runtime.py
  -> doozerlib.runtime.Runtime     # doozer/doozerlib/runtime.py
  -> elliottlib.runtime.Runtime    # elliott/elliottlib/runtime.py
```

`GroupRuntime` provides initialization of logging, Konflux DB connection, and declares the abstract `group_config` property. Tool-specific Runtime classes extend this with their own initialization logic (`doozerlib.runtime.Runtime.initialize()` handles distgit cloning, image tree generation, assembly basis events, etc.).

This pattern avoids global state. All session data flows through the Runtime instance, which Click's `pass_runtime` decorator makes available to every command.

### 3. Assembly defines release boundaries

The assembly system is defined in `artcommon/artcommonlib/assembly.py`. Assembly types (`AssemblyTypes` enum):

- **STREAM** -- Continuous development. Default. No basis event, relaxed constraints.
- **STANDARD** -- Named releases (e.g., `4.17.1`). All consistency checks enforced (sibling matching, RPM version alignment, RHCOS consistency).
- **CANDIDATE** -- Release candidate or feature candidate.
- **CUSTOM** -- No constraints enforced.
- **PREVIEW** -- Internal `.next` or preview releases.

The assembly type controls constraint enforcement throughout the pipeline. STANDARD assemblies enforce all checks via `AssemblyIssueCode` validation. STREAM assemblies skip most checks to allow continuous development.

Assembly definitions live in `releases.yml` in ocp-build-data. A basis event (Brew event ID or Konflux timestamp) pins the build state for reproducibility.

### 4. CLI tools are composable

pyartcd orchestrates releases by shelling out to doozer and elliott commands as subprocesses. This keeps each tool focused and independently testable:

- **doozer** handles builds (images, RPMs) and distgit management
- **elliott** handles advisories, bugs, and errata
- **pyartcd** sequences them into pipelines

Pipeline code in `pyartcd/pyartcd/pipelines/` invokes doozer/elliott via `artcommonlib/exectools.py` subprocess utilities. This separation means each tool can be tested, debugged, and run independently.

### 5. External systems are unreliable

Brew, Errata Tool, GitHub, Jira, Slack, and other external services can fail transiently. The codebase uses tenacity retry decorators for external calls. Examples found in:

- `pyartcd/pyartcd/jenkins.py`
- `pyartcd/pyartcd/jira_client.py`
- `pyartcd/pyartcd/locks.py`
- `pyartcd/pyartcd/git.py`
- `pyartcd/pyartcd/signatory.py`
- `pyartcd/pyartcd/oc.py`
- `elliott/elliottlib/errata.py`
- `elliott/elliottlib/rhcos.py`
- `elliott/elliottlib/bzutil.py`
- `doozer/doozerlib/backend/konflux_image_builder.py`
- Multiple pipeline modules in `pyartcd/pyartcd/pipelines/`

Handle transient failures gracefully. Use `@retry` with appropriate stop/wait strategies rather than bare try/except.

---

## Non-Negotiable Constraints

1. **Kerberos authentication required for Brew/Errata operations.** The `shared_koji_client_session()` context manager in `doozer/doozerlib/runtime.py` calls `gssapi_login()` on first use. Without a valid Kerberos ticket, Brew operations will fail.

2. **Assembly constraints MUST be enforced for STANDARD assemblies.** The `AssemblyIssueCode` enum in `artcommon/artcommonlib/assembly.py` defines specific constraint violations (conflicting RPMs, mismatched siblings, outdated RPMs, inconsistent RHCOS). STANDARD assemblies fail if any non-permitted issues are detected.

3. **Embargo handling must prevent premature disclosure.** The `BuildVisibility` enum in `artcommon/artcommonlib/build_visibility.py` distinguishes PUBLIC vs PRIVATE builds with visibility suffixes (`p0`/`p1` for Brew, `p2`/`p3` for Konflux). The `is_release_embargoed()` function defaults to treating unknown builds as embargoed (safe default). Never bypass this.

4. **ocp-build-data schema must validate before use.** The `ocp-build-data-validator` component (`ocp-build-data-validator/validator/`) validates YAML files against JSON schemas in `validator/json_schemas/` and Python schemas in `validator/schema/`. This runs in CI before data merges.

---

## Patterns We Use

### Runtime pattern

Abstract base in `artcommon/artcommonlib/runtime.py` (`GroupRuntime`), concrete implementations in each tool. Initialized via Click group command, passed to subcommands via `pass_runtime = click.make_pass_decorator(Runtime)`.

```python
# artcommon/artcommonlib/runtime.py
class GroupRuntime(ABC):
    @property
    @abstractmethod
    def group_config(self):
        pass
```

### Model/Missing sentinel for safe YAML traversal

Defined in `artcommon/artcommonlib/model.py`. `Model` wraps dicts, `ListModel` wraps lists. Accessing a non-existent key returns the `Missing` singleton (a `MissingModel` instance) instead of raising `KeyError`. This allows safe chained access:

```python
value = config.some.nested.key  # Returns Missing if any level is absent
if value is Missing:
    # handle default
```

`Missing` is falsy (`__bool__` returns `False`), so `if config.optional_field:` works naturally.

### Click CLI groups with pass_runtime

Each tool defines a Click group as its entry point. The group callback creates the Runtime and stores it in the Click context. Subcommands receive it via `@pass_runtime`:

- doozer: `doozer/doozerlib/cli/__init__.py` -- `@click.group` creates `Runtime`, stores in `ctx.obj`
- elliott: `elliott/elliottlib/cli/common.py` -- same pattern
- pyartcd: `pyartcd/pyartcd/cli.py` -- uses `Runtime.from_config_file()`

### Async pipeline execution with click_coroutine

All three tools define a `click_coroutine` wrapper that bridges Click's synchronous command dispatch with async pipeline code:

- `doozer/doozerlib/cli/__init__.py` -- `click_coroutine()`
- `elliott/elliottlib/cli/common.py` -- `click_coroutine()`
- `pyartcd/pyartcd/cli.py` -- `click_coroutine()`

This allows Click commands to be `async def` while maintaining synchronous CLI entry points.

### Tenacity retry for external calls

External API calls use `@retry` from tenacity with configurable stop conditions and wait strategies. This is used throughout the codebase for Brew, Errata Tool, GitHub, Jira, Jenkins, and other external service interactions.

### Metadata classes

`MetadataBase` in `artcommon/artcommonlib/metadata.py` is the base for `ImageMetadata` and `RPMMetadata`. It loads raw config from ocp-build-data, applies assembly overrides via `assembly_metadata_config()`, and extracts component namespace/name.

---

## Deprecated Patterns

1. **setup.py** -- Replaced by `pyproject.toml` + hatchling for all packages. Do not add new `setup.py` files.

2. **Synchronous Errata API calls** -- Being replaced by `elliott/elliottlib/errata_async.py`. New Errata Tool interactions should prefer the async interface.

3. **Direct OSBS builds** -- Being replaced by Konflux. The Konflux integration lives in `artcommon/artcommonlib/konflux/` and `doozer/doozerlib/backend/konflux_image_builder.py`. New build pipeline work should target Konflux.
