# Design Philosophy and Patterns

## Overview

art-tools prioritizes correctness and automation reliability over performance. The tools manage critical release infrastructure where incorrect builds or advisories have direct customer impact. Every design decision reflects this: prefer explicit over implicit, fail loudly over silently, and make the safe path the easy path.

## Design Principles

### 1. Single source of truth (ocp-build-data)

**Why:** Prevents configuration drift across tools and environments. When build metadata lives in one place, there is exactly one thing to audit, review, and version-control.

**Example:** Image metadata (Dockerfile templates, upstream sources, build targets) is defined in YAML files within the [ocp-build-data](https://github.com/openshift-eng/ocp-build-data) repository, never hardcoded in doozer. Tools clone this repo at startup via `GitData` (`artcommon/artcommonlib/gitdata.py`) and read it through the group config loading infrastructure.

### 2. Session state via Runtime

**Why:** Avoids global mutable state, makes dependencies explicit, and enables testing through injection. Every CLI session has a well-defined lifecycle: create Runtime, initialize it, execute command, tear down.

**Example:** `Runtime.initialize()` in both doozer (`doozer/doozerlib/runtime.py`) and elliott (`elliott/elliottlib/runtime.py`) sets up everything a command needs -- Koji client sessions, working directories, metadata maps, assembly config -- before the command body runs. Both inherit from `GroupRuntime` ABC (`artcommon/artcommonlib/runtime.py`).

### 3. Assembly-driven constraints

**Why:** Release quality gates must be enforced programmatically. Assemblies encode which builds, RPMs, and advisories belong together, preventing accidental inclusion of untested components.

**Example:** STANDARD assemblies enforce sibling consistency (all images built from the same RPM set) and RPM version matching. The assembly system in `artcommon/artcommonlib/assembly.py` determines constraints based on assembly type (stream, standard, custom).

### 4. Composable CLI tools

**Why:** Each tool has a focused responsibility -- builds (doozer), advisories (elliott), orchestration (pyartcd). This separation enables independent testing, clear ownership, and flexible composition.

**Example:** pyartcd pipelines shell out to doozer and elliott commands rather than importing their internals. A release pipeline calls `doozer images:build` followed by `elliott create`, composing focused tools into complex workflows.

### 5. Defensive external integration

**Why:** Brew, Errata Tool, GitHub, and other external systems can and do fail. Network timeouts, transient server errors, and authentication expiry are normal operating conditions, not exceptional ones.

**Example:** Tenacity `@retry` decorators are used throughout the codebase for external calls -- Koji API calls in `doozer/doozerlib/brew.py` (`gssapi_login` retries 3 times with 60s waits), Jira client calls in `pyartcd/pyartcd/jira_client.py` (3 attempts, 5s waits), and `oc` commands in `pyartcd/pyartcd/oc.py`.

## Architecture Decisions

Significant structural decisions are recorded as Architecture Decision Records:

- [ADR-0001: Monorepo Structure](./decisions/adr-0001-monorepo-structure.md) -- why all five tools live in one repository
- [ADR-0002: Runtime Pattern](./decisions/adr-0002-runtime-pattern.md) -- the GroupRuntime ABC and tool-specific subclasses
- [ADR-0003: Dual Build System](./decisions/adr-0003-dual-build-system.md) -- supporting both Brew/OSBS and Konflux

## Design Patterns

### Runtime pattern

`GroupRuntime` ABC (`artcommon/artcommonlib/runtime.py`) defines the shared interface. Tool-specific subclasses (`doozer/doozerlib/runtime.py:Runtime`, `elliott/elliottlib/runtime.py:Runtime`, `pyartcd/pyartcd/runtime.py:GroupRuntime`) extend it with tool-specific state (e.g., doozer adds `image_map`, `rpm_map`; elliott adds errata sessions).

See: [Runtime concept doc](./domain/concepts/runtime.md)

### Model/MissingModel sentinel

Safe traversal of nested YAML config without KeyError. `Model` (`artcommon/artcommonlib/model.py`) wraps dicts to allow dotted attribute access. `MissingModel` is a falsy sentinel returned for absent keys -- accessing any attribute on it returns itself, so `config.foo.bar.baz` never raises, and `if not config.foo.bar.baz:` works naturally.

See: [Model/Missing concept doc](./domain/concepts/model-missing.md)

### Click CLI groups with pass_runtime

CLI commands are organized as Click groups. `pass_runtime = click.make_pass_decorator(Runtime)` (defined in `doozer/doozerlib/cli/__init__.py` and `elliott/elliottlib/cli/common.py`) injects the initialized Runtime into command handlers via decorator.

### click_coroutine

Bridges async functions with Click's synchronous command dispatch. Defined in three places: `doozer/doozerlib/cli/__init__.py`, `elliott/elliottlib/cli/common.py`, and `pyartcd/pyartcd/cli.py`. Wraps an `async def` command handler to run it via `asyncio.get_event_loop().run_until_complete()`. Widely used in pyartcd pipelines and some elliott/doozer commands.

### Tenacity retry

`@retry` decorator from the `tenacity` library wraps external API calls with configurable retry logic (attempt counts, wait strategies, reraise behavior). Used in `doozerlib/brew.py`, `pyartcd/oc.py`, `pyartcd/jira_client.py`, `pyartcd/signatory.py`, `pyartcd/locks.py`, and others.

## Anti-Patterns

- **Hardcoding build metadata in tool code.** Build configuration belongs in ocp-build-data. If you need a new knob, add it to the appropriate YAML schema.
- **Importing across tool boundaries.** doozerlib must not import elliottlib, and vice versa. Shared code belongs in artcommonlib. pyartcd orchestrates the other tools via CLI subprocess calls, not library imports.
- **Synchronous external API calls in hot paths.** Use async (`aiohttp`, `asyncio`) or background execution for operations that fan out to external systems.
- **Mutable global state.** Use the Runtime object to carry session state. Do not store configuration or client sessions in module-level variables.

## Trade-offs

| Decision | Chose | Over | Rationale |
|---|---|---|---|
| Monorepo | Shared venv, unified CI, atomic cross-tool changes | Independent release cycles per tool | Tools evolve together; breaking changes in artcommon need coordinated updates |
| Large Runtime class | Single entry point for all session state | Smaller, more focused service objects | Convenience for CLI commands that need many capabilities; testing via mock Runtime |
| ocp-build-data as external dependency | Single source of truth, separate review/approval for config changes | Self-contained tools with embedded config | Config changes are high-risk and need independent review by release engineers |

## Related Documents

- [Core Beliefs and Operating Principles](./design-docs/core-beliefs.md)
- [Design Documents Index](./design-docs/index.md)
- [Architecture Decision Records](./decisions/)
- [Domain Concepts](./domain/concepts/)
