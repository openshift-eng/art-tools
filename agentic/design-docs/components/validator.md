---
component: ocp-build-data-validator
type: CLI
related: [artcommon]
---

# ocp-build-data-validator

## Purpose

Schema validation for ocp-build-data YAML configuration files. Ensures that image, RPM, group, and release definitions conform to expected schemas and that referenced Git sources exist. Runs in CI as a pre-merge gate to prevent invalid configuration from entering ocp-build-data.

## Location

| What | Path |
|------|------|
| Entry point | `ocp-build-data-validator/validator/__main__.py` -- `main()` function |
| Core modules | `ocp-build-data-validator/validator/` |
| JSON schemas | `ocp-build-data-validator/validator/json_schemas/` |
| Python schemas | `ocp-build-data-validator/validator/schema/` |
| Package config | `ocp-build-data-validator/pyproject.toml` |

## Entry Point

The CLI command is `validate-ocp-build-data`. It accepts one or more file paths as arguments:

```
validate-ocp-build-data FILE [FILE ...]
```

Options:
- `--single-thread` -- Disable parallel validation (useful for debugging with `code.interact()`)
- `--schema-only` -- Only run schema validations, skip Git source verification
- `--images-dir` -- Path to the ocp-build-data images directory (auto-detected from file paths if not specified)

By default, validation runs in parallel using `multiprocessing.Pool` with `cpu_count()` workers. Each worker validates files independently via the `validate()` function.

## Validation Pipeline

The `validate()` function in `ocp-build-data-validator/validator/__main__.py` runs a multi-stage pipeline for each file:

### Stage 1: Format Checking

Module: `ocp-build-data-validator/validator/format.py`

Parses the file as YAML. If parsing fails, validation stops with an error.

### Stage 2: Disabled Check

Files with `mode: disabled` are skipped (via `support.is_disabled()`).

### Stage 3: Schema Validation

Module: `ocp-build-data-validator/validator/schema/`

Validates the parsed YAML against JSON schemas and Python schema definitions. The artifact type is determined from the file path (image, rpm, releases, group).

JSON schemas in `ocp-build-data-validator/validator/json_schemas/` cover:
- `image_config.schema.json`, `member_image.schema.json` -- Image metadata schemas
- `member_rpm.schema.json` -- RPM metadata schema
- `releases.schema.json`, `release.schema.json` -- Releases configuration schemas
- `streams.schema.json` -- Streams definition schema
- `repos.schema.json` -- Repository configuration schema
- `assembly.schema.json`, `assembly_basis.schema.json`, `assembly_dependencies.schema.json`, `assembly_group_config.schema.json`, `assembly_issues.schema.json` -- Assembly schemas
- `shipment.schema.json` -- Shipment schema
- `build_profiles.schema.json` -- Build profile schemas
- `rhcos.schema.json` -- RHCOS configuration schema
- `scanning.schema.json` -- Scanning configuration schema
- `arch.schema.json`, `arches_dict.schema.json` -- Architecture schemas
- `cachito.schema.json` -- Cachito configuration schema
- `permits.schema.json` -- Assembly permit schemas
- `source_modification.schema.json` -- Source modification schemas

Python schemas in `ocp-build-data-validator/validator/schema/`:
- `group_schema.py` -- Group configuration validation
- `image_schema.py` -- Image configuration validation
- `rpm_schema.py` -- RPM configuration validation
- `releases_schema.py` -- Releases configuration validation
- `modification_schema.py` -- Source modification validation
- `streams_schema.py` -- Streams configuration validation
- `shipment_schema.py` -- Shipment configuration validation

### Stage 4: Releases Validation

Module: `ocp-build-data-validator/validator/releases.py`

For `releases.yml` files, performs additional validation of release definitions beyond schema checks.

### Stage 5: Git Source Verification

Modules:
- `ocp-build-data-validator/validator/github.py` -- Validates GitHub repository references exist and are accessible
- `ocp-build-data-validator/validator/cgit.py` -- Validates CGit (internal) repository references
- `ocp-build-data-validator/validator/distgit.py` -- Validates distgit repository references

This stage is skipped when `--schema-only` is specified.

## Support Modules

- `ocp-build-data-validator/validator/support.py` -- Utility functions:
  - `is_disabled()` -- Check if a config file is disabled
  - `get_artifact_type()` -- Determine artifact type from file path
  - `load_group_config_for()` -- Load the group config associated with a given file
  - `fail_validation()` -- Raise validation failure with appropriate exception

- `ocp-build-data-validator/validator/exceptions.py` -- Exception classes:
  - `ValidationFailed` -- Standard validation failure (exit code 1)
  - `ValidationFailedWIP` -- Validation failure for WIP items (non-fatal)

- `ocp-build-data-validator/validator/global_session.py` -- Manages a shared `requests.Session` for HTTP calls across multiprocessing workers

## Architecture

The validator is intentionally standalone with minimal coupling to other art-tools components. It validates the data that other tools consume, acting as a gate in the ocp-build-data CI pipeline.

The validation approach is defensive: each stage runs independently, and failures at any stage produce clear error messages identifying the file and the nature of the problem.

## Dependencies

- `pyyaml` -- YAML parsing
- `jsonschema` -- JSON Schema validation
- `requests` -- HTTP requests for Git source verification
- Standard library `multiprocessing` for parallel validation

The validator does not depend on artcommonlib, doozer, or elliott. This isolation is intentional -- the validator must work independently in CI environments without the full art-tools stack.

## Related Components

- **artcommon** -- Consumes the data that the validator validates; the Model/Missing system in artcommonlib relies on well-formed YAML
- **ocp-build-data** -- The external repository whose files this tool validates
