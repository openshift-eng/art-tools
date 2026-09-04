#!/usr/bin/env python3

"""
This module provides validation for group.yml files using JSON schemas.
"""

import json
import sys

from artcommonlib.util import validate_bridge_release_basis_group
from jsonschema import RefResolver, ValidationError
from jsonschema.validators import validator_for
from schema import SchemaError

from validator.support import replace_vars

if sys.version_info < (3, 9):
    # importlib.resources either doesn't exist or lacks the files()
    # function, so use the PyPI version:
    import importlib_resources
else:
    # importlib.resources has files(), so use that:
    import importlib.resources as importlib_resources


def _demerge(data):
    """
    Recursively turn dict meta-attrs ("!?-") that are merged for inheritance into regular attrs just for schema validation.

    Arg(s):
        data: The data structure to demerge
    Return Value(s):
        The demerged data structure
    """
    if type(data) in [bool, int, float, str, bytes, type(None)]:
        return data

    if type(data) is list:
        return [_demerge(item) for item in data]

    if type(data) is dict:
        new_data = {}
        for name, value in data.items():
            if name[-1] in ("!", "?", "-"):
                merged_name = name[:-1]
                if merged_name in data:
                    raise SchemaError(f"Cannot specify '{name}' and '{merged_name}' attrs in same dict")
                name = merged_name

            new_data[name] = _demerge(value)

        return new_data

    raise TypeError(f"Unexpected value type: {type(data)}: {data}")


def validate(_, data):
    """
    Validate group.yml data against the assembly_group_config schema.

    Arg(s):
        _ (str): File path (unused but kept for consistency with other validators)
        data (dict): Parsed YAML data from group.yml
    Return Value(s):
        str: Error message if validation fails, empty string otherwise
    """
    # Load Json schemas
    path = importlib_resources.files("validator") / "json_schemas"
    schemas = {source.name: json.load(open(source)) for source in path.iterdir() if source.name.endswith(".json")}
    schema_store = {schema.get("$id", filename): schema for filename, schema in schemas.items()}
    schema = schema_store["assembly_group_config.schema.json"]
    resolver = RefResolver.from_schema(schema, store=schema_store)
    validator = validator_for(schema)(schema, resolver=resolver)
    demerged_data = _demerge(data)
    # Validate with JSON schemas
    try:
        validator.validate(demerged_data)
    except ValidationError:
        errors = validator.iter_errors(demerged_data)
        return '\n'.join([f"{e.json_path}: {e.message}" for e in errors])

    bridge_release = demerged_data.get("bridge_release") or {}
    basis_group = bridge_release.get("basis_group")
    group_name = demerged_data.get("name")
    vars_map = demerged_data.get("vars") or {}
    if basis_group and group_name:
        # group.yml's `name` is normally an unresolved "{MAJOR}.{MINOR}" template
        # (substituted at runtime by doozer/elliott). Resolve it here too, otherwise
        # every group.yml with a templated name fails this check.
        if "MAJOR" in vars_map and "MINOR" in vars_map:
            group_name = replace_vars(group_name, vars_map)
            basis_group = replace_vars(basis_group, vars_map)
        try:
            validate_bridge_release_basis_group(group_name, basis_group)
        except ValueError as e:
            return str(e)

    return ''
