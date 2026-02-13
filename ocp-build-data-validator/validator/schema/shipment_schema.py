import json
import sys

from jsonschema import RefResolver, ValidationError
from jsonschema.validators import validator_for

if sys.version_info < (3, 9):
    # importlib.resources either doesn't exist or lacks the files()
    # function, so use the PyPI version:
    import importlib_resources
else:
    # importlib.resources has files(), so use that:
    import importlib.resources as importlib_resources


def validate(_, data):
    # Load Json schemas
    path = importlib_resources.files("validator") / "json_schemas"
    schemas = {
        source.name: json.load(open(source))
        for source in path.iterdir()
        if source.name.endswith(".json") and source.name.startswith("shipment")
    }
    schema_store = {schema.get("$id", filename): schema for filename, schema in schemas.items()}
    schema = schema_store["shipment.schema.json"]
    resolver = RefResolver.from_schema(schema, store=schema_store)
    validator = validator_for(schema)(schema, resolver=resolver)
    # Validate with JSON schemas
    try:
        validator.validate(data)
    except ValidationError:
        errors = validator.iter_errors(data)
        return "\n".join([f"{e.json_path}: {e.message}" for e in errors])
