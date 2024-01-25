import json

import importlib_resources
from jsonschema import RefResolver, ValidationError
from jsonschema.validators import validator_for


def validate(file, data):
    # Load Json schemas
    path = importlib_resources.files("validator") / "json_schemas"
    schemas = {}
    for source in path.iterdir():
        if source.name.endswith(".json"):
            with open(source) as f:
                schemas[source.name] = json.load(f)
    schema_store = {schema.get("$id", filename): schema for filename, schema in schemas.items()}
    schema = schema_store["image_config.schema.json"]
    resolver = RefResolver.from_schema(schema, store=schema_store)
    validator = validator_for(schema)(schema, resolver=resolver)

    # Validate with JSON schemas
    try:
        print(f"Validating {file}...")
        validator.validate(data)
        # TODO: Check if the base images referenced in `from` field exist
    except ValidationError:
        errors = validator.iter_errors(data)
        return '\n'.join([str(e.message) for e in errors])
