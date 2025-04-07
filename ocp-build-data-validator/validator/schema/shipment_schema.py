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


class ShipmentValidator:
    def __init__(self):
        path = importlib_resources.files("validator") / "json_schemas"
        schemas = {source.name: json.load(open(source)) for source in path.iterdir()
                   if source.name.endswith(".json") and source.name.startswith("shipment")}
        schema_store = {schema.get("$id", filename): schema for filename, schema in schemas.items()}
        self.schema = schema_store["shipment.schema.json"]
        self.resolver = RefResolver.from_schema(self.schema, store=schema_store)
        self.validator = validator_for(self.schema)(self.schema, resolver=self.resolver)
        self.full_schema = self.resolve_schema(self.schema)

    def resolve_schema(self, schema):
        def resolve_ref(ref_schema):
            """Resolve schema references."""
            if "$ref" in ref_schema:
                ref = ref_schema["$ref"]
                with self.resolver.resolving(ref) as resolved:
                    return resolved
            return ref_schema

        resolved_schema = resolve_ref(schema)
        if not (schema.get("type") == "object" and "properties" in schema):
            return resolved_schema

        resolved_schema["properties"] = {
            prop_name: self.resolve_schema(prop_schema)
            for prop_name, prop_schema in resolved_schema["properties"].items()
        }
        return resolved_schema

    def validate(self, data):
        try:
            self.validator.validate(data)
        except ValidationError:
            errors = self.validator.iter_errors(data)
            return '\n'.join([f"{e.json_path}: {e.message}" for e in errors])


def validate(_, data):
    return ShipmentValidator().validate(data)
