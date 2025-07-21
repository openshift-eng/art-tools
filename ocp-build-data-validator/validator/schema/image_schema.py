import json
import os

import importlib_resources
from jsonschema import RefResolver, ValidationError
from jsonschema.validators import validator_for


def validate(file, data, images_dir=None):
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
    errors = []
    try:
        print(f"Validating {file}...")
        validator.validate(data)
    except ValidationError:
        errors.extend([f"{err.json_path}: {err.message}" for err in validator.iter_errors(data)])

    # Validate that the image has a delivery.delivery_repo_names field if it is not disabled or for_release is false
    if data.get("mode") != "disabled" and data.get("for_release") is not False:
        delivery_info = data.get('delivery')
        if not delivery_info or 'delivery_repo_names' not in delivery_info:
            errors.append("Image must have a 'delivery.delivery_repo_names' field unless 'mode' is 'disabled' or 'for_release' is false.")


    if images_dir:
        image_files = [os.path.splitext(f)[0] for f in os.listdir(images_dir) if f.endswith(".yml")]

        # Validate 'from' members
        if "from" in data:
            if "member" in data["from"]:
                if data["from"]["member"] not in image_files:
                    errors.append(f"from.member: Image '{data['from']['member']}' not found in {images_dir}")
            if "builder" in data["from"]:
                for builder in data["from"]["builder"]:
                    if "member" in builder:
                        if builder["member"] not in image_files:
                            errors.append(f"from.builder.member: Image '{builder['member']}' not found in {images_dir}")

        # Validate dependents
        if "dependents" in data:
            for dependent in data["dependents"]:
                if dependent not in image_files:
                    errors.append(f"dependents: Dependent image '{dependent}' not found in {images_dir}")

    return '\n'.join(errors) if errors else None
