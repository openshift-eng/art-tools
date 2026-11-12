import json
import os

import importlib_resources
from jsonschema import RefResolver, ValidationError
from jsonschema.validators import validator_for

from .. import support


def validate(file, data, images_dir=None):
    """
    Validate a binaries/<name>.yml member against binary_config.schema.json and check its build context.

    :param file: Path of the binaries/<name>.yml file.
    :param data: Parsed YAML content.
    :param images_dir: ocp-build-data images/ directory; defaults to the one next to binaries/.
    """
    path = importlib_resources.files("validator") / "json_schemas"
    schemas = {source.name: json.load(open(source)) for source in path.iterdir() if source.name.endswith(".json")}
    schema_store = {schema.get("$id", filename): schema for filename, schema in schemas.items()}
    schema = schema_store["binary_config.schema.json"]
    resolver = RefResolver.from_schema(schema, store=schema_store)
    validator = validator_for(schema)(schema, resolver=resolver)

    print(f"Validating {file}...")
    try:
        validator.validate(data)
        errors = []
    except ValidationError:
        errors = [f"{err.json_path}: {err.message}" for err in validator.iter_errors(data)]
    if errors:
        return '\n'.join(errors)

    obd_dir = support.get_ocp_build_data_dir(file)
    images_dir = images_dir or os.path.join(obd_dir, 'images')
    if os.path.isdir(images_dir):
        image_files = {os.path.splitext(f)[0] for f in os.listdir(images_dir) if f.endswith(".yml")}
        name = support.get_repository_name(file)
        if name in image_files:
            errors.append(f"binaries/{name}.yml has the same name as images/{name}.yml; rename one")
        from_config = data.get("from", {})
        for parent in from_config.get("builder", []) + [from_config]:
            member = parent.get("member")
            if member and member not in image_files:
                errors.append(f"from: member '{member}' not found in {images_dir}")

    source = data.get("content", {}).get("source", {})
    if "ocp-build-data" in source.get("git", {}).get("url", "") and source.get("path"):
        context = os.path.join(obd_dir, source["path"], source.get("dockerfile", "Dockerfile"))
        if not os.path.isfile(context):
            errors.append(
                f"content.source: {source['path']}/{source.get('dockerfile', 'Dockerfile')} not found in {obd_dir}"
            )

    return '\n'.join(errors) if errors else None
