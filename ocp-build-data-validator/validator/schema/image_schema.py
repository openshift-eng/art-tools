import json
import os
from pathlib import Path
from typing import Set

import importlib_resources
from jsonschema import RefResolver, ValidationError
from jsonschema.validators import validator_for
from ruamel.yaml import YAML


def _get_valid_repos(ocp_build_data_dir: str) -> Set[str]:
    """
    Collects valid repository names from both old and new style repo definitions.

    Arg(s):
        ocp_build_data_dir (str): Path to the ocp-build-data directory containing group.yml

    Return Value(s):
        set: Set of valid repository names
    """
    valid_repos = set()
    yaml = YAML(typ='safe')

    # New style: repos/ directory with individual YAML files
    repos_dir = Path(ocp_build_data_dir) / 'repos'
    if repos_dir.exists():
        for repo_file in repos_dir.glob('*.yml'):
            try:
                with open(repo_file) as f:
                    data = yaml.load(f)
                    # Repo files contain a list with one element
                    if isinstance(data, list) and len(data) > 0 and 'name' in data[0]:
                        valid_repos.add(data[0]['name'])
            except Exception:
                # Skip files that can't be parsed
                pass

    # Old style: repos section in group.yml
    group_yml = Path(ocp_build_data_dir) / 'group.yml'
    if group_yml.exists():
        try:
            with open(group_yml) as f:
                group_data = yaml.load(f)
                if group_data and 'repos' in group_data:
                    # In old style, repo names are the keys in the repos dict
                    valid_repos.update(group_data['repos'].keys())
        except Exception:
            # Skip if group.yml can't be parsed
            pass

    return valid_repos


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
            errors.append(
                "Image must have a 'delivery.delivery_repo_names' field unless 'mode' is 'disabled' or 'for_release' is false."
            )

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

        # Validate enabled_repos
        if "enabled_repos" in data:
            ocp_build_data_dir = os.path.dirname(images_dir)
            valid_repos = _get_valid_repos(ocp_build_data_dir)

            if valid_repos:  # Only validate if we found repo definitions
                for repo in data["enabled_repos"]:
                    if repo not in valid_repos:
                        errors.append(
                            f"enabled_repos: Repository '{repo}' not found in repos/ directory or group.yml. "
                            f"Valid repositories must be defined in either {ocp_build_data_dir}/repos/*.yml "
                            f"or in the 'repos' section of {ocp_build_data_dir}/group.yml"
                        )

    return '\n'.join(errors) if errors else None
