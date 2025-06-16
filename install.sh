#!/bin/bash

# Requires a local virtualenv at ./.venv. This can be created with "uv venv --python 3.11"
# Ignore .egg-info dirs in repo that might exist
uv pip install -e artcommon/ -e doozer/ -e elliott/ -e pyartcd/ -e ocp-build-data-validator/
