#!/bin/bash

# Requires a local virtualenv at ./.venv. This can be created with "uv venv --python 3.11"
uv pip install \
    -e ./artcommon/ \
    -e ./doozer/[dev] \
    -e ./elliott/[dev] \
    -e ./pyartcd/[dev] \
    -e ./ocp-build-data-validator/[dev]