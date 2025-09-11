.PHONY: venv tox lint test pylint format format-check reinstall clean-reinstall

venv:
	uv venv --python 3.11
	# Install base requirements files first to ensure all runtime dependencies are available
	uv pip install -r artcommon/requirements.txt -r doozer/requirements.txt -r pyartcd/requirements.txt -r ocp-build-data-validator/requirements.txt
	# Install elliott dependencies (it uses pyproject.toml)
	cd elliott && uv pip install . && cd ..
	# Install development dependencies
	uv pip install -r doozer/requirements-dev.txt -r pyartcd/requirements-dev.txt -r ocp-build-data-validator/requirements-dev.txt
	# Install elliott test dependencies
	cd elliott && uv pip install '.[tests]' && cd ..
	# Install packages in editable mode
	./install.sh

format-check:
	uv run -m ruff check --select I --output-format concise --config ruff.toml
	uv run -m ruff format --check --config ruff.toml

format:
	uv run -m ruff check --select I --fix --config ruff.toml
	uv run -m ruff format --config ruff.toml

lint: format-check
	uv run -m ruff check --output-format concise --config ruff.toml

pylint:
	uv run -m pylint --errors-only .

unit:
	uv run -m pytest --verbose --color=yes artcommon/tests/
	uv run -m pytest --verbose --color=yes doozer/tests/
	uv run -m pytest --verbose --color=yes elliott/tests/
	uv run -m pytest --verbose --color=yes pyartcd/tests/
	uv run -m pytest --verbose --color=yes ocp-build-data-validator/tests/

functional-elliott:
	uv run -m pytest --verbose --color=yes elliott/functional_tests/

functional-doozer:
	uv run -m pytest --verbose --color=yes doozer/tests_functional

test: lint unit

reinstall:
	# Force reinstall all editable packages (use when source code structure changes)
	uv pip install --force-reinstall -e artcommon/ -e doozer/ -e elliott/ -e pyartcd/ -e ocp-build-data-validator/

clean-reinstall:
	# Complete clean reinstall (removes venv and recreates)
	rm -rf .venv
	$(MAKE) venv

gen-shipment-schema:
	echo 'from elliottlib.shipment_model import ShipmentConfig; import json; print(json.dumps(ShipmentConfig.model_json_schema(mode="validation"), indent=2))' | uv run python > ocp-build-data-validator/validator/json_schemas/shipment.schema.json
