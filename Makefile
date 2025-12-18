.PHONY: venv tox lint test pylint format format-check reinstall clean-reinstall

venv:
	uv venv --python 3.11
	# Install base requirements files first to ensure all runtime dependencies are available
	./install.sh


format-check:
	uv run ruff check --select I --output-format concise
	uv run ruff format --check

format:
	uv run ruff check --select I --fix
	uv run ruff format

lint: format-check
	uv run ruff check --output-format concise

pylint:
	uv run pylint --errors-only .

unit:
	uv run pytest --verbose --color=yes artcommon/tests/
	uv run pytest --verbose --color=yes doozer/tests/
	uv run pytest --verbose --color=yes elliott/tests/
	uv run pytest --verbose --color=yes pyartcd/tests/
	uv run pytest --verbose --color=yes ocp-build-data-validator/tests/

functional-elliott:
	uv run pytest --verbose --color=yes elliott/functional_tests/

functional-doozer:
	uv run pytest --verbose --color=yes doozer/tests_functional

test: lint unit

reinstall:
	# Force reinstall all editable packages (use when source code structure changes)
	uv sync --reinstall

clean-reinstall:
	# Complete clean reinstall (removes venv and recreates)
	rm -rf .venv
	$(MAKE) venv

gen-shipment-schema:
	echo 'from elliottlib.shipment_model import ShipmentConfig; import json; print(json.dumps(ShipmentConfig.model_json_schema(mode="validation"), indent=2))' | uv run python > ocp-build-data-validator/validator/json_schemas/shipment.schema.json
