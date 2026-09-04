PYTHON_VERSION ?= 3.11

.PHONY: venv tox lint test pylint pylint-imports format format-check reinstall clean-reinstall unit unit-artcommon unit-doozer unit-elliott unit-pyartcd unit-ocp-build-data-validator audit

venv:
	uv venv --python $(PYTHON_VERSION)
	# Install base requirements files first to ensure all runtime dependencies are available
	./install.sh


format-check:
	uv run ruff check --output-format concise
	uv run ruff format --check

format:
	uv run ruff check --fix
	uv run ruff format

lint: format-check pylint-imports
	uv run ruff check --output-format concise

pylint-imports:
	uv run pylint --disable=all --enable=E0401,E0611 --score=no --jobs=0 artcommon/artcommonlib doozer/doozerlib elliott/elliottlib pyartcd/pyartcd ocp-build-data-validator/validator

pylint:
	uv run pylint --errors-only .

unit-artcommon:
	uv run pytest --verbose --color=yes artcommon/tests/

unit-doozer:
	uv run pytest --verbose --color=yes doozer/tests/

unit-elliott:
	uv run pytest --verbose --color=yes elliott/tests/

unit-pyartcd:
	uv run pytest --verbose --color=yes pyartcd/tests/

unit-ocp-build-data-validator:
	uv run pytest --verbose --color=yes ocp-build-data-validator/tests/

unit:
	./run-tests-parallel.sh

functional-elliott:
	uv run pytest --verbose --color=yes elliott/functional_tests/

functional-doozer:
	uv run pytest --verbose --color=yes doozer/tests_functional

audit:
	uv audit --locked --preview-features audit --python-version $(PYTHON_VERSION)

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
