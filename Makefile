.PHONY: venv tox lint test pylint

venv:
	uv venv --python 3.11
	./install.sh
	uv pip install -r doozer/requirements-dev.txt -r pyartcd/requirements-dev.txt -r ocp-build-data-validator/requirements-dev.txt
	cd elliott && uv pip install '.[tests]'

lint:
	uv run -m flake8

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
