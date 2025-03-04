.PHONY: venv tox lint test pylint

venv:
	uv venv --python 3.11
	./install.sh
	uv pip install -r doozer/requirements-dev.txt -r pyartcd/requirements-dev.txt -r ocp-build-data-validator/requirements-dev.txt
	cd elliott && uv pip install '.[tests]'

lint:
	./.venv/bin/python -m flake8

pylint:
	./.venv/bin/python -m pylint .

unit:
	./.venv/bin/python -m pytest --verbose --color=yes artcommon/tests/
	./.venv/bin/python -m pytest --verbose --color=yes doozer/tests/
	./.venv/bin/python -m pytest --verbose --color=yes elliott/tests/
	./.venv/bin/python -m pytest --verbose --color=yes pyartcd/tests/
	./.venv/bin/python -m pytest --verbose --color=yes ocp-build-data-validator/tests/

functional-elliott:
	./.venv/bin/python -m pytest --verbose --color=yes elliott/functional_tests/

test: lint unit
