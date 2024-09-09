.PHONY: venv tox lint test

venv:
	uv venv --python 3.11
	uv self update
	uv pip install -e artcommon/ doozer/ -e elliott/ -e pyartcd/ -e ocp-build-data-validator/
	uv pip install -r doozer/requirements-dev.txt -r pyartcd/requirements-dev.txt -r ocp-build-data-validator/requirements-dev.txt
	cd elliott && uv pip install '.[tests]'

lint:
	./.venv/bin/python -m flake8

unit:
	./.venv/bin/python -m pytest --verbose --color=yes artcommon/tests/
	./.venv/bin/python -m pytest --verbose --color=yes doozer/tests/
	./.venv/bin/python -m pytest --verbose --color=yes elliott/tests/
	./.venv/bin/python -m pytest --verbose --color=yes pyartcd/tests/
	./.venv/bin/python -m pytest --verbose --color=yes ocp-build-data-validator/tests/

test: lint unit
