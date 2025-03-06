.PHONY: venv tox lint test pylint

venv:
	uv venv --python 3.11
	./install.sh
	uv pip install -r doozer/requirements-dev.txt -r pyartcd/requirements-dev.txt -r ocp-build-data-validator/requirements-dev.txt
	cd elliott && uv pip install '.[tests]'
	source venv/bin/activate

lint:
	python3 -m flake8

pylint:
	python3 -m pylint .

unit:
	python3 -m pytest --verbose --color=yes artcommon/tests/
	python3 -m pytest --verbose --color=yes doozer/tests/
	python3 -m pytest --verbose --color=yes elliott/tests/
	python3 -m pytest --verbose --color=yes pyartcd/tests/
	python3 -m pytest --verbose --color=yes ocp-build-data-validator/tests/

functional-elliott:
	python3 -m pytest --verbose --color=yes elliott/functional_tests/

functional-doozer:
	python3 -m pytest --verbose --color=yes doozer/tests_functional

test: lint unit
