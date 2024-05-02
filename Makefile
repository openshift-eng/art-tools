.PHONY: venv tox lint test

venv:
	python3.8 -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -e artcommon/ doozer/ -e elliott/ -e pyartcd/ -e ocp-build-data-validator/
	./venv/bin/pip install -r doozer/requirements-dev.txt -r pyartcd/requirements-dev.txt -r ocp-build-data-validator/requirements-dev.txt
	cd elliott && ../venv/bin/pip install '.[tests]'
	# source venv/bin/activate

lint:
	./venv/bin/python -m flake8

unit:
	./venv/bin/python -m pytest --verbose --color=yes artcommon/tests/
	./venv/bin/python -m pytest --verbose --color=yes doozer/tests/
	./venv/bin/python -m pytest --verbose --color=yes elliott/tests/
	./venv/bin/python -m pytest --verbose --color=yes pyartcd/tests/
	./venv/bin/python -m pytest --verbose --color=yes ocp-build-data-validator/tests/

test: lint unit

default-python-venv:
	python3 -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -e artcommon/ doozer/ -e elliott/ -e pyartcd/ -e ocp-build-data-validator/
	./venv/bin/pip install -r doozer/requirements-dev.txt -r pyartcd/requirements-dev.txt -r ocp-build-data-validator/requirements-dev.txt
	cd elliott && ../venv/bin/pip install '.[tests]'
	# source venv/bin/activate
