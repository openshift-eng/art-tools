.PHONY: venv tox lint test

venv:
	python3.8 -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -e art-common/ -e doozer/ -e elliott/ -e pyartcd/
	./venv/bin/pip install -r doozer/requirements-dev.txt -r pyartcd/requirements-dev.txt
	cd elliott && ../venv/bin/pip install '.[tests]'
	# source venv/bin/activate

lint:
	./venv/bin/python -m flake8

unit:
	./venv/bin/python -m pytest --verbose --color=yes doozer/tests/
	./venv/bin/python -m pytest --verbose --color=yes elliott/tests/
	./venv/bin/python -m pytest --verbose --color=yes pyartcd/tests/

test: lint unit
