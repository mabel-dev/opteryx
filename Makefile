lint:
	python -m pip install --quiet --upgrade pycln isort ruff yamllint
#	python -m yamllint .
	python -m ruff check --fix --exit-zero
	python -m pycln .
	python -m isort .
	python -m ruff format opteryx

update:
	python -m pip install --upgrade pip
	python -m pip install --upgrade -r requirements.txt
	python -m pip install --upgrade -r tests/requirements.txt

t:
	clear
	python tests/sql_battery/test_shapes_and_errors_battery.py

s:
	clear
	python tests/storage/test_sql_sqlite.py

b:
	clear
	python scratch/brace.py

test:
	clear
	export MANUAL_TEST=1
	python -m pytest

mypy:
	clear
	python -m pip install --upgrade mypy
	python -m mypy --ignore-missing-imports --python-version 3.10 --no-strict-optional --check-untyped-defs opteryx

coverage:
	clear
	export MANUAL_TEST=1
	python -m coverage run -m pytest 
	python -m coverage report --include=opteryx/** -m

compile:
	python setup.py build_ext --inplace