lint:
	python -m pip install --quiet --upgrade pycln isort ruff yamllint cython-lint
#	python -m yamllint .
	cython-lint opteryx/compiled/**/*.pyx
	python -m ruff check --fix --exit-zero
	python -m pycln .
	python -m isort .
	python -m ruff format opteryx

update:
	python -m pip install --upgrade pip uv
	python -m uv pip install --upgrade -r tests/requirements.txt
	python -m uv pip install --upgrade -r requirements.txt

t:
	clear
	python tests/sql_battery/test_shapes_and_errors_battery.py

r:
	clear
	python tests/sql_battery/test_run_only_battery.py

test:
	clear
	export MANUAL_TEST=1
	python -m pytest

mypy:
	clear
	python -m pip install --upgrade mypy
	python -m mypy --ignore-missing-imports --python-version 3.11 --no-strict-optional --check-untyped-defs opteryx

coverage:
	clear
	export MANUAL_TEST=1
	python -m coverage run -m pytest --color=yes
	python -m coverage report --include=opteryx/** --fail-under=80 -m

compile:
	clear
	find . -name '*.so' -delete
	python setup.py clean
	python setup.py build_ext --inplace

c:
	python setup.py build_ext --inplace
