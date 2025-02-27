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
	python -m uv pip install --upgrade pytest pytest-xdist
	clear
	export MANUAL_TEST=1
	python -m pytest -n auto --color=yes

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
	python -m pip install --upgrade pip uv
	python -m uv pip install --upgrade numpy 'cython>=3.1.0a1' setuptools
	find . -name '*.so' -delete
	python setup.py clean
	python setup.py build_ext --inplace -j 4

c:
	python setup.py build_ext --inplace
