lint:
	python -m pycln .
	python -m isort .
	python -m black .

refresh:
	python -m pip install --quiet --upgrade -r requirements.txt
	python -m pip install --quiet --upgrade -r tests/requirements.txt

t:
	python tests/sql_battery/test_shapes_and_errors_battery.py

test:
	python -m pytest