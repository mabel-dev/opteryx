# Sample Data

Opteryx has three built-in relations for demonstration and testing.

- `$satellites` (8 columns, 177 rows)
- `$planets` (20 columns, 9 rows) #plutoisaplanet
- `$astronauts` (19 columns,  357 rows)

Satellite and Planet datasets where acquired from [this source](https://github.com/devstronomy/nasa-data-scraper/tree/f610e541a053f05e26573570604aed50b358cc43/data/json).

Astronaut dataset acquired from [Kaggle](https://www.kaggle.com/nasa/astronaut-yearbook).

~~~sql
SELECT *
  FROM $planets
~~~

Note that `$no_table` is used internally to represent no table has been specified.