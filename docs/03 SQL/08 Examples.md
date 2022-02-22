# SQL Examples

Opteryx has two built-in datasets for demonstration and testing.

- `$satellites` (8 columns, 177 rows)
- `$planets` (20 columns, 9 rows) #plutoisaplanet
- `$astronauts` (19 columns,  357 rows)

Satellite and Planet datasets where acquired from [this source](https://github.com/devstronomy/nasa-data-scraper/tree/f610e541a053f05e26573570604aed50b358cc43/data/json).

Astronaut dataset acquired from [Kaggle](https://www.kaggle.com/nasa/astronaut-yearbook)

~~~sql
SELECT *
  FROM $satellites
~~~
~~~
 id │ planetId │  name  │    gm    │ radius │ density │ magnitude │ albedo 
----+----------+--------+----------+--------+---------+-----------+--------
 1  │    3     │  Moon  │ 4902.801 │ 1737.5 │  3.344  │   -12.74  │  0.12  
 2  │    4     │ Phobos │ 0.000711 │  11.1  │  1.872  │    11.4   │ 0.071  
 3  │    4     │ Deimos │ 9.9e-05  │  6.2   │  1.471  │   12.45   │ 0.068  
 4  │    5     │   Io   │ 5959.916 │ 1821.6 │  3.528  │    5.02   │  0.63  
 5  │    5     │ Europa │ 3202.739 │ 1560.8 │  3.013  │    5.29   │  0.67  
 .. │    ...   │ ...    │ ...      │ ...    │  ...    │    ...    │  ...   
~~~