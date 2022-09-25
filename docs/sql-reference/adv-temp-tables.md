# Relation Constructors

[Opteryx](https://github.com/mabel-dev/opteryx) provides options to create temporary relations as part of query definitions. These relations exist only for the execution of the query that defines them.

## Using `VALUES`

`VALUES` allows you to create a multi-column temporary relation where the values in the relation are explicitly defined in the statement.

A simple example is as follows:

~~~sql
SELECT * 
  FROM (
         VALUES ('High', 3),
                ('Medium', 2),
                ('Low', 1)
       ) AS ratings (name, rating);
~~~

Result:

~~~
 name   | rating
--------+--------
 High   |      3
 Medium |      2
 Low    |      1
~~~

## Using `UNNEST`

`UNNEST` allows you to create a single column temporary relation where the values in the relation are explicitly defined in the statement.

A simple example is as follows:

~~~sql
SELECT *
  FROM UNNEST((1,2,3));
~~~

Result:

~~~
 unnest 
--------
      1
      2
      3
~~~

!!! note
    The values in the `UNNEST` function are in two sets of parenthesis. The function accepts a list of values, parenthesis is used to wrap parameters to functions and also used to define lists.

## Using `generate_series`

`generate_series` allows you to create series by defining the bounds of the series, and optionally, an interval to step between values in the created series. 

`generate_series` supports the following variations:

Form                                 | Types   | Description
------------------------------------ | ------- | --------------------------
`generate_series(stop)`              | NUMERIC | Generate a NUMERIC series between 1 and 'stop', with a step of 1 
`generate_series(start, stop)`       | NUMERIC, NUMERIC | Generate a NUMERIC series between 'start' and 'stop', with a step of 1
`generate_series(start, stop, step)` | NUMERIC, NUMERIC, NUMERIC | Generate a NUMERIC series between 'start' and 'stop', with an explicit step size
`generate_series(start, stop, interval)` | TIMESTAMP, TIMESTAMP, INTERVAL | Generate a TIMESTAMP series between 'start' and 'stop', with a given interval
`generate_series(cidr)` | VARCHAR | Generate set of IP addresses from a given CIDR (e.g. `192.168.0.0/24`)

Single Parameter Example (NUMERIC):

~~~sql
SELECT *
  FROM generate_series(3)
~~~
~~~
 generate_series 
----------------
               1
               2
               3
~~~

Single Parameter Example (VARCHAR):

~~~sql
SELECT *
  FROM generate_series('192.168.1.0/30')
~~~
~~~
 generate_series 
----------------
 192.168.1.0
 192.168.1.1
 192.168.1.2
 192.168.1.3
~~~

Two parameter Example:

~~~sql
SELECT *
  FROM generate_series(2, 4)
~~~
~~~
 generate_series 
----------------
               2
               3
               4
~~~

Three parameter NUMERIC Example:

~~~sql
SELECT *
  FROM generate_series(-5, 5, 5)
~~~
~~~
 generate_series 
----------------
              -5
               0
               5
~~~

Three parameter TIMESTAMP example:

~~~sql
SELECT *
  FROM generate_series('2020-01-01', '2025-12-31', '1y')
~~~
~~~
 generate_series 
----------------
2020-01-01 00:00
2021-01-01 00:00
2022-01-01 00:00
2023-01-01 00:00
2024-01-01 00:00
2025-01-01 00:00
~~~

### Interval Definitions

Intervals are defined quantifying one or more periods which make up the interval, supported periods and their notation are:

Recognized interval parts for the `GENERATE_SERIES` function are:

Period  | Symbol                   | Aliases
------- | :----------------------- | ----
Years   | **year** / **years**     | y / yr / yrs
Months  | **month** / **months**   | mo / mon / mons / mth / mths
Weeks   | **week** / **weeks**     | w / wk / wks
Days    | **day** / **days**       | d
Hours   | **hour** / **hours**     | h / hr / hrs
Minutes | **minute** / **minutes** | m / min / mins 
Seconds | **second** / **seconds** | s / sec / secs

Where required, periods can be combined to define more complex intervals, for example `1h30m` represents one hour and 30 minutes.

## Using `FAKE`

`FAKE` creates a table of random integers from provided row and column counts. This functionality has limited application outside of creating datasets for testing.

A simple example is as follows:

~~~sql
SELECT * 
  FROM FAKE(3, 2);
~~~

Result:

~~~
   column_0 │   column_1 
------------┼------------
      32981 │      50883
       5037 │      42087
      51741 │      49456
~~~