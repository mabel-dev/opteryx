# Relation Constructors

[Opteryx](https://github.com/mabel-dev/opteryx) provides options to create temporary relations as part of query definitions. These relations exist only for the execution of the query that defines them.

## Using `VALUES`

`VALUES` allows you to create a multicolumn temporary relation where the values in the relation are explicitly defined in the statement.

A simple example is as follows:

~~~sql
SELECT * 
  FROM
    (
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
    The values in the unnest function are in two sets of parenthesis, this is because the function accepts a list of values and parenthesis is used to wrap parameters to functions and also used to define lists.

## Using `generate_series`

`generate_series` allows you to create series by defining the bounds of the series, and optionally, an interval to step between values in the created series.

A simple example is as follows:

~~~sql
SELECT *
  FROM generate_series(2,11,2)
~~~

Result:

~~~
 generate_series 
----------------
               2
               4
               6
               8
              10
~~~