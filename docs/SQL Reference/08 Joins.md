# Joins

Joins allow you to combine data from multiple relations.

## CROSS JOIN

A cross join returns the Cartesian product (all combinations) of two relations. Cross joins can either be specified using the explit `CROSS JOIN` syntax or by specifying multiple relations in the `FROM` clause.

<img src="../diagrams/cross-join.svg" width="400px">

Both of the following queries are equivalent:

~~~sql
SELECT *
  FROM nation
 CROSS JOIN region;
~~~

~~~sql
SELECT *
  FROM nation, region;
~~~

The nation table contains 25 rows and the region table contains 5 rows, so a cross join between the two tables produces 125 rows.

## INNER JOIN

<img src="../diagrams/inner-join.svg" width="400px">

## LEFT JOIN

<img src="../diagrams/left-join.svg" width="400px">

## FULL JOIN

<img src="../diagrams/full-join.svg" width="400px">