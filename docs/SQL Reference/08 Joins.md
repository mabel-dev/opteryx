# Joins

Joins allow you to combine data from multiple relations.

## CROSS JOIN

~~~
FROM left_table CROSS JOIN right_table
~~~
~~~
FROM left_table, right_table
~~~

A `CROSS JOIN` returns the Cartesian product (all combinations) of two relations. Cross joins can either be specified using the explit `CROSS JOIN` syntax or by specifying multiple relations in the `FROM` clause.

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

~~~
FROM left_table INNER JOIN right_table
~~~
~~~
FROM left_table JOIN right_table
~~~

An `INNER JOIN` selects records that have matching values in both relations.

### Syntax

~~~sql
SELECT column_name(s)
FROM table1
INNER JOIN table2
ON table1.column_name = table2.column_name;
~~~

<img src="../diagrams/inner-join.svg" width="400px">

## LEFT JOIN

~~~
FROM left_table LEFT JOIN right_table ON condition
~~~
~~~
FROM left_table LEFT OUTER JOIN right_table ON condition
~~~

A `LEFT JOIN` selects all records from the left table (table1), and the matching records from the right table (table2). The result is 0 records from the right side, if there is no match.

~~~sql
SELECT column_name(s)
FROM table1
LEFT JOIN table2
ON table1.column_name = table2.column_name;
~~~

<img src="../diagrams/left-join.svg" width="400px">

## FULL JOIN

~~~
FROM left_table FULL JOIN right_table ON condition
~~~
~~~
FROM left_table FULL OUTER JOIN right_table ON condition
~~~

The `FULL JOIN` keyword returns all records when there is a match in left (table1) or right (table2) table records.

~~~sql
SELECT column_name(s)
FROM table1
FULL OUTER JOIN table2
ON table1.column_name = table2.column_name
WHERE condition;
~~~

<img src="../diagrams/full-join.svg" width="400px">