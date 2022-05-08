# Joins

Joins allow you to combine data from multiple relations.

## CROSS JOIN

A `CROSS JOIN` returns the Cartesian product (all combinations) of two relations. Cross joins can either be specified using the explicit `CROSS JOIN` syntax or by specifying multiple relations in the `FROM` clause.

~~~
FROM left_table CROSS JOIN right_table
~~~
~~~
FROM left_table, right_table
~~~

~~~sql
SELECT *
  FROM left_table
 CROSS JOIN right_table;
~~~

<img src="../diagrams/cross-join.svg" width="400px">

The size of the resultant dataset when using `CROSS JOIN` is length of the two datasets multiplied together (2 x 3 = 6, in the pictural example), which can easily result in extremely large datasets. When an alternate join approach is possible, it will almost always perform better than a `CROSS JOIN`.

## INNER JOIN

An `INNER JOIN` selects records that have matching values in both relations. Inner joins can either be specified using the full `INNER JOIN` syntax or the shorter `JOIN` syntax.

~~~
FROM left_table INNER JOIN right_table [ ON | USING ]
~~~
~~~
FROM left_table JOIN right_table [ ON | USING ]
~~~

~~~sql
SELECT *
  FROM left_table
 INNER JOIN right_table
    ON left_table.column_name = right_table.column_name;
~~~

<img src="../diagrams/inner-join.svg" width="400px">

## LEFT JOIN

A `LEFT JOIN` selects all records from the left table (table1), and the matching records from the right table (table2). The result is 0 records from the right side, if there is no match.

~~~
FROM left_table LEFT JOIN right_table ON condition
~~~
~~~
FROM left_table LEFT OUTER JOIN right_table ON condition
~~~

~~~sql
SELECT *
  FROM left_table
  LEFT OUTER JOIN right_table
    ON left_table.column_name = right_table.column_name;
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
SELECT *
  FROM left_table
  FULL OUTER JOIN right_table
    ON left_table.column_name = right_table.column_name;
~~~

<img src="../diagrams/full-join.svg" width="400px">