# SQL Statements

Opteryx aims to be a ANSI SQL compliant query engine. This standard compliance allows Opteryx users to quickly understand how to query data and enables easier porting of SQL between query engines and databases.

## EXPLAIN

Show the logical execution plan of a statement.

~~~sql
EXPLAIN
select statement
~~~

The `EXPLAIN` clause outputs a summary of the execution plan for the query in the `SELECT` statement.

## SELECT

Retrieve rows from zero or more relations.

~~~sql
SELECT select_list
FROM relation
  INNER JOIN relation
  CROSS JOIN relation
FOR period
WHERE condition
GROUP BY groups
  HAVING group_filter
ORDER BY order_expr
OFFSET n
LIMIT n
~~~

### SELECT clause

~~~
SELECT [ DISTINCT ] expression [, ...]
~~~

The `SELECT` clause specifies the list of columns that will be returned by the query. While it appears first in the clause, logically the expressions here are executed only at the end. The `SELECT` clause can contain arbitrary expressions that transform the output, as well as aggregate functions.

The `DISTINCT` quantifier is specified, only unique rows are included in the result set. In this case, each output column must be of a type that allows comparison.

### FROM / JOIN clauses

The `FROM` clause specifies the source of the data on which the remainder of the query should operate. Logically, the `FROM` clause is where the query starts execution. The `FROM` clause can contain a single relation, a combination of multiple relations that are joined together, or another `SELECT` query inside a subquery node.

`JOIN` clauses allow you to combine data from multiple relations.

### FOR clause

The `FOR` clause is a non ANSI SQL extention which filters data by the date it was recorded for.

### WHERE clause

~~~
WHERE condition
~~~

The `WHERE` clause specifies any filters to apply to the data. This allows you to select only a subset of the data in which you are interested. Logically the `WHERE` clause is applied immediately after the `FROM` clause.

### GROUP BY / HAVING clauses

~~~
GROUP BY expression [, ...]
~~~
~~~
HAVING group_filter
~~~

The `GROUP BY` clause specifies which grouping columns should be used to perform any aggregations in the `SELECT` clause. If the `GROUP BY` clause is specified, the query is always an aggregate query, even if no aggregations are present in the `SELECT` clause. The `HAVING` clause specifies filters to apply to aggregated data, `HAVING` clauses require a `GROUP BY` clause.

### ORDER BY / LIMIT / OFFSET clauses

~~~
ORDER BY expression [ ASC | DESC ] [, ...]
~~~
~~~
OFFSET count
~~~
~~~
LIMIT count
~~~

`ORDER BY`, `LIMIT` and `OFFSET` are output modifiers. Logically they are applied at the very end of the query. The `OFFSET` clause discards initial rows from the returned set, the `LIMIT` clause restricts the amount of rows fetched, and the `ORDER BY` clause sorts the rows on the sorting criteria in either ascending or descending order.

## SHOW COLUMNS

List the columns in a relation along with their data type.

~~~sql
SHOW COLUMNS FROM relation
LIKE pattern
WHERE condition
~~~

### LIKE clause

Specify a pattern in the optional `LIKE` clause to filter the results to the desired subset by the column name.

### WHERE clause

The `WHERE` clause specifies any filters to apply to the data. This allows you to select only a subset of the data in which you are interested.

!!! note
    Only one of `LIKE` and `WHERE` can be used in the same statement.