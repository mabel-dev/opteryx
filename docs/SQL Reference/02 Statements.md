# Statements

[Opteryx](https://github.com/mabel-dev/opteryx) targets ANSI SQL compliant syntax. This standard alignment allows Opteryx users to quickly understand how to query data and enables easier porting of SQL between query engines and databases.

## EXPLAIN

Show the logical execution plan of a statement.

~~~sql
EXPLAIN
statement
~~~

The `EXPLAIN` clause outputs a summary of the execution plan for the query in the `SELECT` statement.

!!! Warning  
    The data returned by the `EXPLAIN` statement is intended for interactive usage only and the output format may change between releases. Applications should not depend on the output of the `EXPLAIN` statement.

## SELECT

Retrieve rows from zero or more relations.

~~~sql
SELECT [ DISTINCT ] select_list
  FROM relation [WITH (NO_CACHE,NO_PARTITION,NO_PUSH_PROJECTION)]
       [ INNER ] JOIN relation
                 USING (column)
       CROSS JOIN relation
       LEFT [ OUTER ] JOIN relation
       RIGHT [ OUTER ] JOIN relation
       FULL [ OUTER ] JOIN relation
                      ON condition
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

The `SELECT` clause specifies the list of columns that will be returned by the query. While it appears first in the clause, logically the expressions here are executed after most other clauses. The `SELECT` clause can contain arbitrary expressions that transform the output, as well as aggregate functions.

The `DISTINCT` modifier is specified, only unique rows are included in the result set. In this case, each output column must be of a type that allows comparison.

### FROM / JOIN clauses

~~~
FROM relation [, ...] [WITH (NO_CACHE, NO_PARTITION, NO_PUSH_PROJECTION)]
~~~
~~~
FROM relation [ INNER ] JOIN relation < USING (column) | ON condition >
~~~ 
~~~
FROM relation < LEFT | RIGHT | FULL > [OUTER] JOIN relation
~~~
~~~
FROM relation CROSS JOIN < relation | UNNEST(column) >
~~~

The `FROM` clause specifies the source of the data on which the remainder of the query should operate. Logically, the `FROM` clause is where the query starts execution. The `FROM` clause can contain a single relation, a combination of multiple relations that are joined together, or another `SELECT` query inside a subquery node.

`JOIN` clauses allow you to combine data from multiple relations. If no `JOIN` qualifier is provided, `INNER` will be used. `JOIN` qualifiers are mutually exclusive. `ON` and `USING` clauses are also mutually exclusive and can only be used with `INNER` and `LEFT` joins.

See [Joins](https://mabel-dev.github.io/opteryx/SQL%20Reference/08%20Joins/) for more information on `JOIN` syntax and functionality.

Hints can be provided as part of the statement to direct the query planner and executor to make decisions. Relation hints are declared as `WITH` statements following a relation in the `FROM` and `JOIN` clauses, for example `FROM $astronauts WITH (NO_CACHE)`. Reconised hints are:

Hint               | Effect                         
------------------ | --------------------------------------------
NO_CACHE           | Ignores any cache configuration 
NO_PARTITION       | Do not use partition configuration when reading
NO_PUSH_PROJECTION | Do not attempt to prune columns when reading 

!!! Note  
    Hints are not guaranteed to be followed, the query planner and executor may ignore hints in specific circumstances.

### FOR clause

~~~
FOR date
~~~
~~~
FOR DATES BETWEEN start AND end
~~~
~~~
FOR DATES IN range
~~~

The `FOR` clause is a non ANSI SQL extension which filters data by the date it was recorded for.

See [Temporality](https://mabel-dev.github.io/opteryx/SQL%20Reference/09%20Temporality/) for more information on `FOR` syntax and functionality.

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

`GROUP BY` expressions may use column numbers, however, this is not recommended for statements intended for reuse. 

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

`ORDER BY` expressions may use column numbers, however, this is not recommended for statements intended for reuse.

## SHOW COLUMNS

List the columns in a relation along with their data type. Without any modifiers, `SHOW COLUMNS` only reads a single page of data before returning.

~~~sql
SHOW [EXTENDED] [FULL] COLUMNS
FROM relation
LIKE pattern
 FOR period
~~~

### EXTENDED modifier

Inclusion of the `EXTENDED` modifier includes summary statistics about the columns which take longer and more memory to create than the standard summary information without the modifier. The summary information varies between column types and values.

### FULL modifier

Inclusion of the `FULL` modifier uses the entire dataset in order to return complete column information, rather than just the first page from the dataset.

### LIKE clause

~~~
LIKE pattern
~~~

Specify a pattern in the optional `LIKE` clause to filter the results to the desired subset by the column name. This does not require a left-hand operator, it will always filter by the column name.

### FOR clause

~~~
FOR date
~~~
~~~
FOR DATES BETWEEN start AND end
~~~
~~~
FOR DATES IN range
~~~

The `FOR` clause specifies the date to review data for. Although this supports the full syntax as per the `SELECT` statements.

See [Temporality](https://mabel-dev.github.io/opteryx/SQL%20Reference/09%20Temporality/) for more information on `FOR` syntax and functionality.

## SHOW FUNCTIONS

List the functions and aggregators supported by the engine.

~~~sql
SHOW FUNCTIONS
~~~