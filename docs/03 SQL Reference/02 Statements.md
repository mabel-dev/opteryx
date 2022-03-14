# SQL Statements

~~~sql
SELECT
      [DISTINCT]
      { column | expression } [, { column | expression } ...]
      [FROM table_references
        [FOR temporal_expression]]
      [WHERE condition]
      [GROUP BY { column | expression }
        [HAVING condition]]
      [LIMIT row_count]
      [OFFSET row_count]
      [ORDER BY column [ ASC | DESC ]
~~~

- *column* a column name from one of the tables in the `FROM` clause.
- *expression* a function or aggregate 
- *table_references* names of datasets, dataset alias or virtual dataset definitions
- *temporal_expression* a date partition limit expression
- *condition* a logical expression to filter returned data
- *row_count* a number representing a number of rows

~~~sql
EXPLAIN
      { SELECT statement }
~~~

Outputs a summary of the execution plan for the query in the `SELECT` statement.