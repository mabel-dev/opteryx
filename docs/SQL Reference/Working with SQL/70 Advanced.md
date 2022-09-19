# Advanced 

## Query Variables

Variables can be used when a value within a query would benefit from being configurable by the user running the query. For example pre-built queries which perform the same core statement, but with a variable input.

Variables are defined using the `SET` statement. These variables are available to `SELECT` statements as part of the same query batch. For example:

~~~sql
-- set the planet id, change for different planets
SET @id = 3;
SELECT name
  FROM $planets
 WHERE id = @id;
~~~

The above query batch contains two statements, the `SET` and the `SELECT` separated by a semicolon (`;`). The variable is defined in the `SET` statement and must start with an at symbol (`@`). The variable is then used within a filter in the `WHERE` clause of the `SELECT` statement.

## Query Parameters



## WITH hints

Hints are used to force the planner, optimizer or the executor to make specific decisions. If a hint is not recognized, it is ignored by the planner and executor, however is reported in the warnings.

!!! Note
    Hints use the keyword `WITH` which is also the keyword for CTEs, this information relates to hints and not CTEs.

~~~
FROM dataset WITH(NO_CACHE)
~~~

Instructs blob/file reader to not use cache, regardless of other settings.

~~~
FROM dataset WITH(NO_PARTITION)
~~~

Instructs the blob/file reader to not use partitioning, regardless of other settings.

~~~
FROM dataset WITH(NO_PUSH_PROJECTION)
~~~

Instructs the blob/file reader not to try to prune columns at read time.

~~~
FROM dataset WITH(PARALLEL_READ)
~~~

☣️ **Experimental** Instructs the blob/file reader to try to use multiple processes to download files (experimental). Expected to be be `NO_PARALLEL_READ` when out of experimental mode.

