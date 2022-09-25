# Query Optimization

No optimization technique is universally true, these recommendations should improve performance in most cases. As will all optimization, test in your unique set of circumstances before assuming it to be true.

> Adapted from [15 Best Practices for SQL Optimization](https://betterprogramming.pub/15-best-practices-for-sql-optimization-956759626321).

## 1. Avoid using `SELECT *`

Selecting only the fields you need to be returned improves query performance by reducing the amount of data that is processed internally.

A principle the Query Optimizer uses is to eliminate rows and columns to process as early as possible, `SELECT *` removes the option to remove columns from the data being processed.

## 2. Prune Early

Where available, use temporal filters (`FOR DATE`) to limit the date range over will limit the number of partitions that need need to be read.

Not reading the record is faster than reading and working out if it needs to be filtered out of the result set.

## 3. `GROUP BY` field selection

**VARCHAR**
Grouping by `VARCHAR` columns is usually slower than grouping by `NUMERIC` columns, if you have an option of grouping by a username or a numeric user id, prefer the user id.

**cardinality**
Grouping by columns with high cardinality (mostly unique) is generally slower than grouping where there is a lot of duplication in the groups.

## 4. Avoid `CROSS JOIN`

Cross join will very likely create a lot of records that are not required - if you then filter these records from the two source tables using a `WHERE` clause, it's likely you should use an `INNER JOIN` instead.

## 5. Small table drives big table

Most `JOIN`s require iterating over two relations, the _left_ relation, which is the one in the `FROM` clause, and the _right_ relation which is the one in the `JOIN` clause (`SELECT * FROM left JOIN right`). It is generally faster to put the smaller relation to the _left_.

## 6. Do not use too many values with the `IN` keyword

## 7. Use the correct `JOIN`

## 8. Use `LIMIT`

`LIMIT` stops a query when it has returned the desired number of results; if you do not want the full dataset, using `LIMIT` can reduce the time taken to process a statement.

However, some operations are 'greedy', that is, they need all of the data for their operation (for example `ORDER BY`, and `GROUP BY`) - `LIMIT` does not have the same impact on these queries.

## 9. Use `WHERE` to filter before `GROUP BY`

Only using `HAVING` to filter the aggregation results of `GROUP BY`. `GROUP BY` is a relatively expensive operation in terms of memory and compute, filter as much before the `GROUP BY` by using the `WHERE` clause and only use `HAVING` to filter the by aggregation function (e.g. `COUNT`, `SUM`).

## 10. `IS` filters are generally faster than `=`

`IS` comparisons are optimized for a specific check and perform up to twice as fast as `=` comparisons. However, they are only available for a limited set of checks:

- `IS NONE`
- `IS NOT NONE`
- `IS TRUE`
- `IS FALSE`