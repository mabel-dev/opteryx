# Aggregates

Aggregates are functions that combine multiple rows into a single value. Aggregates can only be used in the `SELECT` and `HAVING` clauses of a SQL query.

When the `ORDER BY` clause is provided, the values being aggregated are sorted after applying the function. 

Most aggregates require all of the data in the result set in order to complete, for large datasets this may result in memory issues; however, some aggregate functions have been written to run over huge datasets, `COUNT`, `MIN`, `MAX`, `SUM`.

## General Functions

The table below shows the available general aggregate functions. (+) indicates aggregates optimized for large datasets. Unless noted, NONE values are ignored.

Function        | Description 
--------------- | ----------------------------------------------------------------
`LIST(a)`       | Values in column 'a' returned as a list
`AVG(a)`        | Average value for all values in column 'a', also `AVERAGE`
`COUNT(a)`      | Number of values in column 'a'
`FIRST(a)`      | First value in column 'a' (includes `NULL`)
`LAST(a)`       | Last value in column 'a', (includes `NULL`)
`MAX(a)`        | Maximum value in column 'a', also `MAXIMUM`
`MEDIAN(a)`     | Middle value for values in column 'a'
`MIN(a)`        | Minimum value in column 'a', also `MINIMUM`
`STDDEV_POP(a)` | Population standard deviation of values in column 'a'
`SUM(a)`        | Cumulative sum value for all values in column 'a'
`VAR_POP(a)`    | Population variance for values in column 'a'
