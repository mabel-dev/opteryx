# SQL Aggregates

Aggregates are functions that combine multiple rows into a single value. Aggregates can only be used in the SELECT and HAVING clauses of a SQL query.

When the ORDER BY clause is provided, the values being aggregated are sorted after applying the function. 

Most aggregates require all of the data in the result set in order to complete, for large datasets this may result in memory issues.

## General Functions

The table below shows the available general aggregate functions. (+) indicates aggregates optimized for large datasets. Unless noted, NONE values are ignored.

Function        | Description 
--------------- | ----------------------------------------------------------------
`AVG(a)`        | The average value for all values in column 'a'
`COUNT(a)` +    | The number of values in column 'a'
`FIRST(a)`      | The first value in column 'a' (includes NONE)
`LAST(a)`       | The last value in column 'a', (includes NONE)
`MAX(a)` +      | The maximum value in column 'a'
`MEDIAN(a)`     | The middle value for values in column 'a'
`MIN(a)` +      | The minimum value in column 'a'
`STDDEV_POP(a)` | The population standard deviation of values in column 'a'
`SUM(a)` +      | The cumulative sum value for all values in column 'a'
`VAR_POP(a)`    | The population variance for values in column 'a'
