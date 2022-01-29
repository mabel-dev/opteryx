# SQL Aggregates

Aggregates are functions that combine multiple rows into a single value. Aggregates can only be used in the SELECT and HAVING clauses of a SQL query.

When the ORDER BY clause is provided, the values being aggregated are sorted after applying the function. 

Most aggregates require all of the data in the result set in order to complete, for large datasets this may result in memory issues. COUNT, MIN, MAX, and SUM have optimized implementations enabling them to run over massive data sets. 

## General Functions

The table below shows the available general aggregate functions.

Function   | Description
---------- | ----------------------------------------------------------------
`COUNT(a)` | Calculates the number of non None values in column 'a'
`MAX(a)`   | Returns the maximum value in column 'a'
`MIN(a)`   | Returns the minimum value in column 'a'
`SUM(a)`   | Calculates the cumulative sum value for all values in column 'a'

