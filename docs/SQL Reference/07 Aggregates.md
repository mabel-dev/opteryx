# Aggregates

Aggregates are functions that combine multiple rows into a single value. Aggregates can only be used in the `SELECT` and `HAVING` clauses of a SQL query.

When the `ORDER BY` clause is provided, the values being aggregated are sorted after applying the function. 

Aggregate functions generally ignore `NULL` values when performing calculations.

## General Functions

The table below shows the available general aggregate functions.

Function             | Description 
-------------------- | ----------------------------------------------------------------
`APPROXIMATE_MEDIAN` | Approximate median of a column with T-Digest algorithm
`AVG`                | The average of a numeric column (alse `MEAN`, and `AVERAGE`)
`COUNT`              | Count the number of values
`COUNT_DISTINCT`     | Count the number of unique values
`LIST`               | The complete list of values 
`MAX`                | The maximum value of a column (also `MAXIMUM`)
`MEDIAN`             | The median of values in a numeric column
`MIN`                | The minimum values of a column (also `MINIMUM`)
`MIN_MAX`            | The minimum and maximum values of a column
`ONE`                | Select a single value from the grouping
`PRODUCT`            | The product of values in a numeric column
`STDDEV`             | The standard deviation of values in a numeric column
`SUM`                | The sum of values in a numeric column
`VARIANCE`           | The variance of values in a numeric column

<!--- `ALL`                | All elements in a column is set to true --->
<!--- `ANY`                | Any elements in a column is set to true --->
<!--- `CUMULATIVE_SUM`     |  --->
<!--- `DISTINCT`           | The list of the unique values  --->
<!--- `MODE`               | The mode of the values  --->
<!--- `QUANTILES`          |   --->