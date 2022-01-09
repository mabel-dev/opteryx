# Query Optimization

## 1. Avoid `SELECT *`

Selecting only the fields you need to be returned improves query performance by
reducing the amount of data that is processed internally.

A principle the Query Optimizer uses is to eliminate rows and columns to process as
early as possible, `SELECT *` removes the option to remove columns from the data being
processed.