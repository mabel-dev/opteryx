# Query Planner

## Query Plan Steps

Query Plans can contain the following steps:

Step       | Description
---------- | -------------
Aggregate  | Perform aggregations such as COUNT and MAX
Distinct   | Remove duplicate records
Explain    | Plan Explainer
Join       | Join Relations
Limit      | Return up to a stated number of records
Offset     | Skip a number of records
Projection | Remove unwanted columns
Reader     | Read datasets
Selection  | Remove unwanted rows

## Query Plan Optimizer

The Query Plan is naive and performs no optimizations.
