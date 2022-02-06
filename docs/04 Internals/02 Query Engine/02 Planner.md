# Query Planner

The Opteryx Query Planner is 

## Query Plan

Query Plans can contain the following steps:

Step       | Description
---------- | -------------
Aggregate  | Perform aggregations such as COUNT and MAX
Evaluation | Evaluate functions
Distinct   | Remove duplicate records
Reader     | Read datasets
Limit      | Return up to a stated number of records
Offset     | Skip a number of records
Projection | Remove unwanted columns
Selection  | Remove unwanted rows

## Query Plan Optimizer

The Query Plan is naive and performs no optimizations.