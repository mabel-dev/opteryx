# Query Execution

## Overview

The planner creates a plan as a DAG, we can see simple plans using the `EXPLAIN` keyword, this is added as the first clause in a statement and the planner will return the plan for the query.

To see the plan for this query

~~~sql
SELECT *
  FROM $planets;
~~~

We insert the `EXPLAIN` clause

~~~sql
EXPLAIN
SELECT *
  FROM $planets;
~~~

Which returns

~~~


~~~

Note that the query plan order does not match the order that the query is written, instead it runs in the order required to respond to the query.

The execution engine starts at the node at left of the tree (LIMIT 10) and requests records from the node below (SELECT *), which in turn requests records from the node to the right (WHERE Alive = TRUE).

This continues until we reach a node which can feed data into the tree, this will usually be at a node which reads data files (FROM data_table).

Records are read in batches, and processed in batches, so although the leftmost node will emit it's result when it has 10 records, it may have recieved 1000 records.

These batches are the data frames with broadly align to the database concept of data pages.

## Execution Optimization

Two optimizations are used to improve execution performance:

- Processing of data is done by page-by-page
- Vectorization of aggregation and function calculations