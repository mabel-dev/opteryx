# Query Engine

> If you are interested in how databases work, I recommend the resources from [The CMU Database Group](https://db.cs.cmu.edu/) and the collection of resources at [Awesome Database Learning](https://github.com/pingcap/awesome-database-learning).

The Opteryx query engine has the following key components and general process:

1) The Parser & Lexer, which recieves the user SQL and builds an Abstract Syntax Tree (AST).
2) The Planner, which recieves the AST and builds a Query Plan.
3) The Query Executor, which recieves the Query Plan and returns the result dataset.

## Parser & Lexer

The primary goal of the Parser and Lexer (which in some engines is two separate components) is to interpret the SQL provided by the user. This is generally done in two steps, the first is the break the query into separate tokens (or words) and the second is to understand the meaning of those tokens.

For example for this statement

~~~sql
SELECT SELECT
  FROM FROM
~~~

Understand that we're requesting the field `SELECT` from the relation `FROM`.

Opteryx uses [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs) as it's Parser and Lexer, as a Rust library, Opteryx uses [sqloxide](https://github.com/wseaton/sqloxide) which creates Python bindings for sqlparser-rs. Opteryx does not support all features and functionality provided by this library.

This sqlparser-rs interprets all SQL except for the Temporal `FOR` clause which is handled separately.

## Query Planner

The Query planner's primary goal is to convert the AST into a plan to respond to the query. The Query Plan is described in a Directed Acyclic Graph (DAG) with the nodes that acquire the raw data, usually from storage, at the start of the flow and the node that forms the data to return to the user (usually the `SELECT` step) at the end.

The DAG is made of different nodes which process the data as they pass through then node. Different node types exist for processing actions like Aggregations (`GROUP BY`), Selection (`WHERE`) and Distinct (`DISTINCT`). There are currently 17 different Node types the planner can use to build a plan to respond to a query.

Most SQL Engines include an Optimization step as part of the planner, Opteryx currently does not perform plan-based optimizations.

Query plans follow a generally accepted order of execution, which does not match the order queries are usually written in:

![OPERATOR ORDER](operator-order.svg) 

The planner ensures the processes to be applied to the data reflect this order.

The Query Plan can be seen for a given query using the `EXPLAIN` query.

## Query Executor

The goal of the Query Executor is to produce the results for the user. It takes the Plan and executes the steps in the plan.

Opteryx implements a vectorized Volcano model executor. This means that the planner starts at the node closest to the end of the plan (e.g. `LIMIT`) and asks it for a page of data. This node asks its preceeding node for a page of data, etc etc until it gets to the node which aquires data from source. The data is then processed by each node until it is returned to the `LIMIT` node at the end.
