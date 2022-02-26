# Writing a SQL Engine

## Motivation

No-one in their right mind would write a SQL Engine if they didn't need to. There are a lot of options in the space of providing SQL query access to distributed data. 

Trino (formerly Presto) is a powerful distributed solution.

## Prior Attempts

The data store was designed to be transctional (read a row of data, process it, save the result, repeat), and the first attempt at a SQL engine was also transactional. This aligned well with the tools that we had written to process the data so most of the effort was with translating the SQL syntax to filters for the existing tools to understand, and for some new functionality like supporting GROUP BY.

This provided an acceptable level of functionality for single-table queries (the existing tools only ever read from one table and write to one table) and the engine was implemented into user-facing systems.

Where this approach really struggled was with processing millions of rows, constraints outside the control of the system meant that jobs that ran for longer than 180 seconds were terminated. This generally meant that queries with more than about 70million records (or fewer records but with calculations) timed out. A lot of queries were still able to be run as not everything hit these thresholds, but it couldn't be used for large data analysis.

## Redesign

The redesigned SQL Engine, called Opteryx, is leveraging Parquet to help improve performance. Parquet was assessed for the transactional use case but the optimized JSONL implementation in Mabel consistently outperformed Parquet. However, a SQL Engine is not a transactional use, can in this case Parquet out performs JSONL.