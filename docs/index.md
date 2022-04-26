# Overview

Opteryx is a SQL query engine to query large data sets designed to run in low-cost serverless environments.

## Use Cases

Cost-optimized environments, querying over ad hoc data stores, and ad hoc querying and analysis.


#### How is it different to SQLite or DuckDB?

Opteryx is solving a different problem in the same space as these solutions. Opteryx avoids loading the dataset into memory unless there is no other option, as such it can query petabytes of data on a single, modest sized node.

This also means that queries are not as fast as solutions like SQLite or DuckDB.

#### How is it different to MySQL or BigQuery?

Opteryx is an ad hoc database, if it can read the files, it can be used to query  the contents of them. This means it can leverage data files used by other systems.

Opteryx is read-only, you can't update or delete data, and it also doesn't have or enforce indexes in your data.

#### How is it differnt to Trino?

Opteryx is designed to run in a serverless environment where there is no persistent state. There is no server or coordinator for Opteryx, the Engine is only running when it is serving queries.

When you are not running queries, your cost to run Opteryx is nil (+). This is particularly useful if you have a small team accessing data.

This also means the Query Engine can scale quickly to respond to demand, running Opteryx in an environment like Cloud Run on GCP, you can scale from 0 to 1000 concurrent queries within seconds - back to 0 almost as quickly.

(+) depending on specifics of your hosting arrangement.