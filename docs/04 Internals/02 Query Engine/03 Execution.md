# Query Execution

- work out which partitions need to be read
- create workers to read each of the partitions (locally or remotely)
- submit part of the query plan DAG to each of the workers
- MERGE (union) or COMBINE (aggregate) the results
- Return result