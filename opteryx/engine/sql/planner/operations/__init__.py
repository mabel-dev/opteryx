from .base_plan_node import BasePlanNode

# BaseOperator

# CREATE INDEX ON
# ANALYZE

- PartitionReaderNode (read a partition)
- ProjectNode (remove unwanted columns including renames)
- SelectionNode (find records matching a predicate)
- JoinNode (currently only INNER JOIN)
- SortNode (order a relation by given keys)
- GroupNode (put a relation into groups - GROUP BY)
- AggregateNode (group by MIN/MAX/AVG etc
- CombineNode (combine sketches and aggregations)
- Limit - Top N records
- Distinct
- Merge - append sets to each other
- Evaluate ()
- Index (create an index, can be temporary or persisted)
