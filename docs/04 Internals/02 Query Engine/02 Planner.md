# Query Planner

## Query Plan

- PartitionReaderNode (read a partition, includes selection and projection pushdowns)
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

## Query Plan Optimizer



// query optimizer
Define static rules that transform logical operators to a physical plan.
→ Perform most restrictive selection early
→ Perform all selections before joins
→ Predicate/Limit/Projection pushdowns
→ Join ordering based on cardinality


-> if it's count(*)
    if we can get the result from the zonemap - do that
    otherwise - reduce the record to a hash only as early as possible


-> if we have a group by with a lot of duplication (cardinality estimates), use the
   index tree to build the groups.
