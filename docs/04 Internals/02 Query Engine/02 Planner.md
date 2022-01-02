# Query Planner

## Query Plan

- BlobScanNode (blobs)
- Project (including renames)
- Select
- CrossJoin
- Sort
- Aggregate - MIN/MAX/AVG etc
- Limit - Top N records
- Distinct
- Union - append sets to each other
- Evaluate -

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
