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