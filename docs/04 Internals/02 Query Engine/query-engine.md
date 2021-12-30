
// implemented queries



// sql parser -> relational algebra

// relational algebra -> graph (query planning)

# BlobScanNode (blobs)
# Project (including renames)
# Select
# CrossJoin
# Sort
# Aggregate - MIN/MAX/AVG etc
# Limit - Top N records
# Distinct
# Union - append sets to each other
# Evaluate -

# * RemoteScan (e.g. ODBC databases)


// query optimizer
Define static rules that transform logical operators to a physical plan.
→ Perform most restrictive selection early
→ Perform all selections before joins
→ Predicate/Limit/Projection pushdowns
→ Join ordering based on cardinality