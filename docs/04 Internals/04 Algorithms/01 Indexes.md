## Types

Type          | Description
------------- | -----------------------
Binary Tree   | 
MinMax        |

[Cardinality](https://en.wikipedia.org/wiki/Cardinality_(SQL_statements))


## Index strategy

- sets with less than 1000 rows are not indexed, the cost of using an
  index is likely to exceed the benefits.

- columns being used for SARGABLE comparisons later in the DAG have a 
  tree created for them.

- Indexes can be single columns only.
