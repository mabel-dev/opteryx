

- if a Index can be used to respond to a query - e.g. only one column is needed
  from a table, and it has a binary tree index, just use the index - OR if the
  query is a COUNT and we have a BRIN, use that.

- JOINS
- Query Optimizer