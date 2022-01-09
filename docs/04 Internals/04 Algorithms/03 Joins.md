# Joins

- if one dataset is small - we keep that in memory and pass over the second dataset

- if both are large, a Binary Tree is created for the set with the smallest number
  of unique values, we then go over the larger set, looking up the join in the index,
  no match - discard, match - get the value from the smaller set.