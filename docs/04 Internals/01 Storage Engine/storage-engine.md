
// write functions, need better reporting of failures - it may not be ACID compliant
to fail on partition writes, but it deos provide some transaction capability

// jsonl - read with simdjson into a schema and list of tuples

// read buffer -> memcached


// .profile data (Zone Maps)
- per dataset:
    - columns [type, min, max, count, number unique, sum, number empty]
        - list of columns allows is to use lists of tuples instead of dicts
        - count/min/max/average allows us to quick return simple whole dataset aggregations
- per partition:
    - columns [min, max, count]
        - min/max allows us to skip files if the value isn't in the file


Relation -> DictSet
-> schema - [names, types, range, etc]
-> generator/list of tuples
-> Select (predicate/Expression)
-> Project (col names)
-> Rename ()

-> Join (cartesian join)
-> Union (e.g. for adding partitions together)