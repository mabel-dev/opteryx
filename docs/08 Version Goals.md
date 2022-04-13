# Version Goals

## Initial Beta Release (0.1)

The initial beta release will have at least all of the functionality of the SQL Reader Mabel 0.5, implementation and syntax may vary.

Work to do before release:

- [ ] [General] Improvements to error messages
- [ ] [Caching] Memcached buffer pool (read-aside caching), this should include hit/miss statistics
- [ ] [Planner] Reader should support mabel `by_` segments (basic support)
- [ ] [Operations] `LEFT JOIN`
- [ ] [Bug] `AS` clauses should be respected
- [ ] [Bug] `FOR` syntax should align to T-SQL

## Version 0.2

Beta 0.2 is primarily about optimizations, either internal to the engine or to the user workflow.

- [ ] [Planner] `CROSS JOIN`s with an equi filter between the tables should be rewritten as a `INNER JOIN`
- [ ] [Planner] Planner should plan the partitions and segments to be read, and selection/projection pushdowns - this will allow identical reads to be cached, and cost-based query planning
- [ ] [Planner] Planner should use cost and range information in sidecar metadata files 
- [ ] [Planner] Planner should use cost estimates to pick a `by_` segment
- [ ] [Evaluation] Functions using the result of functions (e.g. LENGTH(AGG_LIST(field)))
- [ ] [Evaluation] Inline operators (e.g. firstname || surname)
- [ ] [Execution] all `JOIN` operators use Cython (or native pyarrow if available)
- [ ] [Execution] `JOIN` batch size based on sizes of input files (e.g. thin right tables have more rows in a batch than wide tables)
- [ ] [Execution] grouping and aggregation to use native pyarrow functionality if available
 
## Version 1.0

Version 1 aims to be feature complete for current known use cases

Version 1.0 goals may be delivered in other beta versions building toward V1.0

- [Planner] CTEs (`WITH`) statements supported
- [Evaluation] `CASE` statements supported
- [Reader] Use asyncio to read data, to improve through-put
- [API] API supported by tools like Superset and PowerBI

