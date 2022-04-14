# Version Goals

## Initial Beta Release (0.1)

The initial beta release will have at least all of the functionality of the SQL Reader Mabel 0.5, implementation and syntax may vary.

Work to do before release:

- 🔲 **[General]** Improvements to error messages
- 🔲 **[Caching]** Memcached buffer pool (cache-aside), this should include hit/miss statistics
- 🔲 **[Planner]** Planner should plan the reads rather than the Reader
- 🔲 **[Planner]** Reader should support mabel `by_` segments (basic support)
- ⬛ **[Execution]** `LEFT JOIN`

## Version 0.2

Beta 0.2 is primarily about optimizations, either internal to the engine or to the user workflow.

- 🔲 **[Planner]** `CROSS JOIN`s with an equi filter between the tables should be internally rewritten as an `INNER JOIN`
- 🔲 **[Planner]** Planner to determine selection/projection pushdowns
- 🔲 **[Reader]** Reads should be cached so that identical reads (segments, pushdowns) can be read from 'results' cache
- 🔲 **[Planner]** Planner should use cost and range information (BRIN) in sidecar metadata files 
- 🔲 **[Planner]** Planner should use cost estimates to pick a `by_` segment
- 🔲 **[Execution]** Functions using the result of functions (e.g. LENGTH(AGG_LIST(field)))
- 🔲 **[Execution]** Inline operators (e.g. firstname || surname)
- 🔲 **[Execution]** all `JOIN` operators use Cython (or native pyarrow if available)
- 🔲 **[Execution]** `JOIN` batch size based on sizes of input files (e.g. thin right tables have more rows in a batch than wide tables)
- 🔲 **[Execution]** grouping and aggregation to use native pyarrow functionality if available
 
## Version 1.0

Version 1 aims to be feature complete for current known use cases

Version 1.0 goals may be delivered in other beta versions building toward v1.0

- 🔲 **[Planner]** CTEs (`WITH`) statements supported
- 🔲 **[Execution]** `CASE` statements supported
- 🔲 **[Execution]** Use asyncio to read data, to improve through-put
- 🔲 **[API]** API supported by tools like Superset and PowerBI

