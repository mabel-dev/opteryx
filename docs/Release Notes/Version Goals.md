# Version Goals

## Version 1.0

Version 1.0 aims to be feature complete for current known use cases. 

Version 1.0 goals will be delivered across various minor versions building toward v1.0. These minor releases will also include bug fixes, performance improvements and functional completeness. The items listed below are major dev

- 🔲 **Planner** CTEs (`WITH`) statements supported
- ⬛ **Planner** Read across multiple data sources (e.g. GCS and Postgres in the same query) [v0.2]
- ⬛ **Execution** `JOIN` statements supported [v0.1]
- 🔲 **Execution** `CASE` statements supported
- 🔲 **Execution** Use asyncio/threading to read data, to improve through-put
- ⬛ **Execution** Functions using the result of Functions (e.g. `LENGTH(LIST(field))`) [v0.3]
- ⬛ **Execution** Inline operators (e.g. `firstname || surname`) [v0.3]
