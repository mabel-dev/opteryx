# Schema Evolution

[Opteryx](https://mabel-dev.github.io/opteryx/) has support for in-place relation evolution. You can evolve a table schema or change a partition layout without requiring existing data to be rewritten or migrated to a new dataset.

Schema of the data is determined by the first page read to respond to a query, new columns are removed and missing columns are null-filled. This allows graceful handling of pages with different schemas, but may result in the appearance of missing data as columns not found in the first page are removed.

Opteryx supports the following schema evolution changes:

- **Add** - new columns can be added - these are removed if not present on the first page read
- **Remove** - removed columns are null-filled
- **Reorder** - the order of columns can be changed
- **Partitioning** - partition resolution can be changed

!!! Note
    Renamed columns will behave like the column has been removed and a new column added.

Opteryx has limited support for column types changing, some changes within the same broad type (e.g. between numeric types and date resolutions) are supported, but these are not all supported and changing between types is not supported.

### Partitioning

Changes to partition schemes are handled transparently. For example, data using Mabel partitioning moving from a daily to an hourly partition layout can occur without requiring any other changes to the configuration of the query engine. However, moving between no partition and partitioning (or vise-versa) is not supported.
