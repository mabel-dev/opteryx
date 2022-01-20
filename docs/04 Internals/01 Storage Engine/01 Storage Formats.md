# Storage Formats

## Data Files

**JSONL**

Raw and zStandard compressed.

Files don't need an explicit schema, but each partition must have the same columns
in the same order in every row of every file.

**Parquet**

Parquet offers optimizations not available with other formats which are likely to
improve query performance. If a datasource has query performance issues or is
hot in terms of query use, converting to Parquet is likely to improve performance.
Do not take this as true for all situations, do test for your specific circumstances.

**ORC**

## Other Files

## ZoneMap

## Indexes

*.<index_type>.index

e.g.
datafile.btree.index

## Frame Markers

frame.complete
frame.invalid