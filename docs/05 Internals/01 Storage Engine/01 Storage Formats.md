# Storage Formats

## Supported Data Files

### Parquet

Parquet offers optimizations not available with other formats which improve query performance. If a datasource has query performance issues or is hot in terms of query use, converting to Parquet is likely to improve performance.

Do not take this as true for all situations, do test for your specific circumstances.

### JSONL

[JSONL](https://jsonlines.org/) and zStandard compressed JSONL files.

Files don't need an explicit schema, but each partition must have the same columns in the same order in every row of every file.

Data types are inferred from the records, where data types are not consistent, the read will fail.
