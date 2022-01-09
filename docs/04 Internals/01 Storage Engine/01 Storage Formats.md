# Storage Formats

## Data Files

**JSONL**

Raw and zStandard compressed.

Files don't need an explicit schema, but each partition must have the same columns
in the same order in every row of every file.

**Parquet**

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