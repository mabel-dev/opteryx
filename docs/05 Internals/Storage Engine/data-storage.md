
The 'raw' data is JSONL in 64Mb chunks with ZSTD compression. The database-engine MUST
be able to support this format (and other 'raw' formats) to be broadly useful.

A side-car file for each dataset (the folder of partitions) provides information to
optimize database performance, a .map file. This is a JSON file which contains schema
and zonemap information. .index files also exist as sidecar files:

- [].bitmap.index
- [].btree.index

The schema is used to speed up reading into a Relation (as opposed to reading into a
DictSet). Relations are smaller and faster than DictSets and are the internal
representation for the query-engine. 