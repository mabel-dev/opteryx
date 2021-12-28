## Types

Type   | Description
------ | -----------------------
B+Tree | High cardinality
Binary | Unique values
Bitmap | Low cardinality

[Cardinality](https://en.wikipedia.org/wiki/Cardinality_(SQL_statements))

## B+Tree Index

## Binary Index

## Bitmap Index


multiple index strategies

- sets with less than 1000 rows are not automatically indexed, the cost of using an
  index is likely to exceed the benefits.

- unindexed data have Min/Max indexes created on read, this is used for bulk selection,
  i.e. determining if a value is in a set

- enumeration fields (indicated by very low cardinality) have a bitmap index.

- fields which are unique have a binary index.

- fields with some duplication have a b+tree index.

- clustered index are implemented per blob, rather than across datasets. 

Indexes can be single columns only.

Writers automatically create a ZoneMap, this contains information to assist with
indexing strategies including:
- Column Types
- Column Min/Max 
- Cardinality Estimates
- Missing (null) value counts

https://en.wikipedia.org/wiki/Block_Range_Index

Mabel implementation is not identical to Oracle, but more information about Zone Maps
can be found here:
https://docs.oracle.com/database/121/DWHSG/zone_maps.htm#DWHSG-GUID-BEA5ACA1-6718-4948-AB38-1F2C0335FDE4