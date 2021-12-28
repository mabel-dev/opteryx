

## DictSet

List of Dictionaries - functionally rich and flexible, but uses more memory and is
less performant.

## Relation

List of Tuples and sidecar profile information - memory and speed performant. This is
the preferred internal representation for data but won't be used if the system cannot
work out type information about columns at read time, or the data contains unsupported
column types.

