# Relation

A relation is a representation of a data set (complete or partial).

PyArrow tables are the format used internally to communicate, usually in 64Mb chunks - most operations act on these chunks, however there are some operations which do not - such as the aggregators, which use Python dictionaries internally.