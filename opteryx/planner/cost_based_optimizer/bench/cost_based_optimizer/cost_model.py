from orso.types import OrsoTypes

# Approximate of the time in seconds (2dp) to compare 1 million records
# None indicates no comparison is possible
BASIC_COMPARISON_COSTS = {
    OrsoTypes.ARRAY: None,
    OrsoTypes.BLOB: None,
    OrsoTypes.BOOLEAN: 0.07,
    OrsoTypes.DATE: 0.08,
    OrsoTypes.DECIMAL: 2.3,
    OrsoTypes.DOUBLE: 0.07,
    OrsoTypes.INTEGER: 0.07,
    OrsoTypes.INTERVAL: None,
    OrsoTypes.STRUCT: None,
    OrsoTypes.TIMESTAMP: 0.08,
    OrsoTypes.TIME: None,
    OrsoTypes.VARCHAR: 0.5,  # varies based on length, this is about 50 characters
    OrsoTypes.NULL: None,
}

BASIC_COMPARISONS = {
    "Eq",
    "NotEq",
    "Gt",
    "GtEq",
    "Lt",
    "LtEq",
    "Like",
    "ILike",
    "NotLike",
    "NotILike",
    "InList",
    "SimilarTo",
    "NotSimilarTo",
}
