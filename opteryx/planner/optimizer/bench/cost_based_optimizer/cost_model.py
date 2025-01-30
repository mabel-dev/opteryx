from orso.types import OrsoTypes

# Approximate of the time in seconds (3dp) to compare 1 million records
# None indicates no comparison is possible
BASIC_COMPARISON_COSTS = {
    OrsoTypes.ARRAY: None,
    OrsoTypes.BLOB: None,
    OrsoTypes.BOOLEAN: 0.004,
    OrsoTypes.DATE: 0.01,
    OrsoTypes.DECIMAL: 2.35,
    OrsoTypes.DOUBLE: 0.003,
    OrsoTypes.INTEGER: 0.002,
    OrsoTypes.INTERVAL: None,
    OrsoTypes.STRUCT: None,
    OrsoTypes.TIMESTAMP: 0.009,
    OrsoTypes.TIME: None,
    OrsoTypes.VARCHAR: 0.3,  # varies based on length, this is 50 chars
    OrsoTypes.NULL: None,
    OrsoTypes.BLOB: 0.06,  # varies based on length, this is 50 bytes
}
