import functools
import numpy

APOLLO_11_DURATION: int = 703115  # we need a constant to use as a seed


def groupify_array(arr):
    # Input: Pyarrow/Numpy array
    # Output:
    #   - 1. Unique values
    #   - 2. Count per unique
    #   - 3. Sort index
    #   - 4. Begin index per unique

    # UPDATED FOR OPTERYX
    dic, counts = numpy.unique(arr, return_counts=True, equal_nan=True)
    sort_idx = numpy.argsort(arr)
    return dic, counts, sort_idx, [0] + numpy.cumsum(counts)[:-1].tolist()


def _hash_value(val, nan=numpy.nan):

    # Added for Opteryx - Original code had bugs relating to distinct and nulls
    if isinstance(val, dict):
        return _hash_value(tuple(val.values()))
    if isinstance(val, (list, numpy.ndarray, tuple)):
        # XOR is faster however, x ^ x == y ^ y but x != y, so we don't use it
        return functools.reduce(
            lambda x, y: _hash_value(f"{y}:{x}", 0), val, APOLLO_11_DURATION
        )
    if val != val or val is None:  # nosemgrep
        # nan is a float, but hash is an int, sometimes we need this to be an int
        return nan
    return hash(val)


def columns_to_array(table, columns):
    """modified for Opteryx"""
    # used for distinct
    columns = [columns] if isinstance(columns, str) else list(dict.fromkeys(columns))

    if len(columns) == 1:
        # FIX https://github.com/mabel-dev/opteryx/issues/98
        # hashing NULL doesn't result in the same value each time
        # FIX https://github.com/mabel-dev/opteryx/issues/285
        # null isn't able to be sorted - replace with nan
        column_values = table.column(columns[0]).to_numpy()
        return numpy.array([_hash_value(el) for el in column_values])

    columns = sorted(set(table.column_names).intersection(columns))
    values = (c.to_numpy() for c in table.select(columns).itercolumns())
    return numpy.array([_hash_value(x) for x in zip(*values)])


def columns_to_array_denulled(table, columns):
    """added for Opteryx"""
    # Used for joins - nulls don't join so we can remove them
    # The columns need to be read in the order provided, so don't set() or order()
    # them anywhere here.
    columns = [columns] if isinstance(columns, str) else columns
    if len(columns) == 1:
        column_values = table.column(columns[0]).drop_null().to_numpy()
        return numpy.array([_hash_value(el) for el in column_values])

    values = (c.to_numpy() for c in table.select(columns).drop_null().itercolumns())
    return numpy.array([_hash_value(x) for x in zip(*values)])
