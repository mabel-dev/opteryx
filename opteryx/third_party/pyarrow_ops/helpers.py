import numpy


def groupify_array(arr):
    # Input: Pyarrow/Numpy array
    # Output:
    #   - 1. Unique values
    #   - 2. Count per unique
    #   - 3. Sort index
    #   - 4. Begin index per unique
    dic, counts = numpy.unique(arr, return_counts=True, equal_nan=True)
    sort_idx = numpy.argsort(arr)
    return dic, counts, sort_idx, [0] + numpy.cumsum(counts)[:-1].tolist()


def combine_column(table, name):
    return table.column(name).combine_chunks()


def _hash(val):
    # EDGE CASE FOR https://github.com/mabel-dev/opteryx/issues/98
    # hashing NULL doesn't result in the same value each time
    if all(v != v for v in val):
        return numpy.nan
    return hash(val)


def columns_to_array(table, columns):
    """modified for Opteryx"""
    columns = [columns] if isinstance(columns, str) else list(set(columns))
    if len(columns) == 1:
        # FIX https://github.com/mabel-dev/opteryx/issues/98
        # hashing NULL doesn't result in the same value each time
        # FIX https://github.com/mabel-dev/opteryx/issues/285
        # null isn't able to be sorted - replace with nan
        return [
            numpy.nan if el != el else hash(el)
            for el in combine_column(table, columns[0]).to_numpy(zero_copy_only=False)
        ]
    values = [c.to_numpy() for c in table.select(columns).itercolumns()]
    return numpy.array(list(map(_hash, zip(*values))))
