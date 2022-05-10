import numpy as np


def groupify_array(arr):
    # Input: Pyarrow/Numpy array
    # Output:
    #   - 1. Unique values
    #   - 2. Count per unique
    #   - 3. Sort index
    #   - 4. Begin index per unique
    dic, counts = np.unique(arr, return_counts=True)
    sort_idx = np.argsort(arr)
    return dic, counts, sort_idx, [0] + np.cumsum(counts)[:-1].tolist()


def combine_column(table, name):
    return table.column(name).combine_chunks()


def _hash(val):
    # EDGE CASE FOR https://github.com/mabel-dev/opteryx/issues/98
    # hashing NULL doesn't result in the same value each time
    try:
        if all(np.isnan(v) for v in val):
            return 0
    except TypeError:  # np.isnan fails on strings
        pass
    return hash(val)


def columns_to_array(table, columns):
    """modified for Opteryx"""
    columns = [columns] if isinstance(columns, str) else list(set(columns))
    if len(columns) == 1:
        # FIX https://github.com/mabel-dev/opteryx/issues/98
        # hashing NULL doesn't result in the same value each time
        return combine_column(table, columns[0]).to_numpy(zero_copy_only=False)
    values = [c.to_numpy() for c in table.select(columns).itercolumns()]
    return np.array(list(map(_hash, zip(*values))))
