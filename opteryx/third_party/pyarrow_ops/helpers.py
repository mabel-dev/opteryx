import numpy as np


def groupify_array(arr):
    # Input: Pyarrow/Numpy array
    # Output:
    #   - 1. Unique values
    #   - 2. Sort index
    #   - 3. Count per unique
    #   - 4. Begin index per unique
    dic, counts = np.unique(arr, return_counts=True)
    sort_idx = np.argsort(arr)
    return dic, counts, sort_idx, [0] + np.cumsum(counts)[:-1].tolist()


def combine_column(table, name):
    return table.column(name).combine_chunks()


f = np.vectorize(hash)


def columns_to_array(table, columns):
    columns = [columns] if isinstance(columns, str) else list(set(columns))
    if len(columns) == 1:
        # return combine_column(table, columns[0]).to_numpy(zero_copy_only=False)
        return f(combine_column(table, columns[0]).to_numpy(zero_copy_only=False))
    else:
        values = [c.to_numpy() for c in table.select(columns).itercolumns()]
        return np.array(list(map(hash, zip(*values))))


# Old helpers

# Splitting tables by columns
def split_array(arr):
    arr = arr.dictionary_encode()
    ind, dic = arr.indices.to_numpy(zero_copy_only=False), arr.dictionary.to_numpy(
        zero_copy_only=False
    )

    if len(dic) < 1000:
        # This method is much faster for small amount of categories, but slower for large ones
        return {v: (ind == i).nonzero()[0] for i, v in enumerate(dic)}
    else:
        idxs = [[] for _ in dic]
        [idxs[v].append(i) for i, v in enumerate(ind)]
        return dict(zip(dic, idxs))


def split(table, columns, group=(), idx=None):
    # idx keeps track of the orginal table index, getting split recurrently
    if not isinstance(idx, np.ndarray):
        idx = np.arange(table.num_rows)
    val_idxs = split_array(combine_column(table, columns[0]))
    if columns[1:]:
        return [
            s
            for v, i in val_idxs.items()
            for s in split(table, columns[1:], group + (v,), idx[i])
        ]
    else:
        return [(group + (v,), i) for v, i in val_idxs.items()]
