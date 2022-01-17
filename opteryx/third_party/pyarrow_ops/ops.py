import numpy as np
import pyarrow as pa
from .helpers import columns_to_array, groupify_array

# Filter functionality
def arr_op_to_idxs(arr, op, value):
    # Cast value to type arr
    try:
        value = np.array(value, dtype=arr.dtype)
    except:
        raise Exception("Cannot downcast {} to data type {}".format(value, arr.dtype))

    if op in ["=", "=="]:
        return np.where(arr == value)
    elif op == "!=":
        return np.where(arr != value)
    elif op == "<":
        return np.where(arr < value)
    elif op == ">":
        return np.where(arr > value)
    elif op == "<=":
        return np.where(arr <= value)
    elif op == ">=":
        return np.where(arr >= value)
    elif op == "in":
        mask = np.isin(arr, value)
        return np.arange(len(arr))[mask]
    elif op == "not in":
        mask = np.invert(np.isin(arr, value))
        return np.arange(len(arr))[mask]
    else:
        raise Exception("Operand {} is not implemented!".format(op))


def filters(table, filters):
    filters = [filters] if isinstance(filters, tuple) else filters
    # Filter is a list of (col, op, value) tuples
    idxs = np.arange(table.num_rows)
    for (col, op, value) in filters:  # = or ==, !=, <, >, <=, >=, in and not in
        arr = table.column(col).to_numpy()
        f_idxs = arr_op_to_idxs(arr[idxs], op, value)
        idxs = idxs[f_idxs]
    return table.take(idxs)


# Drop duplicates
def drop_duplicates(table, on=[], keep="first"):
    # Gather columns to arr
    arr = columns_to_array(table, (on if on else table.column_names))

    # Groupify
    dic, counts, sort_idxs, bgn_idxs = groupify_array(arr)

    # Gather idxs
    if keep == "last":
        idxs = (np.array(bgn_idxs) - 1)[1:].tolist() + [len(sort_idxs) - 1]
    elif keep == "first":
        idxs = bgn_idxs
    elif keep == "drop":
        idxs = [i for i, c in zip(bgn_idxs, counts) if c == 1]
    return table.take(sort_idxs[idxs])


# Show for easier printing
def head(table, n=5, max_width=100):
    if table.num_rows == 0:
        print("No data in table")
        return

    # Extract head data
    t = table.slice(length=n)
    head = {k: list(map(str, v)) for k, v in t.to_pydict().items()}

    # Calculate width
    col_width = list(map(len, head.keys()))
    data_width = [max(map(len, h)) for h in head.values()]

    # Print data
    data = [list(head.keys())] + [
        [head[c][i] for c in head.keys()] for i in range(t.num_rows)
    ]
    for i in range(len(data)):
        adjust = [
            w.ljust(max(cw, dw) + 2)
            for w, cw, dw in zip(data[i], col_width, data_width)
        ]
        print(
            ("Row  " if i == 0 else str(i - 1).ljust(5)) + "".join(adjust)[:max_width]
        )
    print("\n")
