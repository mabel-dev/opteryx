import numpy as np
import pyarrow.compute as pc

from opteryx.engine.attribute_types import TOKEN_TYPES
from .helpers import columns_to_array, groupify_array

# Filter functionality
def arr_op_to_idxs(arr, op, value):
    if op in ["=", "=="]:
        return np.where(arr == value)
    elif op in ["!=", "<>"]:
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
        # MODIFIED FOR OPTERYX
        # some of the lists are saved as sets, which are faster than searching numpy
        # arrays, even with numpy's native functionality.
        mask = []
        for a in arr:
            mask.append(a in value)
        return np.array(mask)
    elif op == "not in":
        # MODIFIED FOR OPTERYX - see comment above
        mask = []
        for a in arr:
            mask.append(a not in value)
        return np.array(mask)
    elif op == "like":
        return pc.match_like(arr, value)
    elif op == "not like":
        return np.invert(pc.match_like(arr, value))
    elif op == "ilike":
        return pc.match_like(arr, value, ignore_case=True)
    elif op == "not ilike":
        return np.invert(pc.match_like(arr, value, ignore_case=True))
    elif op == "~":
        return pc.match_substring_regex(arr, value)
    else:
        raise Exception("Operand {} is not implemented!".format(op))


def _get_values(table, operand):
    """
    MODIFIED FOR OPTERYX
    This allows us to use two identifiers rather than the original implementation which
    forced <identifier> <op> <literal>
    """
    try:
        if operand[1] == TOKEN_TYPES.IDENTIFIER:
            return table.column(operand[0]).to_numpy()
        else:
            return operand[0]
    except:
        print(table.column_names)


def filters(table, filters):
    filters = [filters] if isinstance(filters, tuple) else filters
    # Filter is a list of (col, op, value) tuples
    idxs = np.arange(table.num_rows)
    for (left_op, op, right_op) in filters:  # =, <>, <, >, <=, >=, in and not in
        # MODIFIED FOR OPTERYX
        f_idxs = arr_op_to_idxs(
            _get_values(table, left_op), op, _get_values(table, right_op)
        )
        idxs = idxs[f_idxs]
    return table.take(idxs)


def ifilters(table, filters):
    # ADDED FOR OPTERYX
    # return the indices so we can do unions (OR) and intersections (AND) on the lists
    # of indices to do complex filters
    filters = [filters] if isinstance(filters, tuple) else filters
    # Filter is a list of (col, op, value) tuples
    idxs = np.arange(table.num_rows)
    for (left_op, op, right_op) in filters:
        f_idxs = arr_op_to_idxs(
            _get_values(table, left_op), op, _get_values(table, right_op)
        )
        idxs = idxs[f_idxs]
    return idxs


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
    if table is None:
        print("No data in table")
        return

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
