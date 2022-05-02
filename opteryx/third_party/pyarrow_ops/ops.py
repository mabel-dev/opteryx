"""
Original code modified for Opteryx.
"""
import numpy
import pyarrow.compute as pc

from opteryx.engine.attribute_types import PARQUET_TYPES, PYTHON_TYPES, TOKEN_TYPES

from .helpers import columns_to_array, groupify_array


def _get_type(var):
    if isinstance(var, numpy.ndarray):
        return PARQUET_TYPES.get(str(var.dtype), f"UNSUPPORTED ({str(var.dtype)})")
    t = type(var).__name__
    return PYTHON_TYPES.get(t, f"OTHER ({t})")


# Filter functionality
def arr_op_to_idxs(arr, operator, value):
    if operator in (
        "=",
        "==",
    ):
        # type checking added for Opteryx
        parquet_type = _get_type(arr)
        if value is None and parquet_type == TOKEN_TYPES.NUMERIC:
            # Nones are stored as NaNs, so perform a different test
            return numpy.where(numpy.isnan(arr))
        python_type = _get_type(value)
        if parquet_type != python_type and value is not None:
            raise TypeError(
                f"Type mismatch, unable to compare {parquet_type} with {python_type}"
            )
        return numpy.where(arr == value)
    elif operator in (
        "!=",
        "<>",
    ):
        return numpy.where(arr != value)
    elif operator == "<":
        return numpy.where(arr < value)
    elif operator == ">":
        return numpy.where(arr > value)
    elif operator == "<=":
        return numpy.where(arr <= value)
    elif operator == ">=":
        return numpy.where(arr >= value)
    elif operator == "in":
        # MODIFIED FOR OPTERYX
        # some of the lists are saved as sets, which are faster than searching numpy
        # arrays, even with numpy's native functionality - choosing the right algo
        # is almost always faster than choosing a fast language.
        return numpy.array([a in value for a in arr], dtype=numpy.bool8)
    elif operator == "not in":
        # MODIFIED FOR OPTERYX - see comment above
        return numpy.array([a not in value for a in arr], dtype=numpy.bool8)
    elif operator == "like":
        return pc.match_like(arr, value)
    elif operator == "not like":
        return numpy.invert(pc.match_like(arr, value))
    elif operator == "ilike":
        return pc.match_like(arr, value, ignore_case=True)
    elif operator == "not ilike":
        return numpy.invert(pc.match_like(arr, value, ignore_case=True))
    elif operator == "~":
        return pc.match_substring_regex(arr, value)
    else:
        raise Exception(f"Operator {operator} is not implemented!")


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
        pass


def ifilters(table, filters):
    """
    ADDED FOR OPTERYX
    return the indices so we can do unions (OR) and intersections (AND) on the lists
    of indices to do complex filters
    """
    filters = [filters] if isinstance(filters, tuple) else filters
    # Filter is a list of (col, op, value) tuples
    indices = numpy.arange(table.num_rows)
    for (left_operand, operator, right_operand) in filters:
        f_idxs = arr_op_to_idxs(
            _get_values(table, left_operand),
            operator,
            _get_values(table, right_operand),
        )
        indices = indices[f_idxs]

    return indices


# Drop duplicates
def drop_duplicates(table, on=[]):
    """
    drops duplicates, keeps the first of the set
    """
    # Gather columns to arr
    arr = columns_to_array(table, (on if on else table.column_names))
    dic, counts, sort_idxs, bgn_idxs = groupify_array(arr)
    return table.take(sort_idxs[bgn_idxs])
