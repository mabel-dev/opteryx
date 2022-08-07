"""
Original code modified for Opteryx.
"""
import numpy
import pyarrow

from pyarrow import compute

from opteryx.engine.attribute_types import PARQUET_TYPES
from opteryx.engine.attribute_types import PYTHON_TYPES
from opteryx.engine.attribute_types import TOKEN_TYPES

from .helpers import columns_to_array

# Added for Opteryx, comparisons in filter_operators updated to match
# this set is from sqloxide
FILTER_OPERATORS = {
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
    "PGRegexMatch",
    "PGRegexNotMatch",
    "PGRegexIMatch",  # "~*"
    "PGRegexNotIMatch",  # "!~*"
}


def _get_type(var):
    # added for Opteryx
    if isinstance(var, (numpy.ndarray)):
        if isinstance(var[0], numpy.ndarray):
            return "LIST"
        _type = str(var.dtype)
        if _type.startswith("<U"):
            _type = "string"
        return PARQUET_TYPES.get(_type, f"UNSUPPORTED ({str(var.dtype)})")
    if isinstance(var, (pyarrow.Array)):
        return PARQUET_TYPES.get(str(var.type), f"UNSUPPORTED ({str(var.type)})")
    type_name = type(var).__name__
    return PYTHON_TYPES.get(type_name, f"OTHER ({type_name})")


def _check_type(operation, provided_type, valid_types):
    # added for Opteryx
    if provided_type not in valid_types:
        raise TypeError(
            f"Cannot use the {operation} operation on a {provided_type} column, a {valid_types} column is required."
        )


# Filter functionality
def filter_operations(arr, operator, value):
    """
    Execute filter operations, this returns an array of the indexes of the rows that
    match the filter
    """

    # ADDED FOR OPTERYX - if all of the values are null, shortcut

    identifier_type = _get_type(arr)
    literal_type = _get_type(value)

    if operator == "Eq":
        # type checking added for Opteryx
        if value is None and identifier_type == TOKEN_TYPES.NUMERIC:
            # Nones are stored as NaNs, so perform a different test.
            # Tests against None should be IS NONE, not = NONE, this code is for = only
            return numpy.where(numpy.isnan(arr))
        if identifier_type != literal_type and value is not None:
            raise TypeError(
                f"Type mismatch, unable to compare {identifier_type} with {literal_type}"
            )
        return numpy.where(arr == value)
    elif operator == "NotEq":
        return numpy.where(arr != value)
    elif operator == "Lt":
        return numpy.where(arr < value)
    elif operator == "Gt":
        return numpy.where(arr > value)
    elif operator == "LtEq":
        return numpy.where(arr <= value)
    elif operator == "GtEq":
        return numpy.where(arr >= value)
    elif operator == "InList":
        # MODIFIED FOR OPTERYX
        # some of the lists are saved as sets, which are faster than searching numpy
        # arrays, even with numpy's native functionality - choosing the right algo
        # is almost always faster than choosing a fast language.
        return numpy.array([a in value[0] for a in arr], dtype=numpy.bool8)
    elif operator == "NotInList":
        # MODIFIED FOR OPTERYX - see comment above
        return numpy.array([a not in value[0] for a in arr], dtype=numpy.bool8)
    elif operator == "Like":
        # MODIFIED FOR OPTERYX
        # null input emits null output, which should be false/0
        _check_type("LIKE", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_like(arr, value[0])
        return compute.fill_null(matches, False)
    elif operator == "NotLike":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("NOT LIKE", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_like(arr, value[0])
        matches = compute.fill_null(matches, True)
        return numpy.invert(matches)
    elif operator == "ILike":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("ILIKE", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_like(arr, value[0], ignore_case=True)
        return compute.fill_null(matches, False)
    elif operator == "NotILike":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("NOT ILIKE", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_like(arr, value[0], ignore_case=True)
        matches = compute.fill_null(matches, True)
        return numpy.invert(matches)
    elif operator == "PGRegexMatch":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("~", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_substring_regex(arr, value[0])
        return compute.fill_null(matches, False)
    elif operator == "PGRegexNotMatch":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("!~", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_substring_regex(arr, value[0])
        matches = compute.fill_null(matches, True)
        return numpy.invert(matches)
    elif operator == "PGRegexIMatch":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("~*", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_substring_regex(arr, value[0], ignore_case=True)
        return compute.fill_null(matches, False)
    elif operator == "PGRegexNotIMatch":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("!~*", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_substring_regex(arr, value[0], ignore_case=True)
        matches = compute.fill_null(matches, True)
        return numpy.invert(matches)
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
        f_idxs = filter_operations(
            _get_values(table, left_operand),
            operator,
            _get_values(table, right_operand),
        )
        indices = indices[f_idxs]

    return indices


# Drop duplicates
def drop_duplicates(table, columns=None):
    """
    drops duplicates, keeps the first of the set

    MODIFIED FOR OPTERYX
    """
    # Gather columns to arr
    arr = columns_to_array(table, (columns if columns else table.column_names))
    values, indices = numpy.unique(arr, return_index=True)
    del values
    return table.take(indices)
