"""
Original code modified for Opteryx.
"""
import numpy
import pyarrow

from pyarrow import compute

from opteryx.attribute_types import PARQUET_TYPES
from opteryx.attribute_types import PYTHON_TYPES
from opteryx.attribute_types import TOKEN_TYPES

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
    "SimilarTo",
    "NotSimilarTo",
    "PGRegexMatch",
    "NotPGRegexMatch",
    "PGRegexNotMatch",
    "PGRegexIMatch",  # "~*"
    "NotPGRegexIMatch",  # "!~*"
    "PGRegexNotIMatch",  # "!~*"
}


def _get_type(var):
    # added for Opteryx
    if isinstance(var, (numpy.ndarray)):
        _type = str(var.dtype)
        if _type.startswith("<U"):
            _type = "string"
        return PARQUET_TYPES.get(_type, f"UNSUPPORTED ({str(var.dtype)})")
    if isinstance(var, (pyarrow.Array)):
        return PARQUET_TYPES.get(str(var.type), f"UNSUPPORTED ({str(var.type)})")
    if isinstance(var, list):
        return PYTHON_TYPES.get(
            type(var[0]).__name__, f"UNSUPPORTED ({type(var[0]).__name__})"
        )
    type_name = type(var).__name__  # pragma: no cover
    return PYTHON_TYPES.get(type_name, f"OTHER ({type_name})")  # pragma: no cover


def _check_type(operation, provided_type, valid_types):
    # added for Opteryx
    if provided_type not in valid_types:  # pragma: no cover
        raise TypeError(
            f"Cannot use the {operation} operation on a {provided_type} column, a {valid_types} column is required."
        )


def filter_operations_for_display(arr, operator, value):
    """
    Wrapped for Opteryx added to correctly handle null semantics.

    This returns an array with tri-state boolean (tue/false/none). This is
    for use where an expression result is deplayed to users.
    """

    # if the input is a table, get the first column
    if isinstance(value, pyarrow.Table):  # pragma: no cover
        value = [value.columns[0].to_numpy()]

    # work out which rows we're going to actually evaluate
    # we're working out if either array has a null value so we can exclude them
    # from the actual evaluation.
    #   True = values, False = null
    record_count = len(arr)
    null_arr = compute.is_null(arr, nan_is_null=True)
    null_val = compute.is_null(value, nan_is_null=True)
    null_positions = numpy.logical_or(null_arr, null_val)

    # if there's no non-null values, stop here
    if all(null_positions):
        return numpy.full(record_count, None)

    any_null = any(null_positions)
    null_positions = numpy.invert(null_positions)

    compressed = False
    if any_null and isinstance(arr, numpy.ndarray) and isinstance(value, numpy.ndarray):
        # if we have nulls and both columns are numpy arrays, we can speed things
        # up by removing the nulls from the calculations, we add the rows back in
        # later
        arr = arr.compress(null_positions)
        value = value.compress(null_positions)
        compressed = True

    # do the evaluation
    results_mask = _inner_filter_operations(arr, operator, value)

    if compressed:
        # fill the result set
        results = numpy.full(record_count, -1, numpy.int8)
        results[numpy.nonzero(null_positions)] = results_mask
        # build tri-state response
        return [bool(r) if r != -1 else None for r in results]

    return results_mask


def filter_operations(arr, operator, value):
    """
    Wrapped for Opteryx added to correctly handle null semantics.

    This is used where the filter actually filters records so is bi-state
    (true/false) where null is coaleced to false.
    """

    # if the input is a table, get the first column
    if isinstance(value, pyarrow.Table):  # pragma: no cover
        value = [value.columns[0].to_numpy()]

    # work out which rows we're going to actually evaluate
    # we're working out if either array has a null value so we can exclude them
    # from the actual evaluation.
    #   True = values, False = null
    record_count = len(arr)
    null_arr = compute.is_null(arr, nan_is_null=True)
    null_val = compute.is_null(value, nan_is_null=True)
    null_positions = numpy.logical_or(null_arr, null_val)

    # if there's no non-null values, stop here
    if all(null_positions):
        return numpy.full(record_count, False)

    any_nulls = any(null_positions)
    null_positions = numpy.invert(null_positions)

    compressed = False
    if (
        any_nulls
        and isinstance(arr, numpy.ndarray)
        and isinstance(value, numpy.ndarray)
    ):
        # if we have nulls and both columns are numpy arrays, we can speed things
        # up by removing the nulls from the calculations, we add the rows back in
        # later
        arr = arr.compress(null_positions)
        value = value.compress(null_positions)
        compressed = True

    # do the evaluation
    results_mask = _inner_filter_operations(arr, operator, value)

    # fill the result set
    if compressed:
        results = numpy.full(record_count, False, numpy.bool_)
        results[numpy.nonzero(null_positions)] = results_mask
        return results

    return results_mask


# Filter functionality
def _inner_filter_operations(arr, operator, value):
    """
    Execute filter operations, this returns an array of the indexes of the rows that
    match the filter
    """
    # ADDED FOR OPTERYX
    identifier_type = _get_type(arr)
    literal_type = _get_type(value)

    if operator == "Eq":
        if identifier_type != literal_type and value is not None:  # pragma: no cover
            raise TypeError(
                f"Type mismatch, unable to compare {identifier_type} with {literal_type}"
            )
        return compute.equal(arr, value).to_numpy(False).astype(dtype=bool)
    elif operator == "NotEq":
        return compute.not_equal(arr, value).to_numpy(False).astype(dtype=bool)
    elif operator == "Lt":
        return compute.less(arr, value).to_numpy(False).astype(dtype=bool)
    elif operator == "Gt":
        return compute.greater(arr, value).to_numpy(False).astype(dtype=bool)
    elif operator == "LtEq":
        return compute.less_equal(arr, value).to_numpy(False).astype(dtype=bool)
    elif operator == "GtEq":
        return compute.greater_equal(arr, value).to_numpy(False).astype(dtype=bool)
    elif operator == "InList":
        # MODIFIED FOR OPTERYX
        # some of the lists are saved as sets, which are faster than searching numpy
        # arrays, even with numpy's native functionality - choosing the right algo
        # is almost always faster than choosing a fast language.
        return numpy.array([a in value[0] for a in arr], dtype=numpy.bool8)  # [#325]?
    elif operator == "NotInList":
        # MODIFIED FOR OPTERYX - see comment above
        return numpy.array(
            [a not in value[0] for a in arr], dtype=numpy.bool8
        )  # [#325]?
    elif operator == "Contains":
        # ADDED FOR OPTERYX
        return numpy.array(
            [None if v is None else (arr[0] in v) for v in value], dtype=numpy.bool8
        )
    elif operator == "NotContains":
        # ADDED FOR OPTERYX
        return numpy.array(
            [None if v is None else (arr[0] not in v) for v in value], dtype=numpy.bool8
        )  # [#325]?
    elif operator == "Like":
        # MODIFIED FOR OPTERYX
        # null input emits null output, which should be false/0
        _check_type("LIKE", identifier_type, (TOKEN_TYPES.VARCHAR))
        return (
            compute.match_like(arr, value[0]).to_numpy(False).astype(dtype=bool)
        )  # [#325]
    elif operator == "NotLike":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("NOT LIKE", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = (
            compute.match_like(arr, value[0]).to_numpy(False).astype(dtype=bool)
        )  # [#325]
        return numpy.invert(matches)
    elif operator == "ILike":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("ILIKE", identifier_type, (TOKEN_TYPES.VARCHAR))
        return (
            compute.match_like(arr, value[0], ignore_case=True)
            .to_numpy(False)
            .astype(dtype=bool)
        )  # [#325]
    elif operator == "NotILike":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("NOT ILIKE", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_like(arr, value[0], ignore_case=True)  # [#325]
        return numpy.invert(matches)
    elif operator in ("PGRegexMatch", "SimilarTo"):
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("~", identifier_type, (TOKEN_TYPES.VARCHAR))
        return (
            compute.match_substring_regex(arr, value[0])
            .to_numpy(False)
            .astype(dtype=bool)
        )  # [#325]
    elif operator in ("PGRegexNotMatch", "NotSimilarTo"):
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("!~", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_substring_regex(arr, value[0])  # [#325]
        return numpy.invert(matches)
    elif operator == "PGRegexIMatch":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("~*", identifier_type, (TOKEN_TYPES.VARCHAR))
        return (
            compute.match_substring_regex(arr, value[0], ignore_case=True)
            .to_numpy(False)
            .astype(dtype=bool)
        )  # [#325]
    elif operator == "PGRegexNotIMatch":
        # MODIFIED FOR OPTERYX - see comment above
        _check_type("!~*", identifier_type, (TOKEN_TYPES.VARCHAR))
        matches = compute.match_substring_regex(
            arr, value[0], ignore_case=True
        )  # [#325]
        return numpy.invert(matches)
    else:
        raise Exception(f"Operator {operator} is not implemented!")  # pragma: no cover


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


# Show for easier printing
def head(table, n=5, max_width=100):
    # Updated to yield rather than print for Opteryx
    if table == set():  # pragma: no cover
        yield "No data in table"
        return
    if table.num_rows == 0:
        yield "No data in table"
        return

    # Extract head data
    if n > 0:
        t = table.slice(length=n)
    else:
        t = table
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
        yield ("Row  " if i == 0 else str(i - 1).ljust(5)) + "".join(adjust)[:max_width]
