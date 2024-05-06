"""
Original code modified for Opteryx.
"""

import numpy
import pyarrow
from orso.types import OrsoTypes
from pyarrow import compute

from opteryx.compiled import list_ops

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
    "PGRegexNotMatch",
    "PGRegexIMatch",  # "~*"
    "NotPGRegexIMatch",  # "!~*"
    "PGRegexNotIMatch",  # "!~*"
    "BitwiseOr",  # |
}


def filter_operations(arr, left_type, operator, value, right_type):
    """
    Wrapped for Opteryx added to correctly handle null semantics.

    This returns an array with tri-state boolean (tue/false/none);
    if being used for display use as is, if being used for filtering, none is false.
    """
    # if the input is a table, get the first column
    if isinstance(value, pyarrow.Table):  # pragma: no cover
        value = value.column(0).to_numpy()

    compressed = False

    if operator not in ("InList", "NotInList"):
        # compressing ARRAY columns is VERY SLOW
        morsel_size = len(arr)

        # compute null positions
        null_positions = numpy.logical_or(
            compute.is_null(arr, nan_is_null=True),
            compute.is_null(value, nan_is_null=True),
        )

        # Early exit if all values are null
        if null_positions.all():
            return pyarrow.array([None] * morsel_size, type=pyarrow.bool_())

        if (
            null_positions.any()
            and isinstance(arr, numpy.ndarray)
            and isinstance(value, numpy.ndarray)
        ):
            # if we have nulls and both columns are numpy arrays, we can speed things
            # up by removing the nulls from the calculations, we add the rows back in
            # later
            valid_positions = ~null_positions
            arr = arr.compress(valid_positions)
            value = value.compress(valid_positions)
            compressed = True

    if OrsoTypes.INTERVAL in (left_type, right_type):
        from opteryx.custom_types.intervals import INTERVAL_KERNELS

        function = INTERVAL_KERNELS.get((left_type, right_type, operator))
        if function is None:
            from opteryx.exceptions import UnsupportedTypeError

            raise UnsupportedTypeError(
                f"Cannot perform {operator.upper()} on {left_type} and {right_type}."
            )

        results_mask = function(arr, left_type, value, right_type, operator)
    else:
        # do the evaluation
        results_mask = _inner_filter_operations(arr, operator, value)

    if compressed:
        # fill the result set
        results = numpy.array([None] * morsel_size, dtype=object)
        numpy.place(results, valid_positions, results_mask)

        # build tri-state response, PyArrow supports tristate, numpy does not
        return pyarrow.array(results, type=pyarrow.bool_())

    return results_mask


# Filter functionality
def _inner_filter_operations(arr, operator, value):
    """
    Execute filter operations, this returns an array of the indexes of the rows that
    match the filter
    """
    # ADDED FOR OPTERYX

    if operator == "Eq":
        return compute.equal(arr, value).to_numpy(False).astype(dtype=bool)
    if operator == "NotEq":
        return compute.not_equal(arr, value).to_numpy(False).astype(dtype=bool)
    if operator == "Lt":
        return compute.less(arr, value).to_numpy(False).astype(dtype=bool)
    if operator == "Gt":
        return compute.greater(arr, value).to_numpy(False).astype(dtype=bool)
    if operator == "LtEq":
        return compute.less_equal(arr, value).to_numpy(False).astype(dtype=bool)
    if operator == "GtEq":
        return compute.greater_equal(arr, value).to_numpy(False).astype(dtype=bool)
    if operator == "InList":
        # MODIFIED FOR OPTERYX
        values = set(value[0])
        return numpy.array([a in values for a in arr], dtype=numpy.bool_)  # [#325]?
    if operator == "NotInList":
        # MODIFIED FOR OPTERYX - see comment above
        values = set(value[0])
        return numpy.array([a not in values for a in arr], dtype=numpy.bool_)  # [#325]?
    if operator == "Like":
        # MODIFIED FOR OPTERYX
        # null input emits null output, which should be false/0
        return compute.match_like(arr, value[0]).to_numpy(False).astype(dtype=bool)  # [#325]
    if operator == "NotLike":
        # MODIFIED FOR OPTERYX - see comment above
        matches = compute.match_like(arr, value[0]).to_numpy(False).astype(dtype=bool)  # [#325]
        return numpy.invert(matches)
    if operator == "ILike":
        # MODIFIED FOR OPTERYX - see comment above
        return (
            compute.match_like(arr, value[0], ignore_case=True).to_numpy(False).astype(dtype=bool)
        )  # [#325]
    if operator == "NotILike":
        # MODIFIED FOR OPTERYX - see comment above
        matches = compute.match_like(arr, value[0], ignore_case=True)  # [#325]
        return numpy.invert(matches)
    if operator == "RLike":
        # MODIFIED FOR OPTERYX - see comment above
        return (
            compute.match_substring_regex(arr, value[0]).to_numpy(False).astype(dtype=bool)
        )  # [#325]
    if operator == "NotRLike":
        # MODIFIED FOR OPTERYX - see comment above
        matches = compute.match_substring_regex(arr, value[0])  # [#325]
        return numpy.invert(matches)
    if operator == "PGRegexIMatch":
        # MODIFIED FOR OPTERYX - see comment above
        return (
            compute.match_substring_regex(arr, value[0], ignore_case=True)
            .to_numpy(False)
            .astype(dtype=bool)
        )  # [#325]
    if operator == "PGRegexNotIMatch":
        # MODIFIED FOR OPTERYX - see comment above
        matches = compute.match_substring_regex(arr, value[0], ignore_case=True)  # [#325]
        return numpy.invert(matches)

    if operator == "AnyOpEq":
        return list_ops.cython_anyop_eq(arr[0], value)
    if operator == "AnyOpNotEq":
        return list_ops.cython_anyop_neq(arr[0], value)
    if operator == "AnyOpGt":
        return list_ops.cython_anyop_gt(arr[0], value)
    if operator == "AnyOpLt":
        return list_ops.cython_anyop_lt(arr[0], value)
    if operator == "AnyOpGtEq":
        return list_ops.cython_anyop_gte(arr[0], value)
    if operator == "AnyOpLtEq":
        return list_ops.cython_anyop_lte(arr[0], value)
    if operator == "AllOpEq":
        return list_ops.cython_allop_eq(arr[0], value)
    if operator == "AllOpNotEq":
        return list_ops.cython_allop_neq(arr[0], value)
    if operator == "Arrow":
        return list_ops.cython_arrow_op(arr, value[0])
    if operator == "LongArrow":
        return list_ops.cython_long_arrow_op(arr, value[0])
    raise NotImplementedError(f"Operator {operator} is not implemented!")  # pragma: no cover
