"""
Original code modified for Opteryx.
"""
from ipaddress import IPv4Address
from ipaddress import IPv4Network

import numpy
import pyarrow
from pyarrow import compute

from opteryx.third_party.pyarrow_ops.helpers import columns_to_array

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
    "BitwiseOr",  # |
}


def filter_operations(arr, operator, value):
    """
    Wrapped for Opteryx added to correctly handle null semantics.

    This returns an array with tri-state boolean (tue/false/none);
    if being used for display use as is, if being used for filtering, none is false.
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
        # build tri-state response, PyArrow supports tristate, numpy does not
        return pyarrow.array((bool(r) if r != -1 else None for r in results), type=pyarrow.bool_())

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
        # some of the lists are saved as sets, which are faster than searching numpy
        # arrays, even with numpy's native functionality - choosing the right algo
        # is almost always faster than choosing a fast language.
        return numpy.array([a in value[0] for a in arr], dtype=numpy.bool_)  # [#325]?
    if operator == "NotInList":
        # MODIFIED FOR OPTERYX - see comment above
        return numpy.array([a not in value[0] for a in arr], dtype=numpy.bool_)  # [#325]?
    if operator == "Contains":
        # ADDED FOR OPTERYX
        return numpy.array([None if v is None else (arr[0] in v) for v in value], dtype=numpy.bool_)
    if operator == "NotContains":
        # ADDED FOR OPTERYX
        return numpy.array(
            [None if v is None else (arr[0] not in v) for v in value], dtype=numpy.bool_
        )  # [#325]?
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
    if operator in ("PGRegexMatch", "SimilarTo"):
        # MODIFIED FOR OPTERYX - see comment above
        return (
            compute.match_substring_regex(arr, value[0]).to_numpy(False).astype(dtype=bool)
        )  # [#325]
    if operator in ("PGRegexNotMatch", "NotSimilarTo"):
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
    if operator == "BitwiseOr":
        try:
            # parse right to be a list of IPs
            value = IPv4Network(value[0], strict=False)
            # is left in right
            result = []
            for address in arr:
                if address:
                    result.append(IPv4Address(address) in value)
                else:
                    result.append(None)
            return result
        except Exception as e:
            raise NotImplementedError("`|` can only be used to test IP address containment.")
    raise NotImplementedError(f"Operator {operator} is not implemented!")  # pragma: no cover


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
