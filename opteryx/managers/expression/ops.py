"""
Code was originally taken from
https://github.com/TomScheffers/pyarrow_ops/blob/main/pyarrow_ops/ops.py
and has been extensively modified for Opteryx.
"""

import re

import numpy
import pyarrow
from orso.types import OrsoTypes
from pyarrow import compute

from opteryx.compiled import list_ops


def filter_operations(left_arr, left_type, operator, right_arr, right_type):
    """
    Wrapped for Opteryx added to correctly handle null semantics.

    This returns an array with tri-state boolean (tue/false/none);
    if being used for display use as is, if being used for filtering, none is false.
    """
    if len(left_arr) == 0 or len(right_arr) == 0:
        return numpy.array([], dtype=bool)

    # INTEGERS and DECIMALS don't play nicely so we cast the INTS to DECIMALS
    if left_type == OrsoTypes.DECIMAL and right_type == OrsoTypes.INTEGER:
        right_arr = compute.cast(right_arr, pyarrow.float64())
        right_type = OrsoTypes.DOUBLE
    elif right_type == OrsoTypes.DECIMAL and left_type == OrsoTypes.INTEGER:
        left_arr = compute.cast(left_arr, pyarrow.float64())
        left_type = OrsoTypes.DOUBLE

    compressed = False

    if operator not in (
        "InList",
        "NotInList",
        "AnyOpEq",
        "AnyOpNotEq",
        "AnyOpGt",
        "AnyOpGtEq",
        "AnyOpLt",
        "AnyOpLtEq",
        "AnyOpLike",
        "AnyOpNotLike",
        "AnyOpILike",
        "AnyOpNotILike",
        "AnyOpRLike",
        "AnyOpNotRLike",
        "AllOpEq",
        "AllOpNotEq",
        "AtArrow",
    ):  # and right_type != OrsoTypes.NULL:
        # compressing ARRAY columns is VERY SLOW
        morsel_size = len(left_arr)

        # compute null positions
        left_null_positions = compute.is_null(left_arr, nan_is_null=True)

        # if the right side is an array, combine the null positions
        if len(right_arr) > 1:
            right_null_positions = compute.is_null(right_arr, nan_is_null=True)
            null_positions = numpy.logical_or(left_null_positions, right_null_positions)
        # if the right side is a scalar and is null, we can just return all nulls
        elif len(right_arr) == 1 and right_arr[0] is None:
            return pyarrow.array([None] * morsel_size, type=pyarrow.bool_())
        # if the right side is a scalar and is not null, we can just use the left nulls
        else:
            null_positions = left_null_positions.to_numpy(False)

        # Early exit if all values are null
        if null_positions.all():
            return pyarrow.array([None] * morsel_size, type=pyarrow.bool_())

        if null_positions.any() and isinstance(left_arr, numpy.ndarray):
            # if we have nulls and both columns are numpy arrays, we can speed things
            # up by removing the nulls from the calculations, we add the rows back in
            # later
            valid_positions = ~null_positions
            left_arr = left_arr.compress(valid_positions)
            compressed = True
            if len(right_arr) > 1 and isinstance(right_arr, numpy.ndarray):
                right_arr = right_arr.compress(valid_positions)
            elif len(right_arr) > 1 and isinstance(
                right_arr, (pyarrow.Array, pyarrow.ChunkedArray)
            ):
                right_arr = compute.filter(right_arr, valid_positions)

        # similarly, if we're working with pyarrow arrays we can remove nulls
        if null_positions.any() and isinstance(left_arr, (pyarrow.Array, pyarrow.ChunkedArray)):
            valid_positions = ~null_positions
            left_arr = compute.filter(left_arr, valid_positions)
            compressed = True
            if len(right_arr) > 1 and isinstance(right_arr, numpy.ndarray):
                right_arr = right_arr.compress(valid_positions)
            elif len(right_arr) > 1 and isinstance(
                right_arr, (pyarrow.Array, pyarrow.ChunkedArray)
            ):
                right_arr = compute.filter(right_arr, valid_positions)

    if (
        OrsoTypes.TIMESTAMP in (left_type, right_type) or OrsoTypes.DATE in (left_type, right_type)
    ) and OrsoTypes.INTEGER in (left_type, right_type):
        from opteryx.functions.date_functions import convert_int64_array_to_pyarrow_datetime

        if left_type == OrsoTypes.INTEGER:
            left_arr = convert_int64_array_to_pyarrow_datetime(left_arr)
            left_type = OrsoTypes.TIMESTAMP
        if right_type == OrsoTypes.INTEGER:
            right_arr = convert_int64_array_to_pyarrow_datetime(right_arr)
            right_type = OrsoTypes.TIMESTAMP

    if OrsoTypes.INTERVAL in (left_type, right_type):
        from opteryx.custom_types.intervals import INTERVAL_KERNELS

        function = INTERVAL_KERNELS.get((left_type, right_type, operator))
        if function is None:
            from opteryx.exceptions import UnsupportedTypeError

            raise UnsupportedTypeError(
                f"Cannot perform {operator.upper()} on {left_type} and {right_type}."
            )

        results_mask = function(left_arr, left_type, right_arr, right_type, operator)
    else:
        # do the evaluation
        results_mask = _inner_filter_operations(left_arr, operator, right_arr)

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
    if not operator.startswith(("AnyOp", "AllOp")) and len(value) == 1:
        value = value[0]
        if hasattr(value, "item"):
            value = value.item()
        if isinstance(value, (tuple, list)):
            value = pyarrow.array(value)

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
        if hasattr(value, "to_pylist"):
            value = value.to_pylist()
        if hasattr(value, "to_numpy"):
            value = value.to_numpy(zero_copy_only=False)
        values = set(value)
        if hasattr(arr, "to_numpy"):
            arr = arr.to_numpy(zero_copy_only=False)
        if arr.dtype == numpy.int64:
            return list_ops.list_in_list.list_in_list_int64(memoryview(arr), values, len(arr))
        else:
            return list_ops.list_in_list.list_in_list(arr.astype(object), values)
    if operator == "NotInList":
        if hasattr(value, "to_pylist"):
            value = value.to_pylist()
        if hasattr(value, "to_numpy"):
            value = value.to_numpy(zero_copy_only=False)
        values = set(value)
        if hasattr(arr, "to_numpy"):
            arr = arr.to_numpy(zero_copy_only=False)
        if arr.dtype == numpy.int64:
            matches = list_ops.list_in_list.list_in_list_int64(memoryview(arr), values, len(arr))
        else:
            matches = list_ops.list_in_list.list_in_list(arr.astype(object), values)
        return numpy.invert(matches.astype(dtype=bool))
    if operator == "InStr":
        needle = str(value)
        return numpy.asarray(list_ops.list_substring.list_substring(arr, needle), dtype=bool)
    if operator == "NotInStr":
        needle = str(value)
        matches = numpy.asarray(list_ops.list_substring.list_substring(arr, needle), dtype=bool)
        return numpy.invert(matches)
    if operator == "IInStr":
        needle = str(value)
        return numpy.asarray(
            list_ops.list_substring.list_substring_case_insensitive(arr, needle), dtype=bool
        )
    if operator == "NotIInStr":
        needle = str(value)
        matches = numpy.asarray(
            list_ops.list_substring.list_substring_case_insensitive(arr, needle), dtype=bool
        )
        return numpy.invert(matches)
    if operator == "Like":
        return compute.match_like(arr, value).to_numpy(False).astype(dtype=bool)
    if operator == "NotLike":
        matches = compute.match_like(arr, value).to_numpy(False).astype(dtype=bool)
        return numpy.invert(matches)
    if operator == "ILike":
        return compute.match_like(arr, value, ignore_case=True).to_numpy(False).astype(dtype=bool)
    if operator == "NotILike":
        matches = compute.match_like(arr, value, ignore_case=True)
        return numpy.invert(matches)
    if operator == "RLike":
        return compute.match_substring_regex(arr, value).to_numpy(False).astype(dtype=bool)
    if operator == "NotRLike":
        matches = compute.match_substring_regex(arr, value)  # [#325]
        return numpy.invert(matches)
    if operator == "AnyOpEq":
        return list_ops.list_anyop_eq.list_anyop_eq(literal=arr[0], column=value)
    if operator == "AnyOpNotEq":
        return list_ops.list_anyop_neq.list_anyop_neq(literal=arr[0], column=value)
    if operator == "AnyOpGt":
        return list_ops.list_anyop_gt.list_anyop_gt(arr[0], value)
    if operator == "AnyOpLt":
        return list_ops.list_anyop_lt.list_anyop_lt(arr[0], value)
    if operator == "AnyOpGtEq":
        return list_ops.list_anyop_gte.list_anyop_gte(arr[0], value)
    if operator == "AnyOpLtEq":
        return list_ops.list_anyop_lte.list_anyop_lte(arr[0], value)
    if operator == "AllOpEq":
        return list_ops.list_allop_eq.list_allop_eq(arr[0], value)
    if operator == "AllOpNotEq":
        return list_ops.list_allop_neq.list_allop_neq(arr[0], value)

    if operator == "AnyOpILike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value, flags=re.IGNORECASE)

    if operator == "AnyOpLike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value)

    if operator == "AnyOpNotLike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value, invert=True)

    if operator == "AnyOpNotILike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value, flags=re.IGNORECASE, invert=True)

    if operator == "AtQuestion":
        from opteryx.third_party.tktech import csimdjson as simdjson

        if hasattr(arr, "to_numpy"):
            arr = arr.to_numpy(zero_copy_only=False)

        parser = simdjson.Parser()

        if not value.startswith("$."):
            # Not a JSONPath, treat as a simple key existence check
            return pyarrow.array(
                [value in parser.parse(doc).keys() for doc in arr],
                type=pyarrow.bool_(),  # type: ignore
            )

        # Convert "$.key1.list[0]" to JSON Pointer "/key1/list/0"
        def jsonpath_to_pointer(jsonpath: str) -> str:
            # Remove "$." prefix
            json_pointer = jsonpath[1:]
            # Replace "." with "/" for dict navigation
            json_pointer = json_pointer.replace(".", "/")
            # Replace "[index]" with "/index" for list access
            json_pointer = json_pointer.replace("[", "/").replace("]", "")
            return json_pointer

        # Convert "$.key1.key2" to JSON Pointer "/key1/key2"
        json_pointer = jsonpath_to_pointer(value)

        def check_json_pointer(doc, pointer):
            try:
                # Try accessing the path via JSON Pointer
                parser.parse(doc).at_pointer(pointer)
                return True  # If successful, the path exists
            except Exception:
                return False  # If an error occurs, the path does not exist

        # Apply the JSON Pointer check
        return pyarrow.array(
            [check_json_pointer(doc, json_pointer) for doc in arr],
            type=pyarrow.bool_(),
        )

    if operator == "AtArrow":
        from opteryx.compiled.list_ops.list_contains_any import list_contains_any

        if hasattr(value, "to_pylist"):
            value = value.to_pylist()
        if hasattr(arr, "to_numpy"):
            arr = arr.to_numpy(zero_copy_only=False)
        if len(arr) == 0:
            return numpy.array([], dtype=bool)
        if len(arr) == 1:
            return numpy.array([set(arr[0]).intersection(value)], dtype=bool)

        return list_contains_any(arr, set(value))

    raise NotImplementedError(f"Operator {operator} is not implemented!")  # pragma: no cover
