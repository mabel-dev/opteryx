"""
Original code modified for Opteryx.
"""

import re

import numpy
import pyarrow
from orso.types import OrsoTypes
from pyarrow import compute

from opteryx.compiled import list_ops


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
    ):
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

    if (
        OrsoTypes.TIMESTAMP in (left_type, right_type) or OrsoTypes.DATE in (left_type, right_type)
    ) and OrsoTypes.INTEGER in (left_type, right_type):
        from opteryx.functions.date_functions import convert_int64_array_to_pyarrow_datetime

        if left_type == OrsoTypes.INTEGER:
            arr = convert_int64_array_to_pyarrow_datetime(arr)
        if right_type == OrsoTypes.INTEGER:
            value = convert_int64_array_to_pyarrow_datetime(value)

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
    if operator == "InStr":
        needle = str(value[0])
        return list_ops.list_ops.list_substring(arr, needle).astype(dtype=bool)
    if operator == "NotInStr":
        needle = str(value[0])
        matches = list_ops.list_ops.list_substring(arr, needle)  # [#325]
        return numpy.invert(matches.astype(dtype=bool))
    if operator == "IInStr":
        needle = str(value[0])
        return list_ops.list_ops.list_substring_case_insensitive(arr, needle).astype(dtype=bool)
    if operator == "NotIInStr":
        needle = str(value[0])
        matches = list_ops.list_ops.list_substring_case_insensitive(arr, needle)  # [#325]
        return numpy.invert(matches.astype(dtype=bool))
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

    if operator == "AnyOpILike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value[0], flags=re.IGNORECASE)

    if operator == "AnyOpLike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value[0])

    if operator == "AnyOpNotLike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value[0], invert=True)

    if operator == "AnyOpNotILike":
        from opteryx.utils.sql import regex_match_any

        return regex_match_any(arr, value[0], flags=re.IGNORECASE, invert=True)

    if operator == "AtQuestion":
        import simdjson

        element = value[0]

        parser = simdjson.Parser()

        if not element.startswith("$."):
            # Not a JSONPath, treat as a simple key existence check
            return pyarrow.array(
                [element in parser.parse(doc).keys() for doc in arr],
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
        json_pointer = jsonpath_to_pointer(element)

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
        from opteryx.compiled.list_ops import list_contains_any

        return list_contains_any(arr, value)

    raise NotImplementedError(f"Operator {operator} is not implemented!")  # pragma: no cover
