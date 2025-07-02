# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import List
from typing import Optional

import numpy
import pyarrow
from pyarrow import compute

from opteryx.exceptions import IncompatibleTypesError
from opteryx.exceptions import SqlError
from opteryx.third_party.tktech import csimdjson as simdjson


def list_contains(array, item):
    """
    does array contain item
    """
    if array is None:
        return False
    return item in set(array)


def list_contains_all(array, items):
    """
    does array contain all of the items in items
    """
    if array is None:
        return False
    required_items = set(items[0])  # Convert items[0] to a set once for efficient lookups
    return [None if a is None else set(a).issuperset(required_items) for a in array]


def search(array, item, ignore_case: Optional[List[bool]] = None):
    """
    `search` provides a way to look for values across different field types, rather
    than doing a LIKE on a string, IN on a list, `search` adapts to the field type.

    This performs a pre-filter of the data to remove nulls - this means that the
    checks should generally be faster.
    """

    if ignore_case is None:
        ignore_case = [True]

    item = item[0]  # [#325]
    record_count = array.size

    if record_count > 0:
        null_positions = compute.is_null(array, nan_is_null=True)
        # if all the values are null, short-cut
        if null_positions.false_count == 0:
            return numpy.full(record_count, False, numpy.bool_)
        # do we have any nulls?
        compressed = null_positions.true_count > 0
        null_positions = numpy.invert(null_positions)
        # remove nulls from the checks
        if compressed:
            array = array.compress(null_positions)
        array_type = type(array[0])
    else:
        return numpy.array([False], dtype=numpy.bool_)

    if array_type in (str, bytes):
        # Return True if the value is in the string
        # We're essentially doing a LIKE here
        from opteryx.compiled import list_ops

        array = pyarrow.array(array)

        if ignore_case[0]:
            results_mask = numpy.asarray(
                list_ops.list_substring.list_substring_case_insensitive(array, str(item)),
                dtype=numpy.bool_,
            )
        else:
            results_mask = numpy.asarray(
                list_ops.list_substring.list_substring(array, str(item)), dtype=numpy.bool_
            )
    elif array_type == numpy.ndarray:
        # converting to a set is faster for a handful of items which is what we're
        # almost definitely working with here - note compute.index is about 50x slower
        results_mask = numpy.array([item in set(record) for record in array], dtype=numpy.bool_)
    elif array_type == dict:
        results_mask = numpy.array([item in record.values() for record in array], dtype=numpy.bool_)
    else:
        raise SqlError("SEARCH can only be used with VARCHAR, BLOB, LIST and STRUCT.")

    if compressed:
        # fill the result set
        results = numpy.full(record_count, False, numpy.bool_)
        results[numpy.nonzero(null_positions)] = results_mask
        return results

    return results_mask


def if_null(values, replacements):
    """
    Replace null values in the input array with corresponding values from the replacement array.

    Parameters:
        values: Union[numpy.ndarray, pyarrow.Array]
            The input array which may contain null values.
        replacement: numpy.ndarray
            The array with replacement values corresponding to the null positions in the input array.

    Returns:
        numpy.ndarray
            The input array with null values replaced by corresponding values from the replacement array.
    """
    from opteryx.managers.expression.unary_operations import _is_null

    # Create a mask for null values
    is_null_mask = _is_null(values)

    if hasattr(replacements, "to_numpy"):
        replacements = replacements.to_numpy(zero_copy_only=False)
    if hasattr(values, "to_numpy"):
        values = values.to_numpy(zero_copy_only=False)

    if len(replacements) == 1:
        if isinstance(replacements, numpy.ndarray):
            replacement = replacements[0]
            if hasattr(is_null_mask, "tolist"):
                is_null_mask = is_null_mask.tolist()
            return numpy.array(
                [replacement if is_null else values[i] for i, is_null in enumerate(is_null_mask)],
                dtype=values.dtype,
            )

        replacements = numpy.full(values.shape, replacements[0], dtype=values.dtype)

    target_type = numpy.promote_types(values.dtype, replacements.dtype)
    return numpy.where(is_null_mask, replacements, values).astype(target_type)


def if_not_null(values: numpy.ndarray, replacements: numpy.ndarray) -> numpy.ndarray:
    """
    Optimizer helper function: replace a value only if it is not null.

    This is *not* SQL's IFNULL/COALESCE. This is used during constant folding
    to preserve null-awareness while simplifying expressions.

    For each element:
        if value is NOT null → use replacement
        if value IS null → keep the original null

    Parameters:
        values: Original values (may include nulls).
        replacements: Values to use if the original is not null.

    Returns:
        Array with replacements where applicable, nulls otherwise.
    """
    from opteryx.managers.expression.unary_operations import _is_not_null

    if hasattr(replacements, "to_numpy"):
        replacements = replacements.to_numpy(zero_copy_only=False)
    if hasattr(values, "to_numpy"):
        values = values.to_numpy(zero_copy_only=False)

    is_not_null_mask = _is_not_null(values)
    target_type = numpy.promote_types(values.dtype, replacements.dtype)
    return numpy.where(is_not_null_mask, replacements, values).astype(target_type)


def null_if(col1, col2):
    """
    Parameters:
        col1: Union[numpy.ndarray, list]
            The first input array.
        col2: Union[numpy.ndarray, list]
            The second input array.

    Returns:
        numpy.ndarray
            An array where elements from col1 are replaced with None if they match the corresponding elements in col2.
    """
    if isinstance(col1, pyarrow.Array):
        col1 = col1.to_numpy(False)
    if isinstance(col1, list):
        col1 = col1.array(col1)
    if isinstance(col2, pyarrow.Array):
        col2 = col2.to_numpy(False)
    if isinstance(col2, list):
        col2 = col2.array(col2)

    from orso.types import PYTHON_TO_ORSO_MAP
    from orso.types import OrsoTypes
    from orso.types import find_compatible_type

    def get_first_non_null_type(array):
        for item in array:
            if item is not None:
                return PYTHON_TO_ORSO_MAP.get(type(item), OrsoTypes._MISSING_TYPE)
        return OrsoTypes.NULL

    col1_type = get_first_non_null_type(col1.tolist())
    col2_type = get_first_non_null_type(col2.tolist())

    if find_compatible_type([col1_type, col2_type], None) is None:
        raise IncompatibleTypesError(
            left_type=col1_type,
            right_type=col2_type,
            message=f"`NULLIF` called with input arrays of different types, {col1_type} and {col2_type}.",
        )

    # Create a mask where elements in col1 are equal to col2
    mask = col1 == col2

    # Return None where the mask is True, else col1
    return numpy.where(mask, None, col1)


def cosine_similarity(arr, val):
    """
    ad hoc cosine similarity function, slow.
    """

    from opteryx.compiled.functions.vectors import tokenize_and_remove_punctuation
    from opteryx.compiled.functions.vectors import vectorize
    from opteryx.virtual_datasets.stop_words import STOP_WORDS

    def cosine_similarity(
        vec1: numpy.ndarray, vec2: numpy.ndarray, vec2_norm: numpy.float32
    ) -> float:
        vec1 = vec1.astype(numpy.float32)
        vec1_norm = numpy.linalg.norm(vec1)

        product = vec1_norm * vec2_norm
        if product == 0:
            return 0

        return numpy.dot(vec1, vec2) / product

    # import time

    if len(val) == 0:
        return []
    tokenized_literal = tokenize_and_remove_punctuation(str(val[0]), STOP_WORDS)
    if len(tokenized_literal) == 0:
        return [0.0] * len(arr)
    # print(len(val))

    # t = time.monotonic_ns()
    tokenized_strings = [tokenize_and_remove_punctuation(s, STOP_WORDS) for s in arr] + [
        tokenized_literal
    ]
    # print("time tokenizing ", time.monotonic_ns() - t)
    # t = time.monotonic_ns()
    vectors = [vectorize(list(tokens)) for tokens in tokenized_strings]
    # print("time vectorizing", time.monotonic_ns() - t)
    comparison_vector = vectors[-1].astype(numpy.float32)
    comparison_vector_norm = numpy.linalg.norm(comparison_vector)

    if comparison_vector_norm == 0.0:
        return [0.0] * len(val)

    # t = time.monotonic_ns()
    similarities = [
        cosine_similarity(vector, comparison_vector, comparison_vector_norm)
        for vector in vectors[:-1]
    ]
    # print("time comparing  ", time.monotonic_ns() - t)

    return similarities


def jsonb_object_keys(arr: numpy.ndarray):
    """
    Extract the keys from a NumPy array of JSON objects or JSON strings/bytes.

    Parameters:
        arr: numpy.ndarray
            A NumPy array of dictionaries or JSON-encoded strings/bytes.

    Returns:
        pyarrow.Array
            A PyArrow Array containing lists of keys for each input element.
    """
    # Early exit for empty input
    if len(arr) == 0:
        return numpy.array([])

    # we may get pyarrow arrays here - usually not though
    if isinstance(arr, pyarrow.Array):
        arr = arr.to_numpy(zero_copy_only=False)

    # Pre-create the result array as a NumPy boolean array set to False
    result = numpy.empty(arr.shape, dtype=list)

    if isinstance(arr[0], dict):
        # Process dictionaries
        for i, row in enumerate(arr):
            result[i] = [str(key) for key in row.keys()]  # noqa: SIM118 - row is not a dict; .keys() is required
    elif isinstance(arr[0], (str, bytes)):
        # SIMD-JSON parser instance for JSON string/bytes
        parser = simdjson.Parser()
        for i, row in enumerate(arr):
            result[i] = [str(key) for key in parser.parse(row).keys()]  # noqa: SIM118 - row is not a dict; .keys() is required
    else:
        raise ValueError("Unsupported dtype for array elements. Expected dict, str, or bytes.")

    # Return the result as a PyArrow array
    return result


def humanize(arr):
    def format_number(num: float) -> str:
        """Formats the number with or without decimal places based on whether it's an integer."""
        return f"{num:,.0f}" if isinstance(num, int) else f"{num:,.1f}"

    def humanize_number(value: float) -> str:
        thresholds = [
            (1_000_000_000_000, "trillion"),
            (1_000_000_000, "billion"),
            (1_000_000, "million"),
            (1_000, "thousand"),
        ]

        for threshold, label in thresholds:
            rounded = round(value / threshold, 1)
            if rounded >= 0.9:  # Ensure we don't get "0.9 million" turning into "0 million"
                return f"{format_number(rounded)} {label}"
        return format_number(value)

    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(zero_copy_only=False)
    if hasattr(arr, "tolist"):
        arr = arr.tolist()

    return [humanize_number(value) for value in arr]


def array_cast(array, element_type):
    from orso.types import OrsoTypes

    result = numpy.empty(len(array), dtype=list)
    parser = OrsoTypes[element_type[0]].parse
    if hasattr(array, "to_numpy"):
        array = array.to_numpy(zero_copy_only=False)
    for i, row in enumerate(array):
        row_res = []
        if row is not None:
            for element in row:
                if element is None:
                    continue
                row_res.append(parser(element))
            result[i] = row_res
    return result


def array_cast_safe(array, element_type):
    from contextlib import suppress

    from orso.types import OrsoTypes

    result = numpy.empty(len(array), dtype=list)
    parser = OrsoTypes[element_type[0]].parse
    for i, row in enumerate(array):
        row_res = []
        with suppress(Exception):
            if row is not None:
                for element in row:
                    if element is None:
                        continue
                    value = parser(element)
                    row_res.append(value)
        result[i] = row_res
    return result
