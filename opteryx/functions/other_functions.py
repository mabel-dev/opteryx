# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import numpy
import pyarrow
from pyarrow import compute

from opteryx.exceptions import SqlError


def list_contains(array, item):
    """
    does array contain item
    """
    if array is None:
        return False
    return item in set(array)


def list_contains_any(array: numpy.ndarray, items: numpy.ndarray) -> numpy.ndarray:
    """
    Check if any element in each sub-array of 'array' is present in 'items'.

    Parameters:
        array (numpy.ndarray): A 1D array where each element is an iterable (e.g., list, set).
        items (numpy.ndarray): An array of items to check against each sub-array in 'array'.

    Returns:
        numpy.ndarray: A boolean array where each element indicates whether any item
                       from 'items' is found in the corresponding sub-array of 'array'.
    """
    items_set = set(items[0])
    res = numpy.empty(array.size, dtype=bool)
    for i, test_set in enumerate(array):
        # Using not to correctly capture overlap (isdisjoint is True when no common elements)
        res[i] = not items_set.isdisjoint(test_set) if test_set is not None else False
    return res


def list_contains_all(array, items):
    """
    does array contain all of the items in items
    """
    if array is None:
        return False
    return set(array).issuperset(items)


def search(array, item):
    """
    `search` provides a way to look for values across different field types, rather
    than doing a LIKE on a string, IN on a list, `search` adapts to the field type.

    This performs a pre-filter of the data to remove nulls - this means that the
    checks should generally be faster.
    """

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

    if array_type == str:
        # return True if the value is in the string
        results_mask = compute.match_substring(array, pattern=item, ignore_case=True)
    elif array_type == numpy.ndarray:
        # converting to a set is faster for a handful of items which is what we're
        # almost definitely working with here - note compute.index is about 50x slower
        results_mask = numpy.array([item in set(record) for record in array], dtype=numpy.bool_)
    elif array_type == dict:
        results_mask = numpy.array([item in record.values() for record in array], dtype=numpy.bool_)
    else:
        raise SqlError("SEARCH can only be used with VARCHAR, LIST and STRUCT.")

    if compressed:
        # fill the result set
        results = numpy.full(record_count, False, numpy.bool_)
        results[numpy.nonzero(null_positions)] = results_mask
        return results

    return results_mask


def iif(mask, true_values, false_values):
    # we have three columns, the first is TRUE offsets
    # the second is TRUE response
    # the third is FAST response

    if isinstance(mask, pyarrow.lib.BooleanArray) or (
        isinstance(mask, numpy.ndarray) and mask.dtype == numpy.bool_
    ):
        mask = numpy.nonzero(mask)[0]

    response = false_values

    for index in mask:
        response[index] = true_values[index]
    return response


def if_null(values, replacement):
    response = values
    for index, value in enumerate(values):
        if value is None or value != value:  # nosec
            response[index] = replacement[index]
    return response


def null_if(col1, col2):
    return [None if a == b else a for a, b in zip(col1, col2)]


def case_when(conditions, values):
    res = []
    cons = list(zip(*conditions))
    vals = zip(*values)
    for idx, val_set in enumerate(vals):
        offset = next((i for i, j in enumerate(cons[idx]) if j), None)
        if offset is not None:
            res.append(val_set[offset])
        else:
            res.append(None)
    return res


def cosine_similarity(arr, val):
    """
    ad hoc cosine similarity function, slow.
    """

    from opteryx.compiled.functions import tokenize_and_remove_punctuation
    from opteryx.compiled.functions import vectorize
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
    vectors = [vectorize(tokens) for tokens in tokenized_strings]
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
