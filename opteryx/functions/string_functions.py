# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import List
from typing import Union

import numpy
import pyarrow
from pyarrow import compute

from opteryx.exceptions import InvalidFunctionParameterError


def split(arr, delimiter=",", limit=None):
    """
    Slice a list of strings from the right
    """
    if not isinstance(delimiter, str):
        delimiter = delimiter[0]
    if limit is not None:
        limit = int(limit[0]) - 1
        if limit < 0:
            raise InvalidFunctionParameterError(
                "`SPLIT` limit parameter must be greater than zero."
            )
    return compute.split_pattern(arr, pattern=delimiter, max_splits=limit).to_numpy(
        zero_copy_only=False
    )


def get_sha224(item):
    """calculate SHA256 hash of a value"""
    import hashlib  # delay the import - it's rarely needed

    if item is None:
        return None

    return hashlib.sha224(str(item).encode()).hexdigest()


def get_sha384(item):
    """calculate SHA256 hash of a value"""
    import hashlib  # delay the import - it's rarely needed

    if item is None:
        return None

    return hashlib.sha384(str(item).encode()).hexdigest()


def base64_encode(arr):
    """calculate BASE64 encoding of a string"""
    from opteryx.third_party.alantsd.base64 import encode

    if isinstance(arr, numpy.ndarray):
        arr = arr.astype(object)
        arr = [item.encode("utf-8") if isinstance(item, str) else item for item in arr]

    return [encode(item) for item in arr]


def base64_decode(arr):
    """calculate BASE64 encoding of a string"""
    from opteryx.third_party.alantsd.base64 import decode

    if isinstance(arr, numpy.ndarray):
        arr = arr.astype(object)
        arr = [item.encode("utf-8") if isinstance(item, str) else item for item in arr]

    return [decode(item) for item in arr]


def get_base85_encode(item):
    """calculate BASE85 encoding of a string"""
    import base64

    if item is None:
        return None

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b85encode(item).decode("UTF8")


def get_base85_decode(item):
    """calculate BASE85 encoding of a string"""
    import base64

    if item is None:
        return None

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b85decode(item).decode("UTF8")


def get_hex_encode(item):
    """calculate HEX encoding of a string"""
    import base64

    if item is None:
        return None

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b16encode(item).decode("UTF8")


def get_hex_decode(item):
    """calculate HEX encoding of a string"""
    import base64

    if item is None:
        return None

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b16decode(item).decode("UTF8")


def concat(list_values):
    """concatenate a list of strings"""
    result: List = []
    for row in list_values:
        if row is None:
            result.append(None)
        else:
            row = row.astype(dtype=numpy.str_)
            result.append("".join(row))
    return result


def concat_ws(separator, list_values):
    """concatenate a list of strings with a separator"""
    result: List = []
    if len(separator) > 0:
        separator = separator[0]
        if separator is None:
            return None
    for row in list_values:
        if row is None:
            result.append(None)
        else:
            row = row.astype(dtype=numpy.str_)
            result.append(separator.join(row))
    return result


def starts_w(arr, test, ignore_case=[False]):
    return compute.starts_with(arr, test[0], ignore_case=ignore_case[0])


def ends_w(arr, test, ignore_case=[False]):
    return compute.ends_with(arr, test[0], ignore_case=ignore_case[0])


def substring(
    arr: List[str], from_pos: List[int], count: List[Union[int, float]]
) -> List[List[str]]:
    """
    Extracts substrings from each string in the 'arr' list.

    Parameters:
        arr: List[str]
            List of strings from which substrings will be extracted.
        from_pos: List[int]
            List of starting positions for each substring.
        count: List[Union[int, float]]
            List of lengths for each substring. Can be a float (NaN) to signify until the end.

    Returns:
        List[str]
            List of extracted substrings.
    """
    if len(arr) == 0:
        return [[]]

    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(zero_copy_only=False)

    def _inner(val, _from, _for):
        if _from is None:
            _from = 0
        if _from > 0:
            _from -= 1
        _for = int(_for) if _for and _for == _for else None  # nosec
        if _for is None:
            return val[_from:]
        return val[_from : _for + _from]

    return [_inner(val, _from, _for) for val, _from, _for in zip(arr, from_pos, count)]


def position(sub, string):
    """
    Returns the starting position of the first instance of substring in string. Positions start with 1. If not found, 0 is returned.
    """
    return string.find(sub) + 1


def trim(*args):
    if len(args) == 1:
        return compute.utf8_trim_whitespace(args[0])
    return compute.utf8_trim(args[0], args[1][0])


def ltrim(*args):
    if len(args) == 1:
        return compute.utf8_ltrim_whitespace(args[0])
    return compute.utf8_ltrim(args[0], args[1][0])


def rtrim(*args):
    if len(args) == 1:
        return compute.utf8_rtrim_whitespace(args[0])
    return compute.utf8_rtrim(args[0], args[1][0])


def levenshtein(a, b):
    from opteryx.compiled.list_ops import list_levenshtein

    # Convert to numpy arrays with object dtype if needed
    if hasattr(a, "to_numpy"):
        a = a.to_numpy(zero_copy_only=False)
    if hasattr(b, "to_numpy"):
        b = b.to_numpy(zero_copy_only=False)

    # Ensure arrays are numpy arrays with object dtype
    if not isinstance(a, numpy.ndarray):
        a = numpy.array(a, dtype=object)
    elif a.dtype.kind in ["U", "S"]:  # Unicode or byte string dtypes
        a = a.astype(object)

    if not isinstance(b, numpy.ndarray):
        b = numpy.array(b, dtype=object)
    elif b.dtype.kind in ["U", "S"]:  # Unicode or byte string dtypes
        b = b.astype(object)

    return list_levenshtein(a, b)


def to_char(arr) -> List[str]:
    return [chr(a) for a in arr]


def to_ascii(arr) -> List[int]:
    return [ord(a) for a in arr]


def left_pad(arr, width, fill):
    width = width[0]
    fill = fill[0]
    return [str(a).rjust(width, fill) for a in arr]


def right_pad(arr, width, fill):
    width = width[0]
    fill = fill[0]
    return [str(a).ljust(width, fill) for a in arr]


def match_against(arr, val):
    """
    Matches each string in `arr` against the tokenized and normalized version of `val[0]`.
    This builds the index during execution which is very slow.

    Args:
        arr (list[str]): List of strings to match against.
        val (list[str]): List containing a single string to match.

    Returns:
        list[bool]: List of booleans indicating if each string in `arr` matches `val[0]`.
    """

    from opteryx.compiled.functions.vectors import tokenize_and_remove_punctuation
    from opteryx.virtual_datasets.stop_words import STOP_WORDS

    if len(val) == 0:
        return []
    tokenized_literal = tokenize_and_remove_punctuation(str(val[0]), STOP_WORDS)

    if len(tokenized_literal) == 0:
        return [False] * len(arr)

    tokenized_strings = (tokenize_and_remove_punctuation(s, STOP_WORDS) for s in arr)

    return [tokenized_literal.issubset(tok) for tok in tokenized_strings]


def regex_replace(array, _pattern, _replacement):
    """
    Regex replacement using the vendored RE2 engine exposed via list_ops.

    This implementation avoids PyArrow's regex facilities so that pattern
    compilation and matching are backed by Google RE2 while keeping the
    vectorised execution model used elsewhere in list_ops.
    """
    from opteryx.compiled import list_ops as compiled_list_ops
    from opteryx.draken import Vector

    list_regex_replace = getattr(compiled_list_ops, "list_regex_replace")

    def _as_arrow(value, label):
        if isinstance(value, pyarrow.Array):
            return value
        if hasattr(value, "to_arrow"):
            return value.to_arrow()
        if isinstance(value, numpy.ndarray):
            if value.ndim != 1:
                raise InvalidFunctionParameterError(f"{label} must be one-dimensional.")
            return pyarrow.array(value)
        if isinstance(value, (list, tuple)):
            return pyarrow.array(value)
        return None

    def as_bytes(value):
        # Handle numpy scalars
        if hasattr(value, "item"):
            value = value.item()
        # Handle pyarrow scalars
        elif hasattr(value, "as_py"):
            value = value.as_py()
        # Convert to bytes
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        # Fallback: convert to string then bytes
        return str(value).encode("utf-8")

    array_arrow = _as_arrow(array, "Input")
    data_vector = Vector.from_arrow(array_arrow)

    pattern = as_bytes(_pattern[0])
    replacement = as_bytes(_replacement[0])

    try:
        return list_regex_replace(data_vector, pattern, replacement)
    except ValueError as exc:
        raise InvalidFunctionParameterError(str(exc)) from exc
