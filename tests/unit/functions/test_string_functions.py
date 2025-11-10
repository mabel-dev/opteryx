"""
slice left is fast but that speed is because the safety has been disabled. These tests
help to ensure that slice left still does what it should safely and correctly.
"""

import os
import sys

import numpy
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken import Vector
from opteryx.compiled import list_ops as compiled_list_ops
from opteryx.functions import string_functions

list_initcap = getattr(compiled_list_ops, "list_initcap")
list_regex_replace = getattr(compiled_list_ops, "list_regex_replace")
list_replace = getattr(compiled_list_ops, "list_replace")
list_string_slice_right = getattr(compiled_list_ops, "list_string_slice_right")
list_string_slice_left = getattr(compiled_list_ops, "list_string_slice_left")


def test_slice_left():
    slicer = list_string_slice_left

    # fmt:off
    assert slicer(numpy.array(["abcdef"]), 3).tolist() == ["abc"], slicer(numpy.array(["abcdef"]), 3)
    assert slicer(numpy.array(["abcdef", "ghijklm"]), 3).tolist() == ["abc","ghi"], slicer(numpy.array(["abcdef", "ghijklm"]), 3)
    assert slicer(numpy.array([]), 3).tolist() == [], slicer(numpy.array([]), 3)
    assert slicer(numpy.array([None]), 3).tolist() == [None], slicer(numpy.array([None]), 3)
    assert slicer(numpy.array([""]), 0).tolist() == [""], slicer(numpy.array([""]), 0)
    assert slicer(numpy.array(["abc", "abcdefghijklmnopqrstuvwxyz"]), 5).tolist() == ["abc","abcde"], slicer(numpy.array(["abc", "abcdefghijklmnopqrstuvwxyz"]), 5)
    assert slicer(numpy.array([None, "", "abcdef", "a"]), 2).tolist() == [None,"","ab","a"], slicer(numpy.array([None, "", "abcdef", "a"]), 2)[0]
    # fmt:on


def test_slice_right():
    slicer = list_string_slice_right

    # fmt:off
    assert slicer(numpy.array(["abcdef"]), 3).tolist() == ["def"], slicer(numpy.array(["abcdef"]), 3)
    assert slicer(numpy.array(["abcdef", "ghijklm"]), 3).tolist() == ["def","klm"], slicer(numpy.array(["abcdef", "ghijklm"]), 3)
    assert slicer(numpy.array([]), 3).tolist() == [], slicer(numpy.array([]), 3)
    assert slicer(numpy.array([None]), 3).tolist() == [None], slicer(numpy.array([None]), 3)
    assert slicer(numpy.array([""]), 0).tolist() == [""], slicer(numpy.array([""]), 0)
    assert slicer(numpy.array(["abc", "abcdefghijklmnopqrstuvwxyz"]), 5).tolist() == ["abc","vwxyz"], slicer(numpy.array(["abc", "abcdefghijklmnopqrstuvwxyz"]), 5)
    assert slicer(numpy.array([None, "", "abcdef", "a"]), 2).tolist() == [None, "","ef", "a"], slicer(numpy.array([None, "", "abcdef", "a"]), 2)[0]
    # fmt:on


def test_random_string():
    from orso.tools import random_string

    seen = set()
    for _ in range(100):
        rs = random_string()
        # we shouldn't see the same string twice
        assert rs not in seen
        seen.add(rs)
        # we shouldn't see padding in the string
        assert rs.count("=") == 0


def test_compiled_replace():
    data = numpy.array(["hello world", "banana", None], dtype=object)
    search = numpy.array(["l"], dtype=object)
    replace = numpy.array(["L"], dtype=object)

    result = list_replace(data, search, replace).tolist()

    assert result == ["heLLo worLd", "banana", None]


def test_compiled_replace_bytes():
    data = numpy.array([b"abcabc", b"", None], dtype=object)
    search = numpy.array([b"abc"], dtype=object)
    replace = numpy.array([b"x"], dtype=object)

    result = list_replace(data, search, replace).tolist()

    assert result == [b"xx", b"", None]


def test_compiled_initcap():
    data = numpy.array(["hello world", "AmiGoS", "o'connor", "3rd street", None], dtype=object)

    result = list_initcap(data).tolist()

    assert result == ["Hello World", "Amigos", "O'Connor", "3rd Street", None]


def test_compiled_initcap_bytes():
    data = numpy.array([b"mixed CASE"], dtype=object)

    result = list_initcap(data).tolist()

    assert result == ["Mixed Case"]


def test_re2_list_regex_replace_strings():
    """Test regex replace with string data (stored as bytes in Draken)"""
    data = Vector.from_arrow(pyarrow.array(["abc123", "xyz789", None]))
    pattern = rb"\d+"
    replacement = b""

    result = list_regex_replace(data, pattern, replacement).to_pylist()

    assert result == [b"abc", b"xyz", None]


def test_re2_list_regex_replace_bytes():
    data = Vector.from_arrow(pyarrow.array([b"http://a.example", b"https://b.example"], type=pyarrow.binary()))
    pattern = b"^https?"
    replacement = b""

    result = list_regex_replace(data, pattern, replacement).to_pylist()

    assert result == [b"://a.example", b"://b.example"]


def test_regex_replace_python_wrapper_returns_arrow():
    """Test that the Python wrapper returns PyArrow arrays with bytes"""
    data = pyarrow.array(["Earth", "Europa"])
    pattern = numpy.array(["^E"], dtype=object)
    replacement = numpy.array(["G"], dtype=object)

    result = string_functions.regex_replace(data, pattern, replacement)

    assert isinstance(result, pyarrow.Array)
    # Result is binary (bytes) because Draken works with bytes
    assert result.to_pylist() == [b"Garth", b"Guropa"]


def test_regex_replace_invalid_pattern_raises():
    from opteryx.exceptions import InvalidFunctionParameterError

    data = pyarrow.array(["test"])
    pattern = numpy.array(["("], dtype=object)
    replacement = numpy.array([""], dtype=object)

    try:
        string_functions.regex_replace(data, pattern, replacement)
    except InvalidFunctionParameterError:
        pass
    else:
        assert False, "Expected InvalidFunctionParameterError to be raised"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
