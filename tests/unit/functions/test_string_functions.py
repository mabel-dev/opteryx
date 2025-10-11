"""
slice left is fast but that speed is because the safety has been disabled. These tests
help to ensure that slice left still does what it should safely and correctly.
"""

import os
import sys

import numpy

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.list_ops import list_initcap
from opteryx.compiled.list_ops import list_replace
from opteryx.compiled.list_ops import list_string_slice_right
from opteryx.compiled.list_ops import list_string_slice_left


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
    for i in range(100):
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


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
