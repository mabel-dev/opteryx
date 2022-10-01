"""
slice left is fast but that speed is because the safety has been disabled. These tests
help to ensure that slice left still does what it should safely and correctly.
"""
import os
import sys
import numpy

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.functions import string_functions


def test_slice_left():

    slicer = string_functions.string_slicer_left

    # fmt:off
    assert slicer(numpy.array(["abcdef"]), 3) == ["abc"], slicer(numpy.array(["abcdef"]), 3)
    assert slicer(numpy.array(["abcdef", "ghijklm"]), 3) == ["abc","ghi"], slicer(numpy.array(["abcdef", "ghijklm"]), 3)
    assert slicer(numpy.array([]), 3) == [[]], slicer(numpy.array([]), 3)
    assert slicer(numpy.array([None]), 3) == [None], slicer(numpy.array([None]), 3)
    assert slicer(numpy.array([""]), 0) == [[""]], slicer(numpy.array([""]), 0)
    assert slicer(numpy.array(["abc", "abcdefghijklmnopqrstuvwxyz"]), 5) == ["abc","abcde"], slicer(numpy.array(["abc", "abcdefghijklmnopqrstuvwxyz"]), 5)
    assert slicer(numpy.array([None, "", "abcdef", "a"]), 2) == [None,"","ab","a"], slicer(numpy.array([None, "", "abcdef", "a"]), 2)[0]
    # fmt:on


def test_slice_right():

    slicer = string_functions.string_slicer_right

    # fmt:off
    assert slicer(numpy.array(["abcdef"]), 3) == ["def"], slicer(numpy.array(["abcdef"]), 3)
    assert slicer(numpy.array(["abcdef", "ghijklm"]), 3) == ["def","klm"], slicer(numpy.array(["abcdef", "ghijklm"]), 3)
    assert slicer(numpy.array([]), 3) == [[]], slicer(numpy.array([]), 3)
    assert slicer(numpy.array([None]), 3) == [None], slicer(numpy.array([None]), 3)
    assert slicer(numpy.array([""]), 0) == [[""]], slicer(numpy.array([""]), 0)
    assert slicer(numpy.array(["abc", "abcdefghijklmnopqrstuvwxyz"]), 5) == ["abc","vwxyz"], slicer(numpy.array(["abc", "abcdefghijklmnopqrstuvwxyz"]), 5)
    assert slicer(numpy.array([None, "", "abcdef", "a"]), 2) == [None, "","ef", "a"], slicer(numpy.array([None, "", "abcdef", "a"]), 2)[0]
    # fmt:on


if __name__ == "__main__":  # pragma: no cover

    test_slice_left()
    skip_test_slice_right()
    print("✅ okay")
