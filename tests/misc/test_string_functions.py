import os
import sys
import numpy

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.engine.functions import string_functions


def test_slice_left():

    slicer = string_functions.string_slicer_left

    assert slicer(numpy.array(["abcdef"]), 3) == ["abc"]
    assert sorted(slicer(numpy.array(["abcdef", "ghijklm"]), 3)[0]) == [
        "abc",
        "ghi",
    ], sorted(slicer(numpy.array(["abcdef", "ghijklm"]), 3))
    assert slicer(numpy.array([]), 3) == [[]], slicer(numpy.array([]), 3)
    assert slicer(numpy.array([None]), 3) == ["Non"], slicer(numpy.array([None]), 3)
    assert slicer(numpy.array([""]), 0) == [[""]], slicer(numpy.array([""]), 0)
    assert sorted(slicer(numpy.array(["abc", "abcdefghijklmnopqrstuvwxyz"]), 5)[0]) == [
        "abc",
        "abcde",
    ], slicer(numpy.array(["abc", "abcdefghijklmnopqrstuvwxyz"]), 5)
    assert sorted(slicer(numpy.array([None, "", "abcdef", "a"]), 2)[0]) == [
        "",
        "No",
        "a",
        "ab",
    ], sorted(slicer(numpy.array([None, "", "abcdef", "a"]), 2)[0])


if __name__ == "__main__":  # pragma: no cover

    test_slice_left()
    print("okay")
