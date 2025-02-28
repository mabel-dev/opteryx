# type:ignore
# isort: skip_file
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.third_party.maki_nage import distogram
from pytest import approx
import random


def test_count_at():
    h = distogram.Distogram(bin_count=3)
    print(h)

    # fill histogram
    distogram.update(h, 16, count=4)
    distogram.update(h, 23, count=3)
    distogram.update(h, 28, count=5)
    print(h)

    actual_result = distogram.count_at(h, 25)
    assert actual_result == approx(6.859999999)


def test_count_at_normal():
    points = 10000
    normal = [random.normalvariate(0.0, 1.0) for _ in range(points)]
    h = distogram.Distogram()

    for i in normal:
        distogram.update(h, i)

    assert distogram.count_at(h, 0) == approx(points / 2, rel=0.05)


def test_count_at_not_enough_elements():
    h = distogram.Distogram()

    distogram.update(h, 1)
    distogram.update(h, 2)
    distogram.update(h, 3)

    assert distogram.count_at(h, 2.5) == 2


def test_count_at_left():
    h = distogram.Distogram(bin_count=6)

    for i in [1, 2, 3, 4, 5, 6, 0.7, 1.1]:
        distogram.update(h, i)

    assert distogram.count_at(h, 0.77) == approx(0.14), distogram.count_at(h, 0.77)


def test_count_at_right():
    h = distogram.Distogram(bin_count=6)

    for i in [1, 2, 3, 4, 5, 6, 6.7, 6.1]:
        distogram.update(h, i)

    assert distogram.count_at(h, 6.5) == approx(7.307692307692308)


def test_count_at_empty():
    h = distogram.Distogram()

    assert distogram.count_at(h, 6.5) is None


def test_count_at_out_of_bouns():
    h = distogram.Distogram()

    for i in [1, 2, 3, 4, 5, 6, 6.7, 6.1]:
        distogram.update(h, i)

    assert distogram.count_at(h, 0.2) is None
    assert distogram.count_at(h, 10) is None


if __name__ == "__main__":  # pragma: no cover
    test_count_at()
    test_count_at_empty()
    test_count_at_left()
    test_count_at_normal()
    test_count_at_not_enough_elements()
    test_count_at_out_of_bouns()
    test_count_at_right()
