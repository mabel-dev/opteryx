# isort: skip_file
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.third_party.maki_nage import distogram
import random


def test_bounds():
    normal = [random.normalvariate(0.0, 1.0) for _ in range(10000)]
    h = distogram.Distogram()

    for i in normal:
        distogram.update(h, i)

    dmin, dmax = distogram.bounds(h)
    assert dmin == min(normal)
    assert dmax == max(normal)


if __name__ == "__main__":  # pragma: no cover
    test_bounds()
