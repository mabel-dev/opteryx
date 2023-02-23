# type:ignore
# isort: skip_file
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import random
import numpy as np
from pytest import approx
import distogram


def test_histogram():
    normal = [random.normalvariate(0.0, 1.0) for _ in range(10000)]
    h = distogram.Distogram(bin_count=64)

    for i in normal:
        distogram.update(h, i)

    np_values, np_edges = np.histogram(normal, 10)
    d_values, d_edges = distogram.histogram(h, 10)

    h = distogram.Distogram(bin_count=3)
    distogram.update(h, 23)
    distogram.update(h, 28)
    distogram.update(h, 16)
    assert distogram.histogram(h, bin_count=3) == (
        approx([1.0714285714285714, 0.6285714285714286, 1.3]),
        [16.0, 20.0, 24.0, 28],
    )
    assert sum(distogram.histogram(h, bin_count=3)[0]) == approx(3.0)


def test_histogram_on_too_small_distribution():
    h = distogram.Distogram(bin_count=64)

    for i in range(5):
        distogram.update(h, i)

    assert distogram.histogram(h, 10) == None


def test_format_histogram():
    bin_count = 4
    h = distogram.Distogram(bin_count=bin_count)

    for i in range(4):
        distogram.update(h, i)

    hist = distogram.histogram(h, bin_count=bin_count)
    assert len(hist[1]) == len(hist[0]) + 1
