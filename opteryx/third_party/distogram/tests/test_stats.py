# type:ignore
# isort: skip_file
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from pytest import approx
import distogram

import numpy as np
import random


def test_stats():
    normal = [random.normalvariate(0.0, 1.0) for _ in range(10000)]
    h = distogram.Distogram()

    for i in normal:
        distogram.update(h, i)

    assert distogram.mean(h) == approx(np.mean(normal), abs=0.1)
    assert distogram.variance(h) == approx(np.var(normal), abs=0.1)
    assert distogram.stddev(h) == approx(np.std(normal), abs=0.1)
