import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import numpy
import opteryx


def test_we_dont_fold_random():

    SQL = "SELECT random() AS r FROM GENERATE_SERIES(5000) AS g"
    df = opteryx.query(SQL)["r"]
    p25, p50, p75, p95, p99 = numpy.percentile(df, [25, 50, 75, 95, 99])

    assert 0.4 < numpy.mean(df) < 0.6
    assert 0.2 < p25 < 0.3, p25
    assert 0.7 < p75 < 0.8, p75


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
