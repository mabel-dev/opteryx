"""
This test is to ensure that constant folding does not fold random function.

Constant folding looks for instances of expressions with no indentifiers, 
which the random function qualifies for, but actually should evaluate for
each row in the table. There are other functions but random is an interesting
case to check for.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import numpy

import opteryx
from tests import is_version, skip_if


@skip_if(is_version("3.9"))
def test_we_dont_fold_random():
    SQL = "SELECT random() AS r FROM GENERATE_SERIES(5000) AS g"
    df = opteryx.query(SQL)["r"]
    p25, p50, p75, p95, p99 = numpy.percentile(df, [25, 50, 75, 95, 99])

    # as we are dealing with random values these tests may fail.
    assert 0.4 < numpy.mean(df) < 0.6
    assert 0.2 < p25 < 0.3, p25
    assert 0.7 < p75 < 0.8, p75


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
