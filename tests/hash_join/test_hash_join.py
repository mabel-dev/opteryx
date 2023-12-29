"""
This is probably going to be reused in the future, it has the distinct
advantage that the right table only needs to by hashed once and can
be reused, unlike to pyarrow version
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_hash_inner_join():
    import opteryx
    from opteryx.third_party.pyarrow_ops import inner_join, left_join

    right_table = opteryx.query_to_arrow("SELECT * FROM $planets")
    left_table = opteryx.query_to_arrow("SELECT * FROM $satellites")

    joined = inner_join(left_table, right_table, ["id"], ["id"])

    assert joined.shape == (9, 25)

    joined = left_join(left_table, right_table, ["id"], ["id"])

    assert joined.shape == (177, 25)


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
