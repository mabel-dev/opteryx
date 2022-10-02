"""
#489 - The HASH join (aka the legacy join) has intermittent failures when multiple
columns were in the join predicate. This was because the code was sorting the column
names after they'd been converted to their internal representation - this should
have failed about 50% of the time, but was closer to 33%.

This test runs the join 20 times to confirm it still works - it's nearly impossible
a 66% chance thing will happen 20 times in a row.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def test_hash_join_consistency():

    conn = opteryx.connect()

    for i in range(25):
        # there was about a 50% failure of this query failing to return any rows due to
        # a bug in the join implementation. 1/(2^25) is a small chance this test will
        # pass if the problem still exists.
        cur = conn.cursor()
        cur.execute("SELECT * FROM $planets INNER JOIN $planets USING (name, id)")
        assert cur.arrow().num_rows == 9


if __name__ == "__main__":  # pragma: no cover

    test_hash_join_consistency()

    print("✅ okay")
