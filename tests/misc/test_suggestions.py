"""
Test show functions works; the number of functions is constantly changing so test it's
more than it was when we last reviewed this test.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_hint_hints():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets WITH(NO_PARTITIONS)")
    assert cur.messages == ["Hint `NO_PARTITIONS` is not recognized, did you mean `NO_PARTITION`?"]


if __name__ == "__main__":  # pragma: no cover
    test_hint_hints()

    print("✅ okay")
