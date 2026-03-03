import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def test_fetchone_reads_all_rows():
    cur = opteryx.query("SELECT id FROM $planets ORDER BY id")
    ids = []
    while True:
        row = cur.fetchone()
        if row is None:
            break
        ids.append(row[0])

    assert ids == list(range(1, 10)), ids


def test_fetchone_returns_none_when_exhausted():
    cur = opteryx.query("SELECT id FROM $planets ORDER BY id LIMIT 1")
    first = cur.fetchone()
    assert first is not None
    assert first[0] == 1
    assert cur.fetchone() is None


def test_fetchone_after_fetchmany():
    cur = opteryx.query("SELECT id FROM $planets ORDER BY id")
    first_two = cur.fetchmany(2)
    assert [r[0] for r in first_two] == [1, 2]
    next_one = cur.fetchone()
    assert next_one[0] == 3

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
