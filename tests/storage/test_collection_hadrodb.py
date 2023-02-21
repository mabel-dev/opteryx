"""
Test we can read from HadroDB
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

from opteryx.connectors import HadroConnector


def test_hadrodb_storage():
    opteryx.register_store("hadro", HadroConnector, remove_prefix=True)

    cur = opteryx.query("SELECT * FROM hadro.testdata.hadro.planets;")
    assert cur.rowcount == 9, cur.rowcount

    # PROCESS THE DATA IN SOME WAY
    cur = opteryx.query(
        "SELECT gravity, COUNT(*) FROM hadro.testdata.hadro.planets GROUP BY gravity;"
    )
    assert cur.rowcount == 8, cur.rowcount
    cur = opteryx.query(
        "SELECT * FROM hadro.testdata.hadro.planets WHERE name LIKE '%a%';"
    )
    assert cur.rowcount == 4, cur.rowcount


if __name__ == "__main__":  # pragma: no cover
    test_hadrodb_storage()

    print("✅ okay")
