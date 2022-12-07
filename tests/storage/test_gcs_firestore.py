"""
Test we can read from GCS
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

from opteryx.connectors import GcpFireStoreConnector


def test_firestore_storage():

    opteryx.register_store("dwarves", GcpFireStoreConnector)
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()

    # SELECT EVERYTHING
    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves;")
    assert cur.rowcount == 7

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute("SELECT actor, COUNT(*) FROM dwarves GROUP BY actor;")
    assert cur.rowcount == 6, cur.rowcount

    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor = 'Pinto Colvig';")
    assert cur.rowcount == 2, cur.rowcount

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM dwarves WHERE actor = 'Pinto Colvig' and name = 'Sleepy';"
    )
    assert cur.rowcount == 1, cur.rowcount

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    test_firestore_storage()
    print("✅ okay")
