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
    rows = list(cur.fetchall())
    assert len(rows) == 7

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute("SELECT actor, COUNT(*) FROM dwarves GROUP BY actor;")
    rows = list(cur.fetchall())
    assert len(rows) == 6, len(rows)

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    test_firestore_storage()
    print("✅ okay")
