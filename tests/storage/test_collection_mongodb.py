"""
Test we can read from Mongo
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import MongoDbConnector
from tests.tools import is_arm, is_mac, is_windows, skip_if
from tests.tools import populate_mongo

COLLECTION_NAME = "tweets"
MONGO_CONNECTION = os.environ.get("MONGODB_CONNECTION")
MONGO_DATABASE = os.environ.get("MONGODB_DATABASE")


# skip to reduce contention
@skip_if(is_arm() or is_windows() or is_mac())
def test_mongo_storage_environment_variables():
    opteryx.register_store(COLLECTION_NAME, MongoDbConnector)

    #populate_mongo()

    conn = opteryx.connect()

    # SELECT EVERYTHING
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {COLLECTION_NAME};")
    rows = cur.arrow()
    assert rows.num_rows == 25, rows.num_rows

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {COLLECTION_NAME} GROUP BY userid;")
    rows = list(cur.fetchall())
    assert len(rows) == 2

    conn.close()


# skip to reduce contention
@skip_if(is_arm() or is_windows() or is_mac())
def test_mongo_storage_explicit_parameters():
    opteryx.register_store(
        "atlas",
        MongoDbConnector,
        database="opteryx",
        connection=MONGO_CONNECTION,
        remove_prefix=True,
    )

    conn = opteryx.connect()

    # SELECT EVERYTHING
    cur = conn.cursor()
    cur.execute("SELECT * FROM atlas.planets;")
    rows = cur.arrow()
    assert rows.num_rows == 9, rows.num_rows

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute("SELECT gravity, COUNT(*) FROM atlas.planets GROUP BY gravity;")

    rows = list(cur.fetchall())
    assert len(rows) == 8, len(rows)

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
