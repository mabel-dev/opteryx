"""
Test we can read from GCS
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import GcpCloudStorageConnector

BUCKET_NAME = "opteryx"


def test_gcs_storage():
    opteryx.register_store(BUCKET_NAME, GcpCloudStorageConnector)

    conn = opteryx.connect()

    # SELECT EVERYTHING
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {BUCKET_NAME}.space_missions;")
    assert cur.rowcount == 4630, cur.rowcount

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) AS Missions, Company FROM {BUCKET_NAME}.space_missions GROUP BY Company;"
    )
    assert cur.rowcount == 62, cur.rowcount

    #  PUSHDOWNS (Parquet)
    cur = conn.cursor()
    cur.execute(f"SELECT Company FROM {BUCKET_NAME}.space_missions WHERE Rocket_Status = 'Active';")
    assert cur.columncount == 1, cur.columncount
    assert cur.rowcount == 1010, cur.rowcount
    assert cur.stats["columns_read"] == 1, cur.stats  # we didn't read out Rocket_Status
    assert cur.stats["rows_read"] == 1010, cur.stats

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
