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
    cur.execute(f"SELECT * FROM {BUCKET_NAME}.space_missions WITH(NO_PARTITION);")
    assert cur.rowcount == 4630, cur.rowcount

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) AS Missions, Company FROM {BUCKET_NAME}.space_missions WITH(NO_PARTITION) GROUP BY Company;"
    )
    assert cur.rowcount == 62, cur.rowcount

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    test_gcs_storage()
    print("✅ okay")
