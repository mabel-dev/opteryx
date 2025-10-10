"""
Test we can read from S3
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import AwsS3Connector
from tests import is_arm, is_mac, is_windows, skip_if

BUCKET_NAME = "mabellabs"


@skip_if(is_arm() or is_windows() or is_mac())  # reduce cost
def test_minio_storage():
    opteryx.register_store(BUCKET_NAME, AwsS3Connector)

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

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
