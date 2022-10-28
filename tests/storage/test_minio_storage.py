"""
Test we can read from MinIO
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import io

from minio import Minio  # type:ignore

import opteryx

from opteryx.connectors import AwsS3Connector

from tests.tools import skip_on_partials

BUCKET_NAME = "miniobucket"
END_POINT = "localhost:9000"
SECRETS = "minioadmin"


def populate_minio():

    client = Minio(END_POINT, SECRETS, SECRETS, secure=False)
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    data = open("testdata/tweets/tweets-0000.jsonl", mode="rb").read()

    client.put_object(
        BUCKET_NAME, "data/tweets/data.jsonl", io.BytesIO(data), len(data)
    )


@skip_on_partials
def test_minio_storage():

    opteryx.register_store(BUCKET_NAME, AwsS3Connector)

    populate_minio()

    conn = opteryx.connect()

    # SELECT EVERYTHING
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {BUCKET_NAME}.data.tweets WITH(NO_PARTITION);")
    rows = list(cur.fetchall())
    assert len(rows) == 25

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM {BUCKET_NAME}.data.tweets WITH(NO_PARTITION) GROUP BY userid;"
    )
    rows = list(cur.fetchall())
    assert len(rows) == 2

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    test_minio_storage()
    print("✅ okay")
