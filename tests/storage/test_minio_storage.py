"""
Test we can read from MinIO
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from minio import Minio  # type:ignore

BUCKET_NAME = "opteryx"
END_POINT = "localhost:9000"
SECRETS = "minioadmin"


def populate_minio():

    import io

    client = Minio(END_POINT, SECRETS, SECRETS, secure=False)
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    data = open("tests/data/tweets/tweets-0000.jsonl", mode="rb").read()

    client.put_object(
        BUCKET_NAME, "data/tweets/data.jsonl", io.BytesIO(data), len(data)
    )


def test_minio_storage():

    import opteryx
    from opteryx.storage.adapters.blob import MinIoStorage

    populate_minio()

    storage = MinIoStorage(
        end_point=END_POINT, access_key=SECRETS, secret_key=SECRETS, secure=False
    )
    conn = opteryx.connect(reader=storage, partition_scheme=None)

    # SELECT EVERYTHING
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {BUCKET_NAME}.data.tweets;")
    rows = list(cur.fetchall())
    assert len(rows) == 25

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {BUCKET_NAME}.data.tweets GROUP BY userid;")
    rows = cur.fetchall()
    rows = list(cur.fetchall())
    assert len(rows) == 2

    conn.close()


if __name__ == "__main__":
    test_minio_storage()
