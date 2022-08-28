"""
Test we can read from GCS
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

from opteryx.connectors import GcsStorage

BUCKET_NAME = "opteryx"


def populate_gcs():

    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:9090"
    client = storage.Client(
        credentials=AnonymousCredentials(),
        project="testing",
    )
    bucket = client.bucket(BUCKET_NAME)
    try:
        bucket.delete(force=True)
    except:  # pragma: no cover
        pass
    bucket = client.create_bucket(BUCKET_NAME)

    data = open("tests/data/tweets/tweets-0000.jsonl", mode="rb").read()

    blob = bucket.blob("data/tweets/data.jsonl")
    blob.upload_from_string(data, content_type="application/octet-stream")

    opteryx.register_prefix(BUCKET_NAME, GcsStorage)


def test_gcs_storage():

    populate_gcs()

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


if __name__ == "__main__":
    test_gcs_storage()
    print("✅ okay")
