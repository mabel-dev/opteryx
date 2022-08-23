"""
Test we can read from MinIO
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import io

import orjson
import pymongo  # type:ignore

import opteryx

from opteryx.storage.adapters.document import MongoDbStore
from opteryx.storage import register_prefix


COLLECTION_NAME = "mongo"
MONGO_CONNECTION = os.environ.get("MONGO_CONNECTION")
MONGO_DATABASE = os.environ.get("MONGO_DATABASE")


def populate_mongo():

    myclient = pymongo.MongoClient(MONGO_CONNECTION)
    mydb = myclient[MONGO_DATABASE]
    collection = mydb[COLLECTION_NAME]

    collection.drop()

    data = open("tests/data/tweets/tweets-0000.jsonl", mode="rb").read()

    collection.insert_many(map(orjson.loads, data.split(b"\n")[:-1]))


def test_mongo_storage():

    register_prefix(COLLECTION_NAME, MongoDbStore)

    populate_mongo()

    conn = opteryx.connect()

    # SELECT EVERYTHING
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {COLLECTION_NAME}.data.tweets WITH(NO_PARTITION);")
    rows = list(cur.fetchall())
    assert len(rows) == 25, len(rows)

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM {COLLECTION_NAME}.data.tweets WITH(NO_PARTITION) GROUP BY userid;"
    )
    rows = list(cur.fetchall())
    assert len(rows) == 2

    conn.close()


if __name__ == "__main__":
    test_mongo_storage()
    print("✅ okay")
