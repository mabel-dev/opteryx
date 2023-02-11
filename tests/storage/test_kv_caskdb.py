"""
Test CaskDB - this is the default local KV store

it's portable (100% python) but not recommended in production)
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import shutil

import pytest

from opteryx.managers.kvstores import CaskDB

TEMP_FOLDER: str = ".temp"


def test_caskdb_storage():
    try:
        # delete the old file and make sure there is a folder present
        shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
        os.makedirs(TEMP_FOLDER, exist_ok=True)

        cdb = CaskDB(f"{TEMP_FOLDER}/test.caskdb")

        # set a value
        cdb["a"] = "b"
        assert cdb["a"] == "b"

        # update a value
        cdb["a"] = "c"
        assert cdb["a"] == "c"

        # get a value
        assert cdb.get("a") == "c"
        # get a missing value
        with pytest.raises(IndexError):
            cdb["b"]
        # get a missing value with a default
        assert cdb.get("b", "g") == "g"

    finally:
        shutil.rmtree(".temp", ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    test_caskdb_storage()
    print("✅ okay")
