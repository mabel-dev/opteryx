"""
Test HadroDB - this is the default local KV store
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import shutil

import pytest

from opteryx.managers.kvstores import HadroDB

TEMP_FOLDER: str = ".temp"


def test_hadrodb_storage():
    try:
        # delete the old file and make sure there is a folder present
        shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
        os.makedirs(TEMP_FOLDER, exist_ok=True)

        cdb = HadroDB(f"{TEMP_FOLDER}/test")

        # set a value
        cdb[b"a"] = "b"
        assert cdb[b"a"] == "b"

        # update a value
        cdb[b"a"] = "c"
        assert cdb[b"a"] == "c"

        # get a value
        assert cdb.get(b"a") == "c"
        # get a missing value
        with pytest.raises(IndexError):
            cdb[b"b"]
        # get a missing value with a default
        assert cdb.get(b"b", "g") == "g"

    finally:
        shutil.rmtree(".temp", ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    test_hadrodb_storage()
    print("✅ okay")
