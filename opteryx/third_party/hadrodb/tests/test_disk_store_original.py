import os
import shutil
import typing
import pytest

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.third_party.hadrodb import HadroDB

TEMP_FOLDER: str = "temp/cask"


class TempStorageFile:
    """
    TempStorageFile provides a wrapper over the temporary files which are used in
    testing.

    Python has two APIs to create temporary files, tempfile.TemporaryFile and
    tempfile.mkstemp. Files created by tempfile.TemporaryFile gets deleted as soon as
    they are closed. Since we need to do tests for persistence, we might open and
    close a file multiple times. Files created using tempfile.mkstemp don't have this
    limitation, but they have to deleted manually. They don't get deleted when the file
    descriptor is out scope or our program has exited.

    Args:
        path (str): path to the file where our data needs to be stored. If the path
            parameter is empty, then a temporary will be created using tempfile API
    """

    def __init__(self, path: typing.Optional[str] = None):
        if path:
            return
        shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
        os.makedirs(TEMP_FOLDER, exist_ok=True)

    def clean_up(self) -> None:
        shutil.rmtree(TEMP_FOLDER, ignore_errors=True)


@pytest.fixture(scope="module")
def file():
    return TempStorageFile()


def test_get():
    store = HadroDB(collection=TEMP_FOLDER)
    store.set("name", "jojo")
    assert store.get("name") == "jojo"
    store.close()


def test_invalid_key():
    store = HadroDB(collection=TEMP_FOLDER)
    with pytest.raises(IndexError):
        store.get("some key")
    store.close()


def test_dict_api():
    store = HadroDB(collection=TEMP_FOLDER)
    store["name"] = "jojo"
    assert store["name"] == "jojo"
    store.close()


def test_persistence():
    store = HadroDB(collection=TEMP_FOLDER)

    tests = {
        "crime and punishment": "dostoevsky",
        "anna karenina": "tolstoy",
        "war and peace": "tolstoy",
        "hamlet": "shakespeare",
        "othello": "shakespeare",
        "brave new world": "huxley",
        "dune": "frank herbert",
    }
    for k, v in tests.items():
        store.set(k, v)
        assert store.get(k) == v
    store.close()

    store = HadroDB(collection=TEMP_FOLDER)
    for k, v in tests.items():
        assert store.get(k) == v
    store.close()


def test_deletion():
    shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
    store = HadroDB(collection=TEMP_FOLDER)

    tests = {
        "crime and punishment": "dostoevsky",
        "anna karenina": "tolstoy",
        "war and peace": "tolstoy",
        "hamlet": "shakespeare",
        "othello": "shakespeare",
        "brave new world": "huxley",
        "dune": "frank herbert",
    }
    for k, v in tests.items():
        store.set(k, v)
    for k, _ in tests.items():
        store.set(k, "")
    store.set("end", "yes")
    store.close()

    store = HadroDB(collection=TEMP_FOLDER)
    for k, v in tests.items():
        assert store.get(k) == ""
    assert store.get("end") == "yes"
    store.close()


def test_get_new_file() -> None:
    temp_db_file_path = "temp.db"
    t = TempStorageFile(path=temp_db_file_path)

    store = HadroDB(collection=temp_db_file_path)
    store.set("name", "jojo")
    assert store.get("name") == "jojo"
    store.close()

    # check for key again
    store = HadroDB(collection=temp_db_file_path)
    assert store.get("name") == "jojo"
    store.close()

    # remove temp db file
    shutil.rmtree(temp_db_file_path, ignore_errors=True)
    t.clean_up()


if __name__ == "__main__":  # pragma: no cover
    test_deletion()
    test_dict_api()
    test_get()
    test_get_new_file()
    test_invalid_key()
    test_persistence()

    print("okay")
