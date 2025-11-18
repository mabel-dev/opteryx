from opteryx.managers.kvstores import create_kv_store
from opteryx.managers.kvstores import FileKeyValueStore, S3KeyValueStore, GCSKeyValueStore
from opteryx.exceptions import MissingDependencyError


def test_create_kv_store_detects_file_scheme(tmp_path):
    s = str(tmp_path)
    store = create_kv_store(s)
    assert isinstance(store, FileKeyValueStore)

    store2 = create_kv_store(f"file://{s}")
    assert isinstance(store2, FileKeyValueStore)


def test_create_kv_store_detects_s3_scheme():
    try:
        store = create_kv_store("s3://mybucket/prefix")
        assert isinstance(store, S3KeyValueStore)
    except MissingDependencyError:
        # acceptable if boto3 is not installed in the test environment
        pass


def test_create_kv_store_detects_gs_scheme():
    try:
        store = create_kv_store("gs://mybucket/prefix")
        assert isinstance(store, GCSKeyValueStore)
    except MissingDependencyError:
        # acceptable if google-cloud deps are not installed
        pass
