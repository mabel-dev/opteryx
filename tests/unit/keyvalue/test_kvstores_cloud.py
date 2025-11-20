import sys
from typing import Dict

from opteryx.managers.kvstores import create_kv_store


class FakeS3Client:
    def __init__(self):
        self._store: Dict[str, bytes] = {}

    def get_object(self, Bucket, Key):
        key = f"{Bucket}/{Key}"
        if key not in self._store:
            raise KeyError("NotFound")
        return FakeBody(self._store[key])

    def put_object(self, Bucket, Key, Body, length=None, **_kwargs):
        # Minio API expects a readable file-like object and length
        if hasattr(Body, "read"):
            try:
                Body.seek(0)
            except (AttributeError, OSError):
                pass
            data = Body.read()
        else:
            data = Body
        self._store[f"{Bucket}/{Key}"] = data

    def stat_object(self, Bucket, Key):
        if f"{Bucket}/{Key}" not in self._store:
            raise KeyError("NotFound")

    def remove_object(self, Bucket, Key):
        self._store.pop(f"{Bucket}/{Key}", None)


class FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data
    def close(self):
        return None


def test_s3_kv_store_with_fake_client():
    # inject fake minio into sys.modules
    fake_minio = type("minio", (), {})()
    fake_client = FakeS3Client()
    fake_minio.Minio = lambda endpoint, access_key, secret_key, secure=True: fake_client
    sys.modules["minio"] = fake_minio

    try:
        store = create_kv_store("s3://mybucket/pfx", S3_END_POINT="minio", S3_ACCESS_KEY="a", S3_SECRET_KEY="b")
        key = b"0x1"
        assert store.get(key) is None
        val = b"abc"
        store.set(key, val)
        assert store.get(key) == val
        assert store.contains([key]) == [key]
        store.delete(key)
        assert store.get(key) is None
    finally:
        del sys.modules["minio"]


class FakeBlob:
    def __init__(self, data: bytes = b""):
        self._data = data

    def download_as_bytes(self):
        if self._data is None:
            raise KeyError("NotFound")
        return self._data

    def upload_from_string(self, value: bytes):
        self._data = value

    def exists(self):
        return self._data is not None

    def delete(self):
        self._data = None


class FakeBucket:
    def __init__(self):
        self._blobs: Dict[str, FakeBlob] = {}

    def blob(self, name: str):
        if name not in self._blobs:
            self._blobs[name] = FakeBlob(None)
        return self._blobs[name]


class FakeGCSClient:
    def __init__(self):
        self._buckets = {"bucket": FakeBucket()}

    def bucket(self, name: str):
        if name not in self._buckets:
            self._buckets[name] = FakeBucket()
        return self._buckets[name]


def test_gcs_kv_store_with_fake_client():
    fake_pkg = type("google", (), {})()
    fake_cloud = type("cloud", (), {})()
    fake_storage = type("storage", (), {})()
    def client_factory():
        return FakeGCSClient()
    fake_storage.Client = client_factory
    fake_cloud.storage = fake_storage
    fake_pkg.cloud = fake_cloud
    sys.modules["google"] = fake_pkg
    sys.modules["google.cloud"] = fake_cloud
    sys.modules["google.cloud.storage"] = fake_storage

    try:
        store = create_kv_store("gs://bucket/pfx")
        key = b"0x1"
        assert store.get(key) is None
        val = b"hello"
        store.set(key, val)
        assert store.get(key) == val
        assert store.contains([key]) == [key]
        store.delete(key)
        assert store.get(key) is None
    finally:
        del sys.modules["google.cloud.storage"]
        del sys.modules["google.cloud"]
        del sys.modules["google"]


def test_s3_kv_store_with_fake_client_and_s3error_class():
    # ensure that KeyError('NotFound') is treated like a not-found error even
    # if the minio package (and S3Error class) are present in the environment
    fake_minio = type("minio", (), {})()
    fake_client = FakeS3Client()
    fake_minio.Minio = lambda endpoint, access_key, secret_key, secure=True: fake_client
    fake_error = type("minio.error", (), {})()
    class S3Err(Exception):
        pass
    fake_error.S3Error = S3Err
    sys.modules["minio"] = fake_minio
    sys.modules["minio.error"] = fake_error
    try:
        store = create_kv_store("s3://mybucket/pfx", S3_END_POINT="minio", S3_ACCESS_KEY="a", S3_SECRET_KEY="b")
        key = b"0x1"
        assert store.get(key) is None
    finally:
        del sys.modules["minio.error"]
        del sys.modules["minio"]


def test_gcs_kv_store_with_fake_client_and_googleapierror_class():
    # ensure KeyError('NotFound') is treated as not-found even if the
    # google.api_core.exceptions.GoogleAPIError class is present in the env
    fake_pkg = type("google", (), {})()
    fake_cloud = type("cloud", (), {})()
    fake_storage = type("storage", (), {})()
    def client_factory():
        return FakeGCSClient()
    fake_storage.Client = client_factory
    fake_cloud.storage = fake_storage
    fake_pkg.cloud = fake_cloud
    sys.modules["google"] = fake_pkg
    sys.modules["google.cloud"] = fake_cloud
    sys.modules["google.cloud.storage"] = fake_storage
    # now create a fake google.api_core.exceptions module
    fake_exceptions = type("google.api_core.exceptions", (), {})()
    class GoogleErr(Exception):
        pass
    fake_exceptions.GoogleAPIError = GoogleErr
    sys.modules["google.api_core.exceptions"] = fake_exceptions
    try:
        store = create_kv_store("gs://bucket/pfx")
        key = b"0x1"
        assert store.get(key) is None
    finally:
        del sys.modules["google.api_core.exceptions"]
        del sys.modules["google.cloud.storage"]
        del sys.modules["google.cloud"]
        del sys.modules["google"]
