"""
GCS-backed Key-Value Store

Expects a location like: gs://bucket/[optional-prefix]
The key provided will be the filename portion of the object key.
"""

from __future__ import annotations

import importlib
from typing import Iterable
from typing import Union
from urllib.parse import urlparse

from orso.tools import single_item_cache

from opteryx.exceptions import MissingDependencyError
from opteryx.managers.kvstores import BaseKeyValueStore

GoogleAPIError = Exception


@single_item_cache
def _gcs_client(**_kwargs):
    try:
        storage = importlib.import_module("google.cloud.storage")
    except ImportError as err:  # pragma: no cover - optional dependency
        raise MissingDependencyError("google-cloud-storage") from err
    try:
        global GoogleAPIError
        GoogleAPIError = importlib.import_module("google.api_core.exceptions").GoogleAPIError
    except Exception:
        GoogleAPIError = Exception
    return storage.Client()


class GCSKeyValueStore(BaseKeyValueStore):
    def __init__(self, location: str, **_kwargs):
        parsed = urlparse(location)
        if parsed.scheme != "gs":
            raise ValueError("location must be a gs:// URI")

        self._bucket_name = parsed.netloc
        self._prefix = parsed.path.lstrip("/")
        self._client = _gcs_client(**_kwargs)
        self._bucket = self._client.bucket(self._bucket_name)
        super().__init__(location)

    def _object_name(self, key: bytes) -> str:
        try:
            key_str = key.decode("utf-8")
        except UnicodeDecodeError:
            key_str = key.hex()
        if self._prefix:
            return f"{self._prefix}/{key_str}"
        return key_str

    def get(self, key: bytes) -> Union[bytes, None]:
        name = self._object_name(key)
        blob = self._bucket.blob(name)
        try:
            return blob.download_as_bytes()
        except (GoogleAPIError, KeyError):
            return None

    def set(self, key: bytes, value: bytes) -> None:
        name = self._object_name(key)
        blob = self._bucket.blob(name)
        blob.upload_from_string(value)

    def contains(self, keys: Iterable) -> Iterable:
        result = []
        for k in keys:
            blob = self._bucket.blob(self._object_name(k))
            try:
                if blob.exists():
                    result.append(k)
            except (GoogleAPIError, KeyError):
                continue
        return result

    def delete(self, key: bytes) -> None:
        blob = self._bucket.blob(self._object_name(key))
        try:
            blob.delete()
        except (GoogleAPIError, KeyError):
            pass
