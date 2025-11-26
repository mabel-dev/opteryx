"""
S3-backed Key-Value Store

Expects a location like: s3://bucket/[optional-prefix]
The key provided will be the filename portion of the object key.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import os
from typing import Iterable
from typing import Union
from urllib.parse import urlparse

from orso.tools import single_item_cache

from opteryx.exceptions import MissingDependencyError
from opteryx.managers.kvstores import BaseKeyValueStore

Minio = None
S3Error = Exception


@single_item_cache
def _s3_client(**_kwargs):
    global Minio, S3Error
    try:
        mod = importlib.import_module("minio")
        Minio = getattr(mod, "Minio")
    except ImportError:
        raise MissingDependencyError("minio")
    try:
        S3Error = getattr(importlib.import_module("minio.error"), "S3Error")
    except Exception:
        S3Error = Exception

    end_point = _kwargs.get("S3_END_POINT", os.environ.get("MINIO_END_POINT"))
    access_key = _kwargs.get("S3_ACCESS_KEY", os.environ.get("MINIO_ACCESS_KEY"))
    secret_key = _kwargs.get("S3_SECRET_KEY", os.environ.get("MINIO_SECRET_KEY"))
    secure = _kwargs.get("S3_SECURE", str(os.environ.get("MINIO_SECURE", "TRUE")).lower() == "true")

    if end_point is None:
        raise MissingDependencyError("MINIO_END_POINT / S3_END_POINT")

    # Minio v7 introduced keyword-only constructor parameters (endpoint=...).
    # Call using keywords for compatibility with both older and newer SDKs.
    try:
        return Minio(
            endpoint=end_point, access_key=access_key, secret_key=secret_key, secure=secure
        )
    except TypeError:
        # Fallback: older Minio versions accept positional args
        return Minio(end_point, access_key, secret_key, secure=secure)


class S3KeyValueStore(BaseKeyValueStore):
    """S3-backed store that maps key -> s3://bucket/prefix/<key>"""

    def __init__(self, location: str, **_kwargs):
        parsed = urlparse(location)
        if parsed.scheme != "s3":
            raise ValueError("location must be an s3:// URI")

        self._bucket = parsed.netloc
        # strip leading slash from path
        self._prefix = parsed.path.lstrip("/")
        self._client = _s3_client(**_kwargs)
        super().__init__(location)

    def _object_key(self, key: bytes) -> str:
        try:
            key_str = key.decode("utf-8")
        except UnicodeDecodeError:
            key_str = key.hex()
        if self._prefix:
            return f"{self._prefix}/{key_str}"
        return key_str

    def get(self, key: bytes) -> Union[bytes, None]:
        obj_key = self._object_key(key)
        try:
            stream = self._client.get_object(self._bucket, obj_key)
            try:
                data = stream.read()
                return data
            finally:
                with contextlib.suppress(Exception):
                    stream.close()
        except (S3Error, KeyError):
            return None

    def set(self, key: bytes, value: bytes) -> None:
        obj_key = self._object_key(key)
        buffer = io.BytesIO(value)
        self._client.put_object(self._bucket, obj_key, buffer, length=len(value))

    def contains(self, keys: Iterable) -> Iterable:
        result = []
        for k in keys:
            try:
                self._client.stat_object(self._bucket, self._object_key(k))
                result.append(k)
            except (S3Error, KeyError):
                continue
        return result

    def delete(self, key: bytes) -> None:
        with contextlib.suppress(S3Error, KeyError):
            self._client.remove_object(self._bucket, self._object_key(key))
