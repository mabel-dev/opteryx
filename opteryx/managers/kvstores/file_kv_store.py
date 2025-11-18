"""
File-backed Key-Value Store

This provider writes binary blobs to a local directory and reads them back.
It accepts a `file://` URI or a plain filesystem path as `location`.

Key: bytes (commonly hex string like b'0x...')
Value: bytes
"""

from __future__ import annotations

import os
from typing import Iterable
from typing import Union
from urllib.parse import urlparse

from opteryx.managers.kvstores import BaseKeyValueStore


class FileKeyValueStore(BaseKeyValueStore):
    """A simple file backed key-value store.

    The `location` may be a `file:///path` URI or a plain path. The store will
    create any missing directories underneath the base path.
    """

    _base_path: str

    def __init__(self, location: str, **_kwargs):
        parsed = urlparse(location)
        if parsed.scheme == "file":
            path = parsed.path
        elif parsed.scheme == "":
            path = location
        else:
            # allow pass-through for plain paths (no scheme) - otherwise caller
            # should use the factory to create a correct store
            path = location

        # normalize path and create directory
        path = os.path.abspath(os.path.expanduser(path))
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

        self._base_path = path
        super().__init__(location)

    def _filename(self, key: bytes) -> str:
        # Keys are bytes - commonly hex strings b'0x...' - map to filenames
        try:
            key_str = key.decode("utf-8")
        except UnicodeDecodeError:
            # Fallback: hex representation
            key_str = key.hex()
        # sanitize to avoid accidental path traversals
        key_str = key_str.replace("/", "_")
        return os.path.join(self._base_path, key_str)

    def get(self, key: bytes) -> Union[bytes, None]:
        filename = self._filename(key)
        if not os.path.exists(filename):
            return None
        with open(filename, "rb") as fh:
            return fh.read()

    def set(self, key: bytes, value: bytes) -> None:
        filename = self._filename(key)
        # ensure directory exists (in case of nested prefixes due to keys)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "wb") as fh:
            fh.write(value)

    def contains(self, keys: Iterable) -> Iterable:
        return [k for k in keys if os.path.exists(self._filename(k))]

    def delete(self, key: bytes) -> None:
        filename = self._filename(key)
        try:
            if os.path.exists(filename):
                os.remove(filename)
        except OSError:
            # best effort
            pass

    def touch(self, key: bytes):
        filename = self._filename(key)
        try:
            os.utime(filename, None)
        except OSError:
            return None
