from typing import Any


class NullCache:

    def get(self, key: bytes) -> None:
        return None

    def set(self, key: bytes, value: Any) -> None:
        return None
