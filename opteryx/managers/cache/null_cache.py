# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Any


class NullCache:
    def get(self, key: bytes) -> None:
        return None

    def set(self, key: bytes, value: Any) -> None:
        return None

    def touch(self, key: str):
        pass
