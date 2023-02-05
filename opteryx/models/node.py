# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any

import orjson


class Node:
    _internal: dict = {}

    def _is_valid_key(self, key):
        if key.startswith("_"):
            raise AttributeError(
                "Node cannot have dynamic attributes starting with an underscore"
            )

    def __init__(self, **kwargs):
        """
        A Python object with run-time defined attributes.
        """
        if isinstance(kwargs, dict):
            for k, v in kwargs.items():
                self._is_valid_key(k)
                self._internal[k] = v

    def __getattr__(self, __name: str) -> Any:
        return self._internal.get(__name)

    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name != "_internal":
            self._is_valid_key(__name)
            if __value is None:
                self._internal.pop(__name, None)
            else:
                self._internal[__name] = __value

    def __repr__(self) -> str:
        return orjson.dumps(self._internal).decode()


if __name__ == "__main__":
    n = Node(a=3)
    n.name = "john"
    print(n.a)
    print(n.b)
    print(n.name)
    print(repr(n))
