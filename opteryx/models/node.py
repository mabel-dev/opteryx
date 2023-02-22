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
    def _is_valid_key(self, key):
        if key.startswith("_"):
            raise AttributeError("Node cannot have dynamic attributes starting with an underscore")

    def __init__(self, **kwargs):
        """
        A Python object with run-time defined attributes.
        """
        internal = {}
        if isinstance(kwargs, dict):
            for k, v in kwargs.items():
                self._is_valid_key(k)
                internal[k] = v
        self.__dict__["_internal"] = internal

    def __getattr__(self, __name: str) -> Any:
        internal = self.__dict__.get("_internal", {})
        return internal.get(__name)

    def __setattr__(self, __name: str, __value: Any) -> None:
        internal = self.__dict__.get("_internal", {})
        if __name != "_internal":
            self._is_valid_key(__name)
            if __value is None:
                internal.pop(__name, None)
            else:
                internal[__name] = __value

    def __str__(self) -> str:
        internal = self.__dict__.get("_internal", {})
        return orjson.dumps(internal).decode()


if __name__ == "__main__":  # pragma: no cover
    n = Node(a=3)
    n.name = "john"
    print(n.a)
    print(n.b)
    print(n.name)
    print(repr(n))
