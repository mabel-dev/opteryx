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


class Node:
    def __init__(self, node_type: str = None, **kwargs):
        """
        Initialize a Node with attributes.

        Args:
            node_type: The type of the node.
            **kwargs: Dynamic attributes for the node.
        """
        object.__setattr__(self, "_internal", {})  # Directly set _internal using the base method
        if node_type:
            self._internal["node_type"] = node_type
        for k, v in kwargs.items():
            self._is_valid_key(k)
            self._internal[k] = v

    def _is_valid_key(self, key: str) -> None:
        """Check if the key is valid for the node."""
        if key.startswith("_"):
            raise AttributeError("Node cannot have dynamic attributes starting with an underscore")

    @property
    def properties(self):
        return self.__dict__.get("_internal", {})

    def __getattr__(self, name: str) -> Any:
        """Retrieve attribute from the internal dictionary."""
        return self._internal.get(name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set attribute in the internal dictionary."""
        if name == "_internal":
            self.__dict__[name] = value
        else:
            self._is_valid_key(name)
            if value is None:
                self._internal.pop(name, None)
            else:
                self._internal[name] = value

    def __str__(self) -> str:
        import orjson

        return orjson.dumps(self._internal, default=str).decode()

    def __repr__(self) -> str:
        """Provide a detailed representation for debugging."""
        node_type = self._internal.get("node_type", "<unspecified>")
        # Modify based on your specific needs or remove.
        if node_type.startswith("LogicalPlanStepType."):
            node_type = node_type[20:]
        return f"<Node type={node_type}>"

    def copy(self) -> "Node":
        """Create an independent copy of the node."""
        new_node = Node()
        for key, value in self._internal.items():
            setattr(new_node, key, value)
        return new_node
