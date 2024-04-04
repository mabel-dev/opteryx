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
"""
Node Module

This module contains the Node class, which provides an implementation for dynamic attribute management.

Noteworthy features and design choices:

1. Dynamic Attributes: The Node class allows you to set and get attributes dynamically, storing them in an internal dictionary.
2. Attribute Validation: Attributes starting with an underscore are not allowed.
3. Property Access: The class provides a `properties` method that exposes the internal attributes for external use.
4. Attribute Defaults: When attempting to access an attribute that doesn't exist, the `__getattr__` method will return None.
5. Deep Copy: The `copy` method allows for deep copying of the Node object, preserving the structure and values of all internal attributes.
6. JSON Representation: The `__str__` method returns a JSON representation of the internal attributes, which can be helpful for debugging or serialization.

"""

import copy
from typing import Any
from typing import Dict
from typing import Union


class Node:

    __slots__ = ("_internal", "node_type")

    def __init__(self, node_type: Union[str, int, None] = None, **kwargs: Any):
        """
        Initialize a Node with attributes.

        Parameters:
            node_type: str, optional
                The type of the node.
            **kwargs: Any
                Dynamic attributes for the node.
        """
        object.__setattr__(
            self, "_internal", kwargs
        )  # Directly set _internal using the base method
        self.node_type = node_type

    @property
    def properties(self) -> Dict[str, Any]:
        """
        Get the internal properties of the Node.

        Returns:
            Dict[str, Any]: The internal properties.
        """
        return self._internal

    def __getattr__(self, name: str) -> Any:
        """
        Retrieve attribute from the internal dictionary or the _identity.

        Parameters:
            name: str
                The name of the attribute to retrieve.

        Returns:
            Any: The attribute value.
        """
        if name in self._internal:
            return self._internal[name]
        return None

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Set attribute in the internal dictionary.

        Parameters:
            name: str
                The name of the attribute.
            value: Any
                The value to set.
        """
        if name == "_internal":
            self._internal = value
        else:
            if value is None:
                self._internal.pop(name, None)
            else:
                self._internal[name] = value

    def __str__(self) -> str:
        """
        Return a string representation of the Node using JSON serialization.

        Returns:
            str: The JSON string representation.
        """
        import orjson

        return orjson.dumps(self._internal, default=str).decode()

    def __repr__(self) -> str:
        """
        Provide a detailed representation for debugging.

        Returns:
            str: A string representation useful for debugging.
        """
        node_type = str(self.node_type)
        if node_type.startswith("LogicalPlanStepType."):
            node_type = node_type[20:]
        return f"<Node type={node_type}>"

    def copy(self) -> "Node":
        """
        Create an independent deep copy of the node.

        Returns:
            Node: The new, independent deep copy.
        """

        def _inner_copy(obj: Any) -> Any:
            """
            Create an independent inner copy of the given object.

            Parameters:
                obj: Any
                    The object to be deep copied.

            Returns:
                Any: The new, independent deep copy.
            """
            obj_type = type(obj)
            if obj_type in (list, tuple, set):
                return obj_type(_inner_copy(item) for item in obj)
            if obj_type == dict:
                return {key: _inner_copy(value) for key, value in obj.items()}
            if hasattr(obj, "copy"):
                return obj.copy()
            if hasattr(obj, "deepcopy"):
                return copy.deepcopy(obj)
            return obj

        new_node = Node(**{key: _inner_copy(value) for key, value in self._internal.items()})
        return new_node
