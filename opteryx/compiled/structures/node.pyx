# cython: language_level=3

"""
Node Module

This module contains the Node class, which provides an implementation for dynamic
attribute management.

Noteworthy features and design choices:

1. Dynamic Attributes: The Node class allows you to set and get attributes
   dynamically, storing them in an internal dictionary.
2. Attribute Validation: Attributes starting with an underscore are not allowed.
3. Property Access: The class provides a `properties` method that exposes the internal
   attributes for external use.
4. Attribute Defaults: When attempting to access an attribute that doesn't exist, the
   `__getattr__` method will return None.
5. Deep Copy: The `copy` method allows for deep copying of the Node object, preserving
   the structure and values of all internal attributes.
6. JSON Representation: The `__str__` method returns a JSON representation of the
   internal attributes, which can be helpful for debugging or serialization.

Node accessors are one of the most frequently called functions, at the time of converting
to Cython, the shape and errors regression test suite called the getter about 850k times
during execution for about 0.2 seconds, Cython runs this class approx 33% faster that the
raw Python version.
"""

from cpython cimport dict

cdef class Node:
    cdef:
        dict _properties
        object node_type

    def __cinit__(self, node_type, **attributes):
        self.node_type = node_type
        self._properties = dict(attributes)

    def __getattr__(self, str name):
        if name == 'node_type':
            return self.node_type
        try:
            return self._properties[name]
        except KeyError:
            return None

    def __setattr__(self, str name, object value):
        if name == 'node_type':
            self.node_type = value
        elif value is None:
            self._properties.pop(name, None)
        else:
            self._properties[name] = value

    @property
    def properties(self):
        # Merge _properties with node_type and return as a new dict
        return {'node_type': self.node_type, **self._properties}

    def get(self, str name, object default=None):
        return self._properties.get(name, default)

    def __str__(self):
        import orjson
        # Serialize the full properties including node_type
        return orjson.dumps(self.properties, default=str).decode('utf-8')

    def __repr__(self):
        cdef str node_type = str(self.node_type)
        cdef str node_type_str = node_type[20:] if node_type.startswith("LogicalPlanStepType.") else node_type
        return f"<Node type={node_type_str}>"

    def copy(self) -> "Node":
        """
        Create an independent deep copy of the node.

        Returns:
            Node: The new, independent deep copy.
        """

        def _inner_copy(obj):
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
                import copy
                return copy.deepcopy(obj)
            return obj

        new_node = Node(self.node_type, **{key: _inner_copy(value) for key, value in self._properties.items()})
        return new_node
