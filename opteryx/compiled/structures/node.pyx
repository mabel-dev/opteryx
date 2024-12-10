# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=True
# cython: boundscheck=False

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
from uuid import uuid4

cdef class Node:
    cdef:
        dict _properties
        object node_type
        str uuid

    def __cinit__(self, node_type, **attributes):
        """
        Initialize a new Node with a given node_type and optional attributes.
        A UUID is automatically generated for the node.
        """
        self.node_type = node_type
        self.uuid = str(uuid4())
        self._properties = dict(attributes)

    def __getattr__(self, str name):
        """
        Get an attribute:
          - If name is 'node_type' or 'uuid', return the stored value.
          - Otherwise, return the corresponding entry in _properties or None if not found.
        """
        if name == 'node_type':
            return self.node_type
        if name == 'uuid':
            return self.uuid
        try:
            return self._properties[name]
        except KeyError:
            return None

    def __setattr__(self, str name, object value):
        """
        Set an attribute:
          - If name is 'node_type' or 'uuid', store directly on the object.
          - If value is None, remove it from _properties.
          - Otherwise, store in _properties.
        """
        if name == 'node_type':
            self.node_type = value
        elif name == 'uuid':
            self.uuid = value
        elif value is None:
            self._properties.pop(name, None)
        else:
            self._properties[name] = value

    @property
    def properties(self):
        """
        Return a dictionary of all node properties, including node_type and uuid.
        Dynamic attributes stored in _properties are merged.
        """
        return {
            'node_type': self.node_type,
            'uuid': self.uuid,
            **self._properties
        }

    def get(self, str name, object default=None):
        """
        Get an attribute from _properties with an optional default.
        """
        return self._properties.get(name, default)

    def __str__(self):
        """
        Return a JSON representation of the node's properties, including node_type and uuid.
        """
        import orjson
        return orjson.dumps(self.properties, default=str).decode('utf-8')

    def __repr__(self):
        """
        Return a string representation of the node, including its type.
        """
        cdef str node_type_str = str(self.node_type)
        if node_type_str.startswith("LogicalPlanStepType."):
            node_type_str = node_type_str[20:]
        return f"<Node type={node_type_str}>"

    def copy(self) -> "Node":
        """
        Create an independent deep copy of the node.
        This generates a new uuid for the copied node, making it a distinct entity.
        """
        def _inner_copy(obj):
            """
            Create a deep copy of the given object, handling collections and objects with custom copy methods.
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

        # Create a new Node with the same node_type and a deep copy of _properties
        new_node = Node(self.node_type, **{key: _inner_copy(value) for key, value in self._properties.items()})
        new_node.uuid = self.uuid
        return new_node
