import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.models import Node


def test_node_simple_usage():
    n = Node("", a=1)
    n.c = 3
    assert n.a == 1
    assert n.b is None
    assert n.c == 3


def test_node_str_representation():
    n = Node("", a=1, c=3)
    stringified = str(n)
    assert '"node_type":""' in stringified
    assert '"a":1' in stringified
    assert '"c":3' in stringified

def test_node_properties():
    n = Node("", a=1, c=3)
    p = n.properties
    p.pop("uuid", None)
    assert p == {"node_type": "", "a": 1, "c": 3}


def test_node_copying():
    n = Node("", a=1, c=3)
    o = n.copy()
    assert o.a == 1
    assert o.c == 3
    assert o is not n


def test_node_deleting_attribute():
    n = Node("", a=1)
    n.a = None
    assert n.a is None
    assert "a" not in n.properties


def test_node_custom_object_attribute():
    class Custom:
        def __init__(self, x):
            self.x = x

        def copy(self):
            return Custom(self.x)

    n = Node("")
    custom_obj = Custom(10)
    n.custom = custom_obj
    p = n.copy()
    assert p.custom is not custom_obj
    assert p.custom.x == 10


def test_node_deep_copy_with_nested_object():
    n = Node("")
    n.inner = Node("", d=4)
    q = n.copy()
    assert q.inner.d == 4
    assert q.inner is not n.inner


def test_node_reassign_value():
    """Test reassigning an existing attribute."""
    n = Node("", a=1)
    n.a = 2
    assert n.a == 2
    p = n.properties
    p.pop("uuid", None)
    assert p == {"node_type": "", "a": 2}


def test_node_node_type():
    """Test setting and retrieving the node_type attribute."""
    n = Node(node_type="Fruit")
    assert n.node_type == "Fruit"
    assert n.properties.get("node_type") == "Fruit"


def test_node_copy_with_node_type():
    """Test copying a Node instance that has a node_type attribute."""
    n = Node(node_type="Animal")
    m = n.copy()
    assert m.node_type == "Animal"
    assert m is not n


def test_node_repr():
    """Test __repr__ method output."""
    from opteryx.planner.logical_planner import LogicalPlanStepType

    n = Node(node_type=LogicalPlanStepType.Distinct)
    assert repr(n) == "<Node type=Distinct>", repr(n)


def test_node_str_with_custom_obj():
    """Test __str__ method when Node instance has a custom object."""

    class Custom:
        def __str__(self):
            return "CustomObject"

    n = Node("")
    n.custom = Custom()
    assert '"custom":"CustomObject"' in str(n)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
