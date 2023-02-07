import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest


def test_node():
    from opteryx.models.node import Node

    # simple usage
    n = Node(a=1)
    n.c = 3
    assert n.a == 1
    assert n.b is None
    assert n.c == 3

    # invalid attribute
    with pytest.raises(AttributeError):
        n._secret = "top"

    # invalid attribute on load
    with pytest.raises(AttributeError):
        b = Node(_node=False)

    assert repr(n) == '{"a":1,"c":3}', repr(n)


if __name__ == "__main__":  # pragma: no cover
    test_node()
    print("✅ okay")
