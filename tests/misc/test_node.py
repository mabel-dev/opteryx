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

    assert str(n) == '{"a":1,"c":3}', str(n)

    m = Node(node_type="apple")
    assert repr(m) == "apple ()"
    o = Node(88)  # not recommended, but supported
    assert repr(o) == "88 ()"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
