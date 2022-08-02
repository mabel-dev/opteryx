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
Expressions describe a calculation or evaluation of some sort.

It is defined as an expression tree of binary and unary operators, and functions.
"""
from enum import Enum

import numpy


class Expression:
    def __init__(self):
        pass

    def parse(self, ast):
        pass


class NodeType(int, Enum):
    """The types of Nodes we will see"""

    UNKNOWN: int = 0

    AND: int = 1
    OR: int = 2
    XOR: int = 4
    NOT: int = 8

    WILDCARD: int = 16
    OPERATOR: int = 32
    FUNCTION: int = 64
    IDENTIFIER: int = 128

    LITERAL_NUMERIC: int = 256
    LITERAL_VARCHAR: int = 512
    LITERAL_BOOLEAN: int = 1024
    LITERAL_INTERVAL: int = 2048
    LITERAL_LIST: int = 4096
    LITERAL_STRUCT: int = 8192
    LITERAL_TIMESTAMP: int = 16384
    LITERAL_NONE: int = 22
    LITERAL_TRUE: int = 23
    LITERAL_FALSE: int = 24


class ExpressionTreeNode:
    __slots__ = (
        "_token_type",
        "_value",
        "_left",
        "_right",
        "_centre",
        "_parameters",
    )

    def __init__(self, token_type, *, value = None, left_node = None, right_node = None, centre_node = None, parameters = None):
        self._token_type: NodeType = token_type
        self._value = value
        self._left = left_node
        self._right = right_node
        self._centre = centre_node
        self._parameters = parameters

        if self._token_type == NodeType.UNKNOWN:
            raise ValueError(f"ExpressionNode of unknown type in plan. {self._value}")

    @property
    def value(self):
        return self._value

    def _inner_print(self, node, prefix):
        ret = prefix + node.value + "\n"
        prefix += " |"
        if node._left:
            ret += self._inner_print(node._left, prefix=prefix+"- ")
        if node._right:
            ret += self._inner_print(node._right, prefix=prefix+"- ")
        return ret

    def __str__(self):
        return self._inner_print(self, "")



OPERATORS: dict = {
    "divide": numpy.divide,
    "minus": numpy.subtract,
    "modulo": numpy.mod,
    "multiply": numpy.multiply,
    "plus": numpy.add,
    "stringconcat": NotImplementedError,
}
