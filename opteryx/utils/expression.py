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

    ROOT: int = 0
    AND: int = 1
    OR: int = 2
    XOR: int = 3
    NOT: int = 4
    OPERATOR: int = 5
    FUNCTION: int = 6
    IDENTIFIER: int = 7
    LITERAL: int = 8


class ExpressionTreeNode:
    __slots__ = (
        "_token_type",
        "_value",
        "_left",
        "_right",
        "_parameters",
    )

    def __init__(self, token_type, value):
        self._token_type: NodeType = token_type
        self._value = value
        self._left = None
        self._right = None
        self._parameters = []


OPERATORS: dict = {
    "divide": numpy.divide,
    "minus": numpy.subtract,
    "modulo": numpy.mod,
    "multiply": numpy.multiply,
    "plus": numpy.add,
    "stringconcat": NotImplementedError,
}
