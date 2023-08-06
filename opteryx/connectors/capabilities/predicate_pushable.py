from orso.types import OrsoTypes

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
from opteryx.exceptions import NotSupportedError
from opteryx.managers.expression import NodeType


class PredicatePushable:
    __slots__ = ("_predicates", "supported_ops")

    # These are the operators this connector supports
    PUSHABLE_OPERATORS = {
        "Gt": ">",
        "Lt": "<",
        "Eq": "==",  # usually ==, sometimes =
        "NotEq": "!=",  # usually !=, sometimes <>
        "GtEq": ">=",
        "LtEq": "<=",
        "Like": "Like",
    }

    def __init__(self, **kwargs):
        self._predicates = []
        self.supported_ops = list(self.PUSHABLE_OPERATORS.values())

    def push_predicate(self, predicate):
        """
        Push the predicate to the set - this creates a set of ANDs
        """
        dnfed = self.to_dnf(predicate)
        if dnfed is None:
            # we can't push all predicates everywhere
            return False
        if not all(d[1] in self.supported_ops for d in dnfed):
            return False
        self._predicates.extend(dnfed)
        return True

    def to_dnf(self, root):
        """
        Convert a filter to the form used by the selection pushdown

        Version 1 only does simple predicate filters in the form
            (identifier, operator, literal)
        although we can form conjuntions (ANDs) by chaining them.

        Return None if we can't convert, or don't support the filter.
        """

        def _predicate_to_dnf(root):
            # Reduce look-ahead effort by using Exceptions to control flow
            if root.node_type == NodeType.AND:
                left = _predicate_to_dnf(root.left)
                right = _predicate_to_dnf(root.right)
                if not isinstance(left, list):
                    left = [left]
                if not isinstance(right, list):
                    right = [right]
                left.extend(right)
                return left
            if root.node_type != NodeType.COMPARISON_OPERATOR:
                raise NotSupportedError()
            if root.left.node_type != NodeType.IDENTIFIER:
                raise NotSupportedError()
            if root.left.type in (
                OrsoTypes.DOUBLE,
                OrsoTypes.INTEGER,
                OrsoTypes.VARCHAR,
            ):
                # not all operands are universally supported
                raise NotSupportedError()
            return (root.left.value, self.PUSHABLE_OPERATORS[root.value], root.right.value)

        try:
            dnf = _predicate_to_dnf(root)
            if not isinstance(dnf, list):
                dnf = [dnf]
        except NotSupportedError:
            return None
        return dnf
