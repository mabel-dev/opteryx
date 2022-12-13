import abc

from opteryx.exceptions import NotSupportedError
from opteryx.managers.expression import NodeType


PUSHABLE_OPERATORS = {
    # these are almost universally supported
    "Gt": ">",
    "Lt": "<",
    "Eq": "==",  # usually ==, sometimes =
    "NotEq": "!=",  # usually !=, sometimes <>
    "GtEq": ">=",
    "LtEq": "<=",
}


class PredicatePushable(abc.ABC):
    @staticmethod
    def to_dnf(root):
        """
        Convert a filter to the form used by the selection pushdown

        Version 1 only does simple predicate filters in the form
            (identifier, operator, literal)
        although we can form conjuntions (ANDs) by chaining them.

        Return None if we can't convert, or don't support the filter.
        """

        def _predicate_to_dnf(root):
            if root.token_type == NodeType.AND:
                left = _predicate_to_dnf(root.left)
                right = _predicate_to_dnf(root.right)
                if not isinstance(left, list):
                    left = [left]
                if not isinstance(right, list):
                    right = [right]
                left.extend(right)
                return left
            if root.token_type != NodeType.COMPARISON_OPERATOR:
                raise NotSupportedError()
            if not root.value in PUSHABLE_OPERATORS:
                # not all operators are universally supported
                raise NotSupportedError()
            if root.left.token_type != NodeType.IDENTIFIER:
                raise NotSupportedError()
            if root.left.token_type in (
                NodeType.LITERAL_NUMERIC,
                NodeType.LITERAL_VARCHAR,
            ):
                # not all operands are universally supported
                raise NotSupportedError()
            return (root.left.value, PUSHABLE_OPERATORS[root.value], root.right.value)

        try:
            dnf = _predicate_to_dnf(root)
            if not isinstance(dnf, list):
                dnf = [dnf]
        except NotSupportedError:
            return None
        return dnf

    _predicates = []

    def push_predicate(self, predicate):
        """
        Push the predicate to the set - this creates a set of ANDs
        """
        if not self.can_push_selection:
            return False
        dnfed = PredicatePushable.to_dnf(predicate)
        if dnfed is not None:
            # we can't push all predicates everywhere
            return False
        self._predicates.append(dnfed)
        return True
