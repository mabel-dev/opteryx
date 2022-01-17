"""
Selection Node

This is a SQL Query Execution Plan Node.

This Node eliminates elminiates records which do not match a predicate using a
DNF (Disjunctive Normal Form) interpretter. 

Predicates in the same list are joined with an AND (all must be True) and predicates
in adjacent lists are joined with an OR (any can be True). This allows for non-trivial
filters to be written with just tuples and lists.

The predicates are in _tuples_ in the form (`key`, `op`, `value`) where the `key`
is the value looked up from the record, the `op` is the operator and the `value`
is a literal.
"""
from re import T
from typing import Union
from pyarrow import Table
import pyarrow.compute as pc
from opteryx.third_party.uintset import UintSet
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode

# pyArrow Operators: https://arrow.apache.org/docs/python/api/compute.html
NATIVE_OPERATORS = {
    "=": pc.equal,
    ">": pc.greater,
    ">=": pc.greater_equal,
    "<": pc.less,
    "<=": pc.less_equal,
    "<>": pc.not_equal,
    "LIKE": pc.match_like,
    "SIMILAR TO": pc.match_substring_regex,
}

CODED_OPERATORS = {}


class InvalidSyntaxError(Exception):
    pass


def _evaluate(predicate: Union[tuple, list], table: Table) -> bool:

    # If we have a tuple extract out the key, operator and value and do the evaluation
    if isinstance(predicate, tuple):
        key, operator, value = predicate
        if operator in NATIVE_OPERATORS:
            # returns a list of lists of booleans which creates a mask of the rows
            # that match the predicate
            mask = NATIVE_OPERATORS[operator](table[key], value)
            mask = UintSet([i for i, v in enumerate(mask) if v.as_py()])
            return mask
        elif operator in CODED_OPERATORS:
            raise NotImplementedError(f"Operator {operator} has not been implemented.")
        else:
            raise NotImplementedError(f"Operator {operator} has not been implemented.")

    # If we have a list, we're going to recurse and call ourselves with the items in
    # the list
    if isinstance(predicate, list):
        # Are all of the entries tuples?
        # We AND them together
        mask = None
        if all([isinstance(p, tuple) for p in predicate]):
            for p in predicate:
                if mask is None:
                    # The first time round we either set the mask to the first set of
                    # values, or we initialize the mask beforehand to all True.
                    mask = _evaluate(p, table)
                else:
                    mask = mask & _evaluate(p, table)
            return mask

        # Are all of the entries lists?
        # We OR them together
        if all([isinstance(p, list) for p in predicate]):
            mask = UintSet()
            for p in predicate:
                mask = mask | _evaluate(p, table)
            return mask

        # if we're here the structure of the filter is wrong
        raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover

    raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover


class SelectionNode(BasePlanNode):
    def __init__(self, **config):
        self._filter = config.get("filter")

    def __repr__(self):
        return str(self._filter)

    def execute(self, relation: Table) -> Table:

        if self._filter is None:
            return relation

        from opteryx.third_party.pyarrow_ops import filters

        return filters(relation, self._filter)


#        mask = _evaluate(self._filter, relation)
#        return relation.filter([mask[i] for i in range(relation.num_rows)])
