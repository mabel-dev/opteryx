"""
Selection Node

This is a SQL Query Execution Plan Node.

This Node eliminates elminiates records which do not match a predicate using a
DNF (Disjunctive Normal Form) interpretter. 

Predicates in the same list are joined with an AND/Intersection (all must be True)
and predicates in adjacent lists are joined with an OR/Union (any can be True).
This allows for non-trivial filters to be written with just tuples and lists.

The predicates are in _tuples_ in the form (`key`, `op`, `value`) where the `key`
is the value looked up from the record, the `op` is the operator and the `value`
is a literal.
"""
import numpy
from typing import Union, Iterable
from pyarrow import Table
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
from opteryx.third_party.pyarrow_ops import ifilters


class InvalidSyntaxError(Exception):
    pass


def _evaluate(predicate: Union[tuple, list], table: Table) -> bool:

    # If we have a tuple extract out the key, operator and value and do the evaluation
    if isinstance(predicate, tuple):
        # filters from pyarrow_ops only filters on a single predicate
        return ifilters(table, predicate)

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
                    mask = numpy.intersect1d(mask, _evaluate(p, table))
            return mask

        # Are all of the entries lists?
        # We OR them together
        if all([isinstance(p, list) for p in predicate]):
            for p in predicate:
                if mask is None:
                    # The first time round we either set the mask to the first set of
                    # values, or we initialize the mask beforehand to all empty.
                    mask = _evaluate(p, table)
                else:
                    mask = numpy.union1d(mask, _evaluate(p, table))
            return mask

        # if we're here the structure of the filter is wrong
        raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover

    raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover


class SelectionNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._filter = config.get("filter")

    def __repr__(self):
        return str(self._filter)

    def execute(self, data_pages: Iterable) -> Iterable:

        if self._filter is None:
            yield from data_pages

        else:
            for page in data_pages:
                mask = _evaluate(self._filter, page)
                yield page.take(mask)
