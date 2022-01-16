"""
This is a filtering mechanism to be applied when reading data.

This is row-orientated, the DNF filter in the SelectionNode is column-orientated.
"""
import operator
from typing import Optional, Iterable, List, Tuple, Union
from opteryx.exceptions import InvalidSyntaxError
from ...logging import get_logger
from opteryx.utils.text import like, similar_to


def _in(x, y):
    return x in y


def _nin(x, y):
    return x not in y


def _con(x, y):
    return y in x


def _ncon(x, y):
    return y not in x


def true(x):
    return True


# convert text representation of operators to functions
OPERATORS = {
    "=": operator.eq,
    "==": operator.eq,
    "is": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    ">": operator.gt,
    "<=": operator.le,
    ">=": operator.ge,
    "like": like,
    "similar to": similar_to,
    "in": _in,
    "!in": _nin,
    "not in": _nin,
    "contains": _con,
    "!contains": _ncon,
}


def evaluate(predicate: Union[tuple, list], record: dict) -> bool:
    """
    This is the evaluation routine for the Filter class.

    Implements a DNF (Disjunctive Normal Form) interpretter. Predicates in the same
    list are joined with an AND (*all()*) and predicates in adjacent lists are joined
    with an OR (*any()*). This allows for non-trivial filters to be written with just
    _tuples_ and _lists_.

    The predicates are in _tuples_ in the form (`key`, `op`, `value`) where the `key`
    is the value looked up from the record, the `op` is the operator and the `value`
    is a literal.
    """
    # No filter doesn't filter
    if predicate is None:  # pragma: no cover
        return True

    # If we have a tuple extract out the key, operator and value and do the evaluation
    if isinstance(predicate, tuple):
        key, op, value = predicate
        if key in record:
            return OPERATORS[op.lower()](record[key], value)
        return False

    if isinstance(predicate, list):
        # Are all of the entries tuples?
        # We AND them together (_all_ are True)
        if all([isinstance(p, tuple) for p in predicate]):
            return all([evaluate(p, record) for p in predicate])

        # Are all of the entries lists?
        # We OR them together (_any_ are True)
        if all([isinstance(p, list) for p in predicate]):
            return any([evaluate(p, record) for p in predicate])

        # if we're here the structure of the filter is wrong
        raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover

    raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover


def get_indexable_filter_columns(predicate):
    """
    Returns all of the columns in a filter which the operation benefits
    from an index

    This creates an list of tuples of (field,value) that we can feed to the
    index search.
    """
    SARGABLE_OPS = {"=", "==", "is", "in", "contains"}
    if predicate is None:
        return []
    if isinstance(predicate, tuple):
        key, op, value = predicate
        if op in SARGABLE_OPS:
            return [
                (
                    key,
                    value,
                )
            ]
    if isinstance(predicate, list):
        if all([isinstance(p, tuple) for p in predicate]):
            return [
                (
                    k,
                    v,
                )
                for k, o, v in predicate
                if o in SARGABLE_OPS
            ]
        if all([isinstance(p, list) for p in predicate]):
            columns = []
            for p in predicate:
                columns += get_indexable_filter_columns(p)
            return columns
    return []  # pragma: no cover


class DnfFilters:

    __slots__ = ("empty_filter", "predicates")

    def __init__(self, filters: Optional[List[Tuple[str, str, object]]] = None):
        """
        A class to supporting filtering data.

        Parameters:
            filters: list of tuples
                Each tuple has format: (`key`, `op`, `value`). When run the
                filter will extract the `key` field from the dictionary and
                compare to the `value` using the operator `op`. Multiple
                filters are treated as AND, lists of ANDs are treated as ORs.
                The supported `op` values are: `=` or `==`, `!=`, `<`, `>`,
                `<=`, `>=`, `in`, `!in` (not in), `contains`, `!contains`
                (does not contain) and `like`. If the `op` is `in` or `!in`,
                the `value` must be a collection such as a _list_, a _set_
                or a _tuple_.
                `like` performs similar to the SQL operator, `%` is a
                multi-character wildcard and `_` is a single character
                wildcard.

        Examples:
            filters = Filters([('name', '==', 'john')])
            filters = Filters([('name', '!=', 'john'),('name', '!=', 'tom')])

        """
        if filters:
            self.predicates = filters
            self.empty_filter = False
            # record the filters, will help optimize indicies later
            get_logger().info({"filter_columns": self._get_filter_columns(filters)})
        else:
            self.empty_filter = True

    def _get_filter_columns(self, predicate):
        if predicate is None:
            return []
        if isinstance(predicate, tuple):
            key, op, value = predicate
            return [key]
        if isinstance(predicate, list):
            if all([isinstance(p, tuple) for p in predicate]):
                return [k for k, o, v in predicate]
            if all([isinstance(p, list) for p in predicate]):
                columns = []
                for p in predicate:
                    columns += self._get_filter_columns(p)
                return columns
        return []  # pragma: no cover

    def filter_dictset(self, dictset: Iterable[dict]) -> Iterable:
        """
        Tests each entry in a Iterable against the filters

        Parameters:
            dictset: iterable of dictionaries
                The dictset to process

        Yields:
            dictionary
        """
        if self.empty_filter:
            yield from dictset
        else:
            for record in dictset:
                if evaluate(self.predicates, record):
                    yield record

    def __call__(self, record) -> bool:
        return evaluate(self.predicates, record)
