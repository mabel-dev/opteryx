# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is both a marker and a wrapper for key functionality to support predicate/filter
pushdowns. This is where we a sending filters to the thing that is acquiring the data
for the query. For example sending filters to remote database servers, or to pyarrow
readers. This allows for data to be prefiltered before reaching Opteryx - this is
almost always going to be faster than reading, loading and filtering.

Note that for some file types, although we accept the pushdown, we fake it by reading,
loading and filtering. We do this because we have a single file interface and some
accept filters and others don't so we 'fake' the read-time filtering.
"""

import datetime
from typing import Dict

from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.exceptions import NotSupportedError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node


class PredicatePushable:
    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": False,
        "NotEq": False,
        "Gt": False,
        "GtEq": False,
        "Lt": False,
        "LtEq": False,
        "Like": False,
        "NotLike": False,
    }

    OPS_XLAT: Dict[str, str] = {
        "Eq": "=",
        "NotEq": "!=",
        "Gt": ">",
        "GtEq": ">=",
        "Lt": "<",
        "LtEq": "<=",
        "Like": "LIKE",
        "NotLike": "NOT LIKE",
    }

    PUSHABLE_TYPES: set = {t for t in OrsoTypes}

    def can_push(self, operator: Node, types: set = None) -> bool:
        # we can only push simple expressions
        all_nodes = get_all_nodes_of_type(operator.condition, ("*",))
        if any(
            n.node_type not in (NodeType.IDENTIFIER, NodeType.LITERAL, NodeType.COMPARISON_OPERATOR)
            for n in all_nodes
        ):
            return False
        # we can only push certain types
        if types and not types.issubset(self.PUSHABLE_TYPES):
            return False
        # we can only push certain operators
        return self.PUSHABLE_OPS.get(operator.condition.value, False)

    def __init__(self, **kwargs):
        pass

    @staticmethod
    @single_item_cache
    def to_dnf(root):
        """
        Convert a filter to DNF form, this is the form used by PyArrow.

        This is specifically opinionated for the Parquet reader for PyArrow.
        """

        def _predicate_to_dnf(root):
            # Reduce look-ahead effort by using Exceptions to control flow
            if root.node_type == NodeType.AND:  # pragma: no cover
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
                root.left, root.right = root.right, root.left
            if root.right.schema_column.type == OrsoTypes.DATE:
                date_val = root.right.value
                if hasattr(date_val, "item"):
                    date_val = date_val.item()
                root.right.value = datetime.datetime.combine(date_val, datetime.time.min)
                root.right.schema_column.type = OrsoTypes.TIMESTAMP
            if root.left.node_type != NodeType.IDENTIFIER:
                raise NotSupportedError()
            if root.right.node_type != NodeType.LITERAL:
                raise NotSupportedError()
            if root.left.schema_column.type == OrsoTypes.VARCHAR:
                root.left.schema_column.type = OrsoTypes.BLOB
            if root.right.schema_column.type == OrsoTypes.VARCHAR:
                root.right.schema_column.type = OrsoTypes.BLOB
            if root.right.schema_column.type != root.left.schema_column.type:
                raise NotSupportedError()
            return (
                root.left.value,
                PredicatePushable.OPS_XLAT[root.value],
                root.right.value,
            )

        not_converted = []
        dnf = []
        if not isinstance(root, list):
            root = [root]
        for predicate in root:
            try:
                converted = _predicate_to_dnf(predicate)
                dnf.append(converted)
            except NotSupportedError:
                not_converted.append(predicate)
        return dnf if dnf else None, not_converted
