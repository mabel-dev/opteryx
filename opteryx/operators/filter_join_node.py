# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Filter Join Node

This is a SQL Query Execution Plan Node.

This module contains implementations for LEFT SEMI and LEFT ANTI joins.
These joins are used to filter rows from the left table based on the
presence or absence of matching rows in the right table.
"""

import pyarrow

from opteryx import EOS
from opteryx.compiled.structures import anti_join
from opteryx.compiled.structures import filter_join_set
from opteryx.compiled.structures import semi_join
from opteryx.models import QueryProperties

from . import JoinNode


class FilterJoinNode(JoinNode):
    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)
        self.join_type = parameters["type"]
        self.on = parameters.get("on")
        self.using = parameters.get("using")

        self.left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers")

        self.right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers")

        self.right_hash_set = None

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return self.join_type.replace(" ", "_")

    @property
    def config(self) -> str:  # pragma: no cover
        from opteryx.managers.expression import format_expression

        if self.on:
            return f"{self.join_type.upper()} JOIN ({format_expression(self.on, True)})"
        if self.using:
            return f"{self.join_type.upper()} JOIN (USING {','.join(map(format_expression, self.using))})"
        return f"{self.join_type.upper()}"

    def execute(self, morsel: pyarrow.Table, join_leg: str) -> pyarrow.Table:
        if join_leg == "left":
            if morsel == EOS:
                yield EOS
            else:
                join_provider = providers.get(self.join_type)
                yield join_provider(
                    relation=morsel,
                    join_columns=self.left_columns,
                    seen_hashes=self.right_hash_set,
                )
        if join_leg == "right" and morsel != EOS:
            self.right_hash_set = filter_join_set(morsel, self.right_columns, self.right_hash_set)
            yield None


providers = {
    "left anti": anti_join,
    "left semi": semi_join,
}
