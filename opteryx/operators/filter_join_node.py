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
Filter Join Node

This is a SQL Query Execution Plan Node.

This module contains implementations for LEFT SEMI and LEFT ANTI joins.
These joins are used to filter rows from the left table based on the
presence or absence of matching rows in the right table.
"""

from typing import List
from typing import Set

import pyarrow

from opteryx import EOS
from opteryx.models import QueryProperties

from . import JoinNode


def left_anti_join(left_relation, left_columns: List[str], right_hash_set: Set[str]):
    """
    Perform a LEFT ANTI JOIN.

    This implementation ensures that all rows from the left table are included in the result set,
    where there are no matching rows in the right table based on the join columns.

    Parameters:
        left_relation (pyarrow.Table): The left pyarrow.Table to join.
        left_columns (list of str): Column names from the left table to join on.
        right_hash_set (set of tuple): A set of tuples representing the hashed values of the right table's join columns.

    Returns:
        A pyarrow.Table containing the result of the LEFT ANTI JOIN operation.
    """

    left_indexes = []
    left_values = left_relation.select(left_columns).drop_null().itercolumns()
    for i, value_tuple in enumerate(map(hash, zip(*left_values))):
        if (
            value_tuple not in right_hash_set
        ):  # Only include left rows that have no match in the right table
            left_indexes.append(i)

    # Filter the left_chunk based on the anti join condition
    if left_indexes:
        return left_relation.take(left_indexes)
    else:
        return left_relation.slice(0, 0)


def left_semi_join(left_relation, left_columns: List[str], right_hash_set: Set[str]):
    """
    Perform a LEFT SEMI JOIN.

    This implementation ensures that all rows from the left table that have a matching row in the right table
    based on the join columns are included in the result set.

    Parameters:
        left_relation (pyarrow.Table): The left pyarrow.Table to join.
        left_columns (list of str): Column names from the left table to join on.
        right_hash_set (set of tuple): A set of tuples representing the hashed values of the right table's join columns.

    Returns:
        A pyarrow.Table containing the result of the LEFT ANTI JOIN operation.
    """
    left_indexes = []
    left_values = left_relation.select(left_columns).drop_null().itercolumns()
    for i, value_tuple in enumerate(map(hash, zip(*left_values))):
        if (
            value_tuple in right_hash_set
        ):  # Only include left rows that have a match in the right table
            left_indexes.append(i)

    # Filter the left_chunk based on the semi join condition
    if left_indexes:
        return left_relation.take(left_indexes)
    else:
        return left_relation.slice(0, 0)


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

        self.right_buffer = []
        self.right_hash_set = set()

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return self.join_type

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
                    left_relation=morsel,
                    left_columns=self.left_columns,
                    right_hash_set=self.right_hash_set,
                )
        if join_leg == "right":
            if morsel == EOS:
                right_relation = pyarrow.concat_tables(self.right_buffer, promote_options="none")
                self.right_buffer.clear()
                non_null_right_values = right_relation.select(self.right_columns).drop_null().itercolumns()
                self.right_hash_set = set(map(hash, zip(*non_null_right_values)))
            else:
                self.right_buffer.append(morsel)
                yield None


providers = {
    "left anti": left_anti_join,
    "left semi": left_semi_join,
}
