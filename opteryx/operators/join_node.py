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
Join Node

We have our own implementations of INNER and OUTER joins, this uses PyArrow
to implement less-common joins of ANTI and SEMI joins.
"""

from typing import Generator

import pyarrow

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


class JoinNode(BasePlanNode):
    operator_type = OperatorType.PASSTHRU

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self._join_type = config["type"]
        self._on = config.get("on")
        self._using = config.get("using")

        self._left_columns = config.get("left_columns")
        self._left_relation = config.get("left_relation_names")

        self._right_columns = config.get("right_columns")
        self._right_relation = config.get("right_relation_names")

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return f"{self._join_type} Join"

    @property
    def config(self):  # pragma: no cover
        from opteryx.managers.expression import format_expression

        if self._on:
            return f"{self._join_type.upper()} JOIN ({format_expression(self._on, True)})"
        if self._using:
            return f"{self._join_type.upper()} JOIN (USING {','.join(map(format_expression, self._using))})"
        return f"{self._join_type.upper()}"

    def execute(self) -> Generator:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        left_table = pyarrow.concat_tables(left_node.execute(), promote_options="none")
        right_table = pyarrow.concat_tables(right_node.execute(), promote_options="none")

        try:
            new_morsel = left_table.join(
                right_table,
                keys=self._left_columns,
                right_keys=self._right_columns,
                join_type=self._join_type,
                coalesce_keys=self._using is not None,
            )
        except pyarrow.ArrowInvalid as err:  # pragma: no cover
            last_token = str(err).split(" ")[-1]
            column = None
            for col in left_node.columns:
                if last_token == col.identity:
                    column = col.name
                    break
            for col in right_node.columns:
                if last_token == col.identity:
                    column = col.name
                    break
            if column:
                raise UnsupportedSyntaxError(
                    f"Unable to ANTI/SEMI JOIN with unsupported column types in table, '{column}'."
                ) from err
            raise UnsupportedSyntaxError(
                "Unable to ANTI/SEMI JOIN with unsupported column types in table."
            ) from err

        yield new_morsel
