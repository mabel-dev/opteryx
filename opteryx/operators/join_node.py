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

This is a SQL Query Execution Plan Node.

This handles most of the join types as a wrapper for pyarrow's JOIN functions, 
except for INNER JOIN and CROSS JOIN. PyArrow has a very good set of JOIN
implementations which use here, we don't use the INNER JOIN because PyArrow
has limitations on the column types allowed in the relations, so we use our
own INNER JOIN algo.
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
        return ""

    def execute(self) -> Generator:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        right_table = pyarrow.concat_tables(right_node.execute(), promote_options="none")

        for morsel in left_node.execute():
            # in place until #1295 resolved
            if not self._right_columns[0] in morsel.column_names:
                self._right_columns, self._left_columns = self._left_columns, self._right_columns

            try:
                # do the join
                new_morsel = morsel.join(
                    right_table,
                    keys=self._right_columns,
                    right_keys=self._left_columns,
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
                        f"Unable to JOIN with unsupported column types in table, '{column}'."
                    ) from err
                raise UnsupportedSyntaxError(
                    "Unable to JOIN with unsupported column types in table."
                ) from err

            yield new_morsel
