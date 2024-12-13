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

import pyarrow

from opteryx import EOS
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryProperties

from . import JoinNode


class PyArrowJoinNode(JoinNode):
    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)
        self._join_type = parameters["type"]
        self._on = parameters.get("on")
        self._using = parameters.get("using")

        self._left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers")

        self._right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers")

        self.stream = "left"
        self.left_buffer = []
        self.right_buffer = []
        self.left_relation = None

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

    def execute(self, morsel: pyarrow.Table, join_leg: str) -> pyarrow.Table:
        if self.stream == "left":
            if morsel == EOS:
                self.stream = "right"
                self.left_relation = pyarrow.concat_tables(self.left_buffer, promote_options="none")
                self.left_buffer.clear()

                # in place until #1295 resolved
                if self._left_columns[0] not in self.left_relation.column_names:
                    self._right_columns, self._left_columns = (
                        self._left_columns,
                        self._right_columns,
                    )

            else:
                self.left_buffer.append(morsel)
            yield None
            return

        if morsel == EOS:
            right_relation = pyarrow.concat_tables(self.right_buffer, promote_options="none")
            self.right_buffer.clear()
            # do the join
            try:
                new_morsel = self.left_relation.join(
                    right_relation,
                    keys=self._left_columns,
                    right_keys=self._right_columns,
                    join_type=self._join_type,
                    coalesce_keys=self._using is not None,
                )
            except pyarrow.ArrowInvalid as err:  # pragma: no cover
                last_token = str(err).split(" ")[-1]
                column = None
                for col in self.left_relation.columns:
                    if last_token == col.identity:
                        column = col.name
                        break
                for col in right_relation.columns:
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

        else:
            self.right_buffer.append(morsel)
            yield None
