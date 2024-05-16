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
Exit Node

This is a SQL Query Execution Plan Node.

This does the final preparation before returning results to users.

This does two things that the projection node doesn't do:
    - renames columns from the internal names
    - removes all columns not being returned to the user

This node doesn't do any calculations, it is a pure Projection.
"""
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Generator
from typing import List

from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import LogicalColumn
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType
from opteryx.operators.base_plan_node import BasePlanDataObject


@dataclass
class ExitDataObject(BasePlanDataObject):
    columns: List[LogicalColumn] = field(default_factory=list)


class ExitNode(BasePlanNode):

    operator_type = OperatorType.PASSTHRU

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.columns = config.get("columns", [])

        self.do = ExitDataObject(columns=self.columns)

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def config(self):  # pragma: no cover
        return None

    @property
    def name(self):  # pragma: no cover
        return "Exit"

    def execute(self) -> Generator:
        start = time.monotonic_ns()
        morsels = self._producers[0]  # type:ignore

        final_columns = []
        final_names = []
        for column in self.columns:
            final_columns.append(column.schema_column.identity)
            final_names.append(column.current_name)

        if len(final_columns) != len(set(final_columns)):  # pragma: no cover
            from collections import Counter

            duplicates = [column for column, count in Counter(final_columns).items() if count > 1]
            matches = {a for a, b in zip(final_names, final_columns) if b in duplicates}
            raise AmbiguousIdentifierError(
                message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(matches)}`"
            )

        if len(set(final_names)) != len(final_names):  # we have duplicate names
            final_names = []
            for column in self.columns:
                if column.schema_column.origin:
                    final_names.append(f"{column.schema_column.origin[0]}.{column.current_name}")
                else:
                    final_names.append(column.qualified_name)

        self.statistics.time_exiting += time.monotonic_ns() - start
        for morsel in morsels.execute():
            start = time.monotonic_ns()
            if not set(final_columns).issubset(morsel.column_names):  # pragma: no cover
                mapping = {name: int_name for name, int_name in zip(final_columns, final_names)}
                missing_references = {
                    mapping.get(ref): ref for ref in final_columns if ref not in morsel.column_names
                }

                raise InvalidInternalStateError(
                    f"The following fields were not in the resultset - {', '.join(missing_references.keys())}"
                )

            morsel = morsel.select(final_columns)
            morsel = morsel.rename_columns(final_names)

            self.statistics.time_exiting += time.monotonic_ns() - start
            yield morsel
            start = time.monotonic_ns()
