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
from typing import Iterable

from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode


class ExitNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.columns = config.get("columns", [])

    @property
    def config(self):  # pragma: no cover
        return None

    @property
    def name(self):  # pragma: no cover
        return "Exit"

    def execute(self) -> Iterable:
        start = time.monotonic_ns()
        morsels = self._producers[0]  # type:ignore

        final_columns = []
        final_names = []
        for column in self.columns:
            final_columns.append(column.schema_column.identity)
            final_names.append(column.current_name)

        if len(final_columns) != len(set(final_columns)):
            from collections import Counter

            duplicates = [column for column, count in Counter(final_columns).items() if count > 1]
            matches = {a for a, b in zip(final_names, final_columns) if b in duplicates}
            raise AmbiguousIdentifierError(
                message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(matches)}`"
            )

        self.statistics.time_exiting += time.monotonic_ns() - start
        for morsel in morsels.execute():
            start = time.monotonic_ns()
            if not set(final_columns).issubset(morsel.column_names):
                mapping = {int_name: name for name, int_name in zip(final_columns, final_names)}
                missing_references = [
                    f"{mapping.get(ref)} {ref}"
                    for ref in final_columns
                    if ref not in morsel.column_names
                ]

                raise InvalidInternalStateError(f"Problem - {missing_references} not in results")

            morsel = morsel.select(final_columns)
            morsel = morsel.rename_columns(final_names)

            self.statistics.time_exiting += time.monotonic_ns() - start
            yield morsel
            start = time.monotonic_ns()
