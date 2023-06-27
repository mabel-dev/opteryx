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
Explain Node

This is a SQL Query Execution Plan Node.


"""
import time
from typing import Iterable

from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode


def normalize_table(schema, table):
    return table
    normalized_names = []

    for column_in_file in table.column_names:
        logical_column = schema.find_column(column_in_file)
        if logical_column is None:
            # add it as a null column
            pass
        else:
            normalized_names.append(logical_column.identity)
    return table.rename_columns(physical_names)


class ScannerNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)

    @property
    def name(self):  # pragma: no cover
        """friendly name for this step"""
        return "Scan"

    @property  # pragma: no cover
    def config(self):
        """Additional details for this step"""
        date_range = ""
        if self.parameters.get("start_date") == self.parameters.get("end_date"):
            if self.parameters.get("start_date") is not None:
                date_range = f" FOR '{self.parameters.get('start_date')}'"
        else:
            date_range = (
                f" FOR '{self.parameters.get('start_date')}' TO '{self.parameters.get('end_date')}'"
            )
        return (
            f"({self.parameters.get('relation')}"
            f"{' AS ' + self.parameters.get('alias') if self.parameters.get('alias') else ''}"
            f"{date_range}"
            f"{' WITH(' + ','.join(self.parameters.get('hints')) + ')' if self.parameters.get('hints') else ''})"
        )

    def execute(self) -> Iterable:
        """Perform this step, time how long is spent doing work"""
        schema = self.parameters["schema"]
        start_clock = time.monotonic_ns()
        reader = self.parameters.get("connector").read_dataset(self.parameters.get("relation"))
        for morsel in reader:
            self.execution_time += time.monotonic_ns() - start_clock
            yield normalize_table(schema, morsel)
            start_clock = time.monotonic_ns()
