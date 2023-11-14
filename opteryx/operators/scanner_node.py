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

This is the SQL Query Execution Plan Node responsible for the reading of data.

It wraps different internal readers (e.g. GCP Blob reader, SQL Reader), 
normalizes the data into the format for internal processing. 
"""
import time
from typing import Iterable

from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode


def normalize_morsel(schema, morsel):
    # rename columns for internal use
    target_column_names = []
    # columns in the data but not in the schema, droppable
    droppable_columns = []

    for i, column in enumerate(morsel.column_names):
        column_name = schema.find_column(column)
        if column_name is None:
            droppable_columns.append(i)
        target_column_names.append(str(column_name))

    for droppable in droppable_columns:
        morsel = morsel.remove_column(droppable)

    morsel = morsel.rename_columns(target_column_names)
    return morsel


class ScannerNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)
        self.start_date = parameters.get("start_date")
        self.end_date = parameters.get("end_date")
        self.hints = parameters.get("hints", [])
        self.columns = parameters.get("columns", [])

        if len(self.hints) != 0:
            self.statistics.add_message("All HINTS are currently ignored")

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
        morsel = None
        schema = self.parameters["schema"]
        start_clock = time.monotonic_ns()
        reader = self.parameters["connector"].read_dataset(columns=self.columns)
        for morsel in reader:
            self.statistics.blobs_read += 1
            self.statistics.rows_read += morsel.num_rows
            self.statistics.bytes_processed += morsel.nbytes
            self.statistics.time_reading_blobs += time.monotonic_ns() - start_clock
            yield normalize_morsel(schema, morsel)
            start_clock = time.monotonic_ns()
        if morsel:
            self.statistics.columns_read += morsel.num_columns
