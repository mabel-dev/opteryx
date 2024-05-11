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
Read Node

This is the SQL Query Execution Plan Node responsible for the reading of data.

It wraps different internal readers (e.g. GCP Blob reader, SQL Reader), 
normalizes the data into the format for internal processing. 
"""
import time
from typing import Generator

import pyarrow
from orso.schema import RelationSchema

from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


def normalize_morsel(schema: RelationSchema, morsel: pyarrow.Table) -> pyarrow.Table:
    if len(schema.columns) == 0:
        one_column = pyarrow.array([1] * morsel.num_rows, type=pyarrow.int8())
        morsel = morsel.append_column("*", one_column)
        return morsel.select(["*"])

    # rename columns for internal use
    target_column_names = []
    # columns in the data but not in the schema, droppable
    droppable_columns = []

    # find which columns to drop and which columns we already have
    for i, column in enumerate(morsel.column_names):
        column_name = schema.find_column(column)
        if column_name is None:
            droppable_columns.append(i)
        else:
            target_column_names.append(str(column_name))

    # remove from the end otherwise we'll remove the wrong columns after the first one
    droppable_columns.reverse()
    for droppable in droppable_columns:
        morsel = morsel.remove_column(droppable)

    # remane columns to the internal names (identities)
    morsel = morsel.rename_columns(target_column_names)

    # add columns we don't have
    for column in schema.columns:
        if column.identity not in target_column_names:
            null_column = pyarrow.array([None] * morsel.num_rows)
            morsel = morsel.append_column(column.identity, null_column)

    # ensure the columns are in the right order
    return morsel.select([col.identity for col in schema.columns])


class ReaderNode(BasePlanNode):

    operator_type = OperatorType.PRODUCER

    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)
        self.start_date = parameters.get("start_date")
        self.end_date = parameters.get("end_date")
        self.hints = parameters.get("hints", [])
        self.columns = parameters.get("columns", [])
        self.predicates = parameters.get("predicates", [])

        self.connector = parameters.get("connector")
        self.schema = parameters.get("schema")

        if len(self.hints) != 0:
            self.statistics.add_message("All HINTS are currently ignored")

    def to_dict(self) -> dict:
        return {
            "identity": f"read-{self.identity}",
            "opterator": "ReadNode",
            "schema": self.columns,
            "projection": self.columns,
            "filters": self.predicates,
        }

    @classmethod
    def from_dict(cls, dic: dict) -> "BasePlanNode":
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        """friendly name for this step"""
        return "Read"

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

    def execute(self) -> Generator:
        """Perform this step, time how long is spent doing work"""
        morsel = None
        orso_schema = self.schema
        orso_schema_cols = []
        for col in orso_schema.columns:
            if col.identity in [c.identity for c in self.columns]:
                orso_schema_cols.append(col)
        orso_schema.columns = orso_schema_cols
        arrow_schema = None
        start_clock = time.monotonic_ns()
        reader = self.connector.read_dataset(columns=self.columns, predicates=self.predicates)
        for morsel in reader:
            morsel = normalize_morsel(orso_schema, morsel)
            if arrow_schema:
                morsel = morsel.cast(arrow_schema)
            else:
                arrow_schema = morsel.schema
            self.statistics.time_reading_blobs += time.monotonic_ns() - start_clock
            self.statistics.blobs_read += 1
            self.statistics.rows_read += morsel.num_rows
            self.statistics.bytes_processed += morsel.nbytes
            yield morsel
            start_clock = time.monotonic_ns()
        if morsel:
            self.statistics.columns_read += morsel.num_columns
