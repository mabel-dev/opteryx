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
Blob Reader Node

This is a SQL Query Execution Plan Node.

This Node creates datasets based on function calls like VALUES and UNNEST.
"""
import time
from typing import Generator

import pyarrow

from opteryx.exceptions import SqlError
from opteryx.managers.expression import NodeType
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType
from opteryx.utils import series


def _generate_series(**kwargs):
    value_array = series.generate_series(*kwargs["args"])
    column_name = kwargs["columns"][0].schema_column.identity
    return pyarrow.Table.from_arrays([value_array], [column_name])


def _unnest(**kwargs):
    """unnest converts an list into rows"""
    if kwargs["args"][0].node_type == NodeType.NESTED:
        list_items = [kwargs["args"][0].centre.value]
    else:
        list_items = kwargs["args"][0].value
    column_name = kwargs["columns"][0].schema_column.identity

    return pyarrow.Table.from_arrays([list_items], [column_name])


def _values(**parameters):
    columns = [col.schema_column.identity for col in parameters["columns"]]
    values_array = parameters["values"]
    return [{columns[i]: value.value for i, value in enumerate(values)} for values in values_array]


def _fake_data(**kwargs):
    from orso.faker import generate_fake_data

    rows = kwargs["rows"]
    schema = kwargs["schema"]
    for column in schema.columns:
        column.name = column.identity
    return generate_fake_data(schema, rows)


FUNCTIONS = {
    "FAKE": _fake_data,
    "GENERATE_SERIES": _generate_series,
    "UNNEST": _unnest,
    "VALUES": _values,
}


class FunctionDatasetNode(BasePlanNode):

    operator_type = OperatorType.PRODUCER

    def __init__(self, properties: QueryProperties, **config):
        """
        The Blob Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        super().__init__(properties=properties)
        self.alias = config.get("alias")
        self.function = config["function"]
        self.parameters = config
        self.columns = config.get("columns", [])

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def config(self):  # pragma: no cover
        if self.alias:
            return f"{self.function} => {self.alias}"
        return f"{self.function}"

    @property
    def name(self):  # pragma: no cover
        return "Dataset Constructor"

    @property
    def can_push_selection(self):
        return False

    def execute(self) -> Generator:
        try:
            start_time = time.time_ns()
            data = FUNCTIONS[self.function](**self.parameters)  # type:ignore
            self.statistics.time_evaluate_dataset += time.time_ns() - start_time
        except TypeError as err:  # pragma: no cover
            if str(err).startswith("_unnest() takes 2"):
                raise SqlError(
                    "UNNEST expects a literal list in paranthesis, or a field name as a parameter."
                )
            raise err

        if isinstance(data, list):
            table = pyarrow.Table.from_pylist(data)
        elif hasattr(data, "arrow"):
            table = data.arrow()
        else:
            table = data

        self.statistics.rows_read += table.num_rows
        self.statistics.columns_read += len(table.column_names)

        yield table
