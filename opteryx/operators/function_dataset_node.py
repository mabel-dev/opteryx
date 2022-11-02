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
import random
import time

from typing import Iterable

import pyarrow

from opteryx.models import Columns, QueryProperties
from opteryx.managers.expression import NodeType, evaluate
from opteryx.operators import BasePlanNode
from opteryx.exceptions import SqlError
from opteryx.utils import arrays


def _generate_series(alias, *args):
    value_array = arrays.generate_series(*args)
    return [{alias: value} for value in value_array]


def _unnest(alias, values):
    """unnest converts an list into rows"""
    list_items = values.value
    if values.token_type == NodeType.NESTED:
        # single item lists are reported as nested
        from opteryx.samples import no_table

        list_items = evaluate(values, no_table(), True)
    return [{alias: row} for row in list_items]


def _values(alias, *values):
    return values


def _fake_data(alias, *args):
    rows, columns = int(args[0].value), int(args[1].value)
    return [
        {f"column_{col}": random.getrandbits(16) for col in range(columns)}
        for row in range(rows)
    ]


FUNCTIONS = {
    "fake": _fake_data,
    "generate_series": _generate_series,
    "unnest": _unnest,
    "values": _values,
}


class FunctionDatasetNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        """
        The Blob Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        super().__init__(properties=properties)
        self._alias = config["alias"]
        self._function = config["dataset"]["function"]
        self._args = config["dataset"]["args"]

    @property
    def config(self):  # pragma: no cover
        if self._alias:
            return f"{self._function} => {self._alias}"
        return f"{self._function}"

    @property
    def name(self):  # pragma: no cover
        return "Dataset Constructor"

    def execute(self) -> Iterable:

        try:
            start_time = time.time_ns()
            data = FUNCTIONS[self._function](self._alias, *self._args)  # type:ignore
            self.statistics.time_data_read += time.time_ns() - start_time
        except TypeError as err:  # pragma: no cover
            if str(err).startswith("_unnest() takes 2"):
                raise SqlError(
                    "UNNEST expects a literal list in paranthesis, or a field name as a parameter."
                )
            raise err

        table = pyarrow.Table.from_pylist(data)

        self.statistics.rows_read += table.num_rows
        self.statistics.columns_read += len(table.column_names)

        table = Columns.create_table_metadata(
            table=table,
            expected_rows=table.num_rows,
            name=self._function,
            table_aliases=[self._alias],
        )
        yield table
