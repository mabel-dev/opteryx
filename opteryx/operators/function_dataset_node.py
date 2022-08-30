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
import os
import sys
import time

from typing import Iterable

import pyarrow

from opteryx.models import Columns, QueryDirectives, QueryStatistics
from opteryx.managers.expression import NodeType, evaluate
from opteryx.operators import BasePlanNode
from opteryx.exceptions import SqlError


def _generate_series(alias, *args):

    from opteryx.utils import intervals, dates

    arg_len = len(args)
    arg_vals = [i.value for i in args]
    first_arg_type = args[0].token_type

    # if the parameters are numbers, generate series is an alias for range
    if first_arg_type == NodeType.LITERAL_NUMERIC:
        if arg_len not in (1, 2, 3):
            raise SqlError("generate_series for numbers takes 1,2 or 3 parameters.")
        return [{alias: i} for i in intervals.generate_range(*arg_vals)]

    # if the params are timestamps, we create time intervals
    if first_arg_type == NodeType.LITERAL_TIMESTAMP:
        if arg_len != 3:
            raise SqlError(
                "generate_series for dates needs start, end, and interval parameters"
            )
        return [{alias: i} for i in dates.date_range(*arg_vals)]

    # if the param is a CIDR, we create network ranges
    if first_arg_type == NodeType.LITERAL_VARCHAR:
        if arg_len not in (1,):
            raise SqlError("generate_series for strings takes 1 CIDR parameter.")

        import ipaddress

        ips = ipaddress.ip_network(arg_vals[0], strict=False)
        return [{alias: str(ip)} for ip in ips]


def _unnest(alias, values):
    """unnest converts an list into rows"""
    list_items = values.value
    if values.token_type == NodeType.NESTED:
        # single item lists are reported as nested
        from opteryx.samples import no_table

        list_items = evaluate(values, no_table())
    return [{alias: row} for row in list_items]


def _values(alias, *values):
    return values


def _fake_data(alias, *args):
    def _inner(rows, columns):
        for row in range(rows):
            record = {
                f"column_{col}": int.from_bytes(os.urandom(2), sys.byteorder)
                for col in range(columns)
            }
            yield record

    rows, columns = args[0].value, args[1].value
    rows = int(rows)
    columns = int(columns)
    return list(_inner(rows, columns))


FUNCTIONS = {
    "fake": _fake_data,
    "generate_series": _generate_series,
    "unnest": _unnest,
    "values": _values,
}


class FunctionDatasetNode(BasePlanNode):
    def __init__(
        self, directives: QueryDirectives, statistics: QueryStatistics, **config
    ):
        """
        The Blob Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        super().__init__(directives=directives, statistics=statistics)
        self._statistics = statistics
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
            self._statistics.time_data_read += time.time_ns() - start_time
        except TypeError as err:  # pragma: no cover
            print(str(err))
            if str(err).startswith("_unnest() takes 2"):
                raise SqlError(
                    "UNNEST expects a literal list in paranthesis, or a field name as a parameter."
                )
            raise err

        table = pyarrow.Table.from_pylist(data)

        self._statistics.rows_read += table.num_rows
        self._statistics.columns_read += len(table.column_names)

        table = Columns.create_table_metadata(
            table=table,
            expected_rows=table.num_rows,
            name=self._function,
            table_aliases=[self._alias],
        )
        yield table
