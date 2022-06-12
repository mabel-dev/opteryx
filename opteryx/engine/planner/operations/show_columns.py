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
Show Columns Node

This is a SQL Query Execution Plan Node.

Gives information about a dataset's columns
"""
from functools import reduce
from typing import Iterable

import numpy
import pyarrow

from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.attribute_types import OPTERYX_TYPES, determine_type
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
from opteryx.exceptions import SqlError
from opteryx.utils.columns import Columns


def myhash(any):
    from cityhash import CityHash64

    if isinstance(any, list):
        hashed = map(myhash, any)
        return reduce(lambda x, y: x ^ y, hashed, 0)
    if isinstance(any, dict):
        return CityHash64("".join([f"{k}:{any[k]}" for k in sorted(any.keys())]))
    return CityHash64(str(any))


class ShowColumnsNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._full = (config.get("full"),)
        self._extended = config.get("extended")
        pass

    @property
    def name(self):  # pragma: no cover
        return "Show Columns"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Iterable:

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore

        if data_pages is None:
            return None

        if self._full:
            dataset = pyarrow.concat_tables(data_pages.execute())
        else:
            dataset = next(data_pages.execute())

        source_metadata = Columns(dataset)

        buffer = []
        for column in dataset.column_names:
            column_data = dataset.column(column)
            _type = determine_type(str(column_data.type))
            new_row = {
                "column_name": source_metadata.get_preferred_name(column),
                "type": _type,
                "nulls": (column_data.null_count) > 0,
            }

            if self._extended:

                new_row = {
                    **new_row,
                    "count": -1,
                    "min": None,
                    "max": None,
                    "mean": None,
                    "quantiles": None,
                    "histogram": None,
                    "unique": -1,
                    "missing": -1,
                    "most_frequent": None
                }

                if _type == OPTERYX_TYPES.TIMESTAMP:
                    import datetime

                    to_linux_epoch = (
                        lambda x: numpy.nan
                        if x.as_py() is None
                        else datetime.datetime.fromisoformat(
                            x.as_py().isoformat()
                        ).timestamp()
                    )
                    #column_data = [to_linux_epoch(i) for i in column_data]
                    

                if not _type in (
                    OPTERYX_TYPES.VARCHAR,
                    OPTERYX_TYPES.LIST,
                    OPTERYX_TYPES.STRUCT,
                    OPTERYX_TYPES.TIMESTAMP,
                ):
                    new_row["min"] = numpy.nanmin(column_data)
                    new_row["max"] = numpy.nanmax(column_data)
                    new_row["mean"] = numpy.nanmean(column_data)
                    new_row["quantiles"] = numpy.nanpercentile(column_data, [25, 50, 75])
                #    new_row["histogram"], _ = numpy.histogram(column_data[~numpy.isnan(column_data)], 10)
                    hashes = column_data
                else:
                    hashes = [myhash(i) for i in column_data]

                new_row["missing"] = reduce(
                    lambda x, y: x + 1,
                    (i for i in column_data if i in (None, numpy.nan)),
                    0,
                )
                new_row["count"] = len(column_data)

                # most common
                values, counts = numpy.unique(hashes, return_counts=True)
                new_row["unique"] = len(values)

    #            if _type not in (OPTERYX_TYPES.LIST, OPTERYX_TYPES.STRUCT) and max(counts) > 1:
    #                print(
    #                    [{values[index]:count} for index, count in enumerate(counts) if count in sorted(counts)[0:3]]
    #                )

            buffer.append(new_row)

        table = pyarrow.Table.from_pylist(buffer)
        table = Columns.create_table_metadata(
            table=table,
            expected_rows=len(buffer),
            name="show_columns",
            table_aliases=[],
        )
        yield table
        return
