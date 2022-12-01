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
Build Statistics Node

This is a SQL Query Execution Plan Node.

Gives information about a dataset's columns
"""
import datetime

from functools import reduce
from typing import Iterable

import numpy
import orjson
import pyarrow

from opteryx.attribute_types import OPTERYX_TYPES, determine_type
from opteryx.exceptions import SqlError
from opteryx.models import Columns, QueryProperties
from opteryx.operators import BasePlanNode

MAX_COLLECTOR: int = 8
MAX_VARCHAR_SIZE: int = 64  # long strings tend to lose meaning
MAX_DATA_SIZE: int = 100 * 1024 * 1024


def _to_linux_epoch(date):
    if date.as_py() is None:
        return numpy.nan
    return datetime.datetime.fromisoformat(date.as_py().isoformat()).timestamp()


def increment(dic: dict, value):
    if value in dic:
        dic[value] += 1
    else:
        dic[value] = 1


def _extended_collector(pages):
    """
    Collect summary statistics about each column
    """
    from opteryx.third_party import distogram

    empty_profile = orjson.dumps(
        {
            "name": None,
            "type": [],
            "count": 0,
            "missing": 0,
            "most_frequent_values": None,
            "most_frequent_counts": None,
            "numeric_range": None,
            "varchar_range": None,
            "distogram_values": None,
            "distogram_counts": None,
        }
    )

    uncollected_columns = []

    columns = None
    profile_collector = {}

    for page in pages:

        if columns is None:
            columns = Columns(page)

        for block in page.to_batches(10000):

            for column in page.column_names:

                column_data = block.column(column)

                profile = profile_collector.get(column, orjson.loads(empty_profile))
                _type = determine_type(str(column_data.type))
                if _type not in profile["type"]:
                    profile["type"].append(_type)

                profile["count"] += len(column_data)

                # calculate the missing count more robustly
                missing = reduce(
                    lambda x, y: x + 1,
                    (
                        i
                        for i in column_data
                        if i in (None, numpy.nan) or not i.is_valid
                    ),
                    0,
                )
                profile["missing"] += missing

                # interim save
                profile_collector[column] = profile

                # don't collect problematic columns
                if column in uncollected_columns:
                    continue

                # to prevent problems, we set some limits
                if column_data.nbytes > MAX_DATA_SIZE:  # pragma: no cover
                    if column not in uncollected_columns:
                        uncollected_columns.append(column)
                    continue

                # don't collect columns we can't analyse
                if _type in (
                    OPTERYX_TYPES.LIST,
                    OPTERYX_TYPES.STRUCT,
                    OPTERYX_TYPES.OTHER,
                ):
                    continue

                # long strings are meaningless
                if _type in (OPTERYX_TYPES.VARCHAR):

                    column_data = [v.as_py() for v in column_data if v.is_valid]

                    max_len = reduce(
                        lambda x, y: max(len(y), x),
                        column_data,
                        0,
                    )
                    if max_len > MAX_VARCHAR_SIZE:
                        if column not in uncollected_columns:
                            uncollected_columns.append(column)
                        continue

                    # collect the range values
                    varchar_range_min = min(column_data)
                    varchar_range_max = max(column_data)

                    if profile["varchar_range"] is not None:
                        varchar_range_min = min(
                            varchar_range_min, profile["varchar_range"][0]
                        )
                        varchar_range_max = max(
                            varchar_range_max, profile["varchar_range"][1]
                        )

                    profile["varchar_range"] = (varchar_range_min, varchar_range_max)

                # convert TIMESTAMP into a NUMERIC (seconds after Linux Epoch)
                if _type == OPTERYX_TYPES.TIMESTAMP:
                    column_data = (_to_linux_epoch(i) for i in column_data)
                elif _type != OPTERYX_TYPES.VARCHAR:
                    column_data = (i.as_py() for i in column_data)

                # remove empty values
                column_data = numpy.array(
                    [i for i in column_data if i not in (None, numpy.nan)]
                )

                if _type in (
                    OPTERYX_TYPES.BOOLEAN,
                    OPTERYX_TYPES.VARCHAR,
                    OPTERYX_TYPES.NUMERIC,
                    OPTERYX_TYPES.TIMESTAMP,
                ):
                    # counter is used to collect and count unique values
                    counter = profile.get("counter")
                    if counter is None:
                        counter = {}
                    if len(counter) < MAX_COLLECTOR:
                        [
                            increment(counter, value)
                            for value in column_data
                            if len(counter) < MAX_COLLECTOR
                        ]
                    profile["counter"] = counter

                if _type in (OPTERYX_TYPES.NUMERIC, OPTERYX_TYPES.TIMESTAMP):
                    # populate the distogram, this is used for distribution statistics
                    dgram = profile.get("dgram")
                    if dgram is None:
                        dgram = distogram.Distogram()
                    values, counts = numpy.unique(column_data, return_counts=True)
                    for index, value in enumerate(values):
                        dgram = distogram.update(
                            dgram, value=value, count=counts[index]
                        )
                    profile["dgram"] = dgram

                profile_collector[column] = profile

    buffer = []

    for column, profile in profile_collector.items():
        profile["name"] = columns.get_preferred_name(column)
        profile["type"] = ", ".join(profile["type"])

        if column not in uncollected_columns:

            dgram = profile.pop("dgram", None)
            if dgram:
                profile["numeric_range"] = (dgram.min, dgram.max)
                profile["distogram_values"], profile["distogram_counts"] = zip(
                    *dgram.bins
                )
                profile["distogram_values"] = numpy.array(
                    profile["distogram_values"], numpy.double
                )

            counter = profile.pop("counter", None)
            if counter:
                if len(counter) < MAX_COLLECTOR:
                    profile["unique"] = len(counter)
                    counts = list(counter.values())
                    if min(counts) != max(counts):
                        profile["most_frequent_values"] = [
                            str(k) for k in counter.keys()
                        ]
                        profile["most_frequent_counts"] = counts

        # remove collectors
        profile.pop("dgram", None)
        profile.pop("counter", None)

        buffer.append(profile)

    #    import pprint
    #    pprint.pprint(buffer)

    table = pyarrow.Table.from_pylist(buffer)

    table = Columns.create_table_metadata(
        table=table,
        expected_rows=len(buffer),
        name="show_columns",
        table_aliases=[],
    )
    return table


class BuildStatisticsNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)

    @property
    def name(self):  # pragma: no cover
        return "Analyze Table"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Iterable:

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore

        if data_pages is None:
            return None

        yield _extended_collector(data_pages.execute())
