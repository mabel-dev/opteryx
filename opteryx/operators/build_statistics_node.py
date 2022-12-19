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
UNIX_EPOCH = datetime.datetime(1970, 1, 1)


def _to_unix_epoch(date):
    if date.as_py() is None:
        return numpy.nan
    # Not all platforms can handle negative timestamp()
    # https://bugs.python.org/issue29097
    return (date.as_py() - UNIX_EPOCH).total_seconds()


def increment(dic: dict, value):
    if value in dic:
        dic[value] += 1
    else:
        dic[value] = 1


def _statitics_collector(pages):
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
            "bytes": 0,
            "most_frequent_values": None,
            "most_frequent_counts": None,
            "numeric_range": None,
            "varchar_range": None,
            "distogram_values": None,
            "distogram_counts": None,
        }
    )
    target_metadata = None

    for page in pages:

        uncollected_columns = []
        profile_collector: dict = {}

        columns = Columns(page)
        table_path = columns.table_path

        for block in page.to_batches(10000):

            for column in page.column_names:

                column_data = block.column(column)

                profile = profile_collector.get(column, orjson.loads(empty_profile))
                _type = determine_type(str(column_data.type))
                if _type not in profile["type"]:
                    profile["type"].append(_type)

                profile["count"] += len(column_data)
                profile["bytes"] += column_data.nbytes
                profile["missing"] += column_data.null_count

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
                if _type == OPTERYX_TYPES.VARCHAR:

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
                    if len(column_data) > 0:
                        varchar_range_min = min(column_data)
                        varchar_range_max = max(column_data)

                        if profile["varchar_range"] is not None:
                            varchar_range_min = min(
                                varchar_range_min, profile["varchar_range"][0]
                            )
                            varchar_range_max = max(
                                varchar_range_max, profile["varchar_range"][1]
                            )

                        profile["varchar_range"] = (
                            varchar_range_min,
                            varchar_range_max,
                        )

                # convert TIMESTAMP into a NUMERIC (seconds after Unix Epoch)
                if _type == OPTERYX_TYPES.TIMESTAMP:
                    column_data = (_to_unix_epoch(i) for i in column_data)
                elif _type != OPTERYX_TYPES.VARCHAR:
                    column_data = (i.as_py() for i in column_data)
                # remove empty values
                column_data = numpy.array(
                    [i for i in column_data if i not in (None, numpy.nan)]
                )

                if _type in (OPTERYX_TYPES.BOOLEAN):
                    # we can make it easier to collect booleans
                    counter = profile.get("counter")
                    if counter is None:
                        counter = {"True": 0, "False": 0}
                    trues = sum(column_data)
                    counter["True"] += trues
                    counter["False"] += column_data.size - trues
                    profile["counter"] = counter

                if _type == OPTERYX_TYPES.VARCHAR and profile.get("counter") != {}:
                    # counter is used to collect and count unique values
                    vals, counts = numpy.unique(column_data, return_counts=True)
                    counter = {}
                    if len(vals) <= MAX_COLLECTOR:
                        counter = dict(zip(vals, counts))
                        for k, v in profile.get("counter", {}).items():
                            counter[k] = counter.pop(k, 0) + v
                        if len(counter) > MAX_COLLECTOR:
                            counter = {}
                    profile["counter"] = counter

                if _type in (OPTERYX_TYPES.NUMERIC, OPTERYX_TYPES.TIMESTAMP):
                    # populate the distogram, this is used for distribution statistics
                    dgram = profile.get("dgram")
                    if dgram is None:
                        dgram = distogram.Distogram()  # type:ignore
                    dgram.bulkload(column_data)
                    profile["dgram"] = dgram

                profile_collector[column] = profile

        buffer = []

        for column, profile in profile_collector.items():
            profile["name"] = columns.get_preferred_name(column)
            profile["type"] = ", ".join(profile["type"])

            if column not in uncollected_columns:

                dgram = profile.pop("dgram", None)
                if dgram:
                    # force numeric types to be the same
                    profile["numeric_range"] = (
                        numpy.double(dgram.min),
                        numpy.double(dgram.max),
                    )
                    profile["distogram_values"], profile["distogram_counts"] = zip(
                        *dgram.bins
                    )
                    profile["distogram_values"] = numpy.array(
                        profile["distogram_values"], numpy.double
                    )

                counter = profile.pop("counter", None)
                if counter:
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

        table = pyarrow.Table.from_pylist(buffer)

        if target_metadata is None:
            table = Columns.create_table_metadata(
                table=table,
                expected_rows=len(buffer),
                name="statistics",
                table_aliases=[],
                disposition="statistics",
                path=table_path,
            )
            target_metadata = Columns(table)
        else:
            table = target_metadata.apply(table, path=table_path)
        yield table


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

        return _statitics_collector(data_pages.execute())
