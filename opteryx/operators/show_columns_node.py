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
import datetime

from functools import reduce
from typing import Iterable
from numpy import nan, nanmin, nanmax

import numpy
import orjson
import pyarrow

from opteryx.attribute_types import OPTERYX_TYPES, determine_type
from opteryx.exceptions import SqlError
from opteryx.models import Columns, QueryProperties
from opteryx.operators import BasePlanNode

MAX_COLLECTOR: int = 17
MAX_VARCHAR_SIZE: int = 64  # long strings tend to lose meaning
MAX_DATA_SIZE: int = 100 * 1024 * 1024
UNIX_EPOCH = datetime.datetime(1970, 1, 1)


def _to_unix_epoch(date):
    if date.as_py() is None:
        return numpy.nan
    # Not all platforms can handle negative timestamp()
    # https://bugs.python.org/issue29097
    return (date.as_py() - UNIX_EPOCH).total_seconds()


def _simple_collector(page):
    """
    Collect the very summary type information only, we read only a single page to do
    this so it's pretty quick - helpful if you want to know what fields are available
    programatically.
    """
    columns = Columns(page)

    buffer = []
    for column in page.column_names:
        column_data = page.column(column)
        _type = determine_type(str(column_data.type))
        new_row = {"name": columns.get_preferred_name(column), "type": _type}
        buffer.append(new_row)

    table = pyarrow.Table.from_pylist(buffer)
    table = Columns.create_table_metadata(
        table=table,
        expected_rows=len(buffer),
        name="show_columns",
        table_aliases=[],
        disposition="calculated",
        path="show_columns",
    )
    return table


def _full_collector(pages):
    """
    Collect basic count information about columns, to do this we read the entire
    dataset.
    """

    empty_profile = orjson.dumps(
        {
            "name": None,
            "type": [],
            "count": 0,
            "min": None,
            "max": None,
            "missing": 0,
        }
    )

    columns = None
    profile_collector: dict = {}

    for page in pages:
        if columns is None:
            columns = Columns(page)

        for column in page.column_names:
            column_data = page.column(column)
            profile = profile_collector.get(column, orjson.loads(empty_profile))
            _type = determine_type(str(column_data.type))
            if _type not in profile["type"]:
                profile["type"].append(_type)

            profile["count"] += len(column_data)

            # calculate the missing count more robustly
            missing = reduce(
                lambda x, y: x + 1,
                (i for i in column_data if i in (None, nan) or not i.is_valid),
                0,
            )
            profile["missing"] += missing

            if _type == OPTERYX_TYPES.NUMERIC:
                if profile["min"]:
                    profile["min"] = min(profile["min"], nanmin(column_data))
                    profile["max"] = max(profile["max"], nanmax(column_data))
                else:
                    profile["min"] = nanmin(column_data)
                    profile["max"] = nanmax(column_data)

            profile_collector[column] = profile

    buffer = []

    for column, profile in profile_collector.items():
        profile["name"] = columns.get_preferred_name(column)
        profile["type"] = ", ".join(profile["type"])
        buffer.append(profile)

    table = pyarrow.Table.from_pylist(buffer)
    table = Columns.create_table_metadata(
        table=table,
        expected_rows=len(buffer),
        name="show_columns",
        table_aliases=[],
        disposition="calculated",
        path="show_columns",
    )
    return table


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
    from opteryx.third_party import hyperloglog

    empty_profile = orjson.dumps(
        {
            "name": None,
            "type": [],
            "count": 0,
            "min": None,
            "max": None,
            "missing": 0,
            "mean": None,
            "unique": None,
            "quantiles": None,
            "histogram": None,
            "most_frequent_values": None,
            "most_frequent_counts": None,
        }
    )

    uncollected_columns = []

    columns = None
    profile_collector: dict = {}

    for page in pages:

        if columns is None:
            columns = Columns(page)

        for block in page.to_batches(5000):

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
                    max_len = reduce(
                        lambda x, y: max(len(y), x),
                        (v.as_py() for v in column_data if v.is_valid),
                        0,
                    )
                    if max_len > MAX_VARCHAR_SIZE:
                        if column not in uncollected_columns:
                            uncollected_columns.append(column)
                        continue

                # convert TIMESTAMP into a NUMERIC (seconds after Unix Epoch)
                if _type == OPTERYX_TYPES.TIMESTAMP:
                    column_data = (_to_unix_epoch(i) for i in column_data)
                else:
                    column_data = (i.as_py() for i in column_data)

                # remove empty values
                column_data = numpy.array(
                    [i for i in column_data if i not in (None, numpy.nan)]
                )

                if _type in (
                    OPTERYX_TYPES.VARCHAR,
                    OPTERYX_TYPES.NUMERIC,
                    OPTERYX_TYPES.TIMESTAMP,
                ):
                    # hyperloglog estimates cardinality/uniqueness
                    hll = profile.get("hyperloglog")
                    if hll is None:
                        hll = hyperloglog.HyperLogLogPlusPlus(p=16)
                    for value in column_data:
                        hll.update(value)
                    profile["hyperloglog"] = hll

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
                        for value in column_data:
                            if len(counter) < MAX_COLLECTOR:
                                increment(counter, value)
                    profile["counter"] = counter

                if _type in (OPTERYX_TYPES.NUMERIC, OPTERYX_TYPES.TIMESTAMP):
                    # populate the distogram, this is used for distribution statistics
                    dgram = profile.get("distogram")
                    if dgram is None:
                        dgram = distogram.Distogram()  # type:ignore
                    dgram.bulkload(column_data)
                    profile["distogram"] = dgram

                profile_collector[column] = profile

    buffer = []

    for column, profile in profile_collector.items():
        profile["name"] = columns.get_preferred_name(column)
        profile["type"] = ", ".join(profile["type"])

        if column not in uncollected_columns:

            dgram = profile.pop("distogram", None)
            if dgram:
                profile["min"], profile["max"] = distogram.bounds(dgram)  # type:ignore
                profile["mean"] = distogram.mean(dgram)  # type:ignore

                histogram = distogram.histogram(dgram, bin_count=20)  # type:ignore
                if histogram:
                    profile["histogram"] = histogram[0]

                profile["quantiles"] = (
                    distogram.quantile(dgram, value=0.25),  # type:ignore
                    distogram.quantile(dgram, value=0.5),  # type:ignore
                    distogram.quantile(dgram, value=0.75),  # type:ignore
                )

                if dgram.bin_count < dgram.max_bin_count:
                    profile["unique"] = dgram.bin_count

            # if we can get the exact count from the distrogram, don't approximate
            hll = profile.pop("hyperloglog", None)
            if hll and profile.get("unique") is None:
                profile["unique"] = hll.count()

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
        profile.pop("distogram", None)
        profile.pop("hyperloglog", None)
        profile.pop("counter", None)

        buffer.append(profile)

    table = pyarrow.Table.from_pylist(buffer)
    table = Columns.create_table_metadata(
        table=table,
        expected_rows=len(buffer),
        name="show_columns",
        table_aliases=[],
        disposition="calculated",
        path="show_columns",
    )
    return table


class ShowColumnsNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self._full = config.get("full")
        self._extended = config.get("extended")

    @property
    def name(self):  # pragma: no cover
        return "Show Columns"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Iterable:

        # TODO: [TARCHIA] - use the metastore to get the column statisitcs

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore

        if data_pages is None:
            return None

        if not (self._full or self._extended):
            # if it's not full or extended, do just get the list of columns and their
            # types
            yield _simple_collector(next(data_pages.execute()))
            return

        if self._full and not self._extended:
            # we're going to read the full table, so we can count stuff
            yield _full_collector(data_pages.execute())
            return

        if self._extended:
            # get everything we can reasonable get
            yield _extended_collector(data_pages.execute())
            return
