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
import orjson
import pyarrow
from numpy import nan
from numpy import nanmax
from numpy import nanmin

from opteryx.constants.attribute_types import OPTERYX_TYPES
from opteryx.constants.attribute_types import determine_type
from opteryx.exceptions import SqlError
from opteryx.models import Columns
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode

MAX_COLLECTOR: int = 17
MAX_VARCHAR_SIZE: int = 64  # long strings tend to lose meaning
MAX_DATA_SIZE: int = 100 * 1024 * 1024


def _to_unix_epoch(date):
    timestamp = date.value
    if timestamp is None:
        return numpy.nan
    return timestamp / 1e6


def _simple_collector(morsel):
    """
    Collect the very summary type information only, we read only a single morsel to do
    this so it's pretty quick - helpful if you want to know what fields are available
    programatically.
    """
    columns = Columns(morsel)

    buffer = []
    for column in morsel.column_names:
        column_data = morsel.column(column)
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


def _full_collector(morsels):
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

    for morsel in morsels:
        if columns is None:
            columns = Columns(morsel)

        for column in morsel.column_names:
            column_data = morsel.column(column)
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


def _extended_collector(morsels):
    """
    Collect summary statistics about each column
    """
    import orso
    from orso import converters

    from opteryx import utils

    rows, schema = converters.from_arrow(utils.arrow.rename_columns(morsels))
    df = orso.DataFrame(rows=rows, schema=schema)
    table = df.profile.arrow()
    table = Columns.create_table_metadata(
        table=table,
        expected_rows=table.num_rows,
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

        if len(self._producers) != 1:  # pragma: no cover
            raise SqlError(f"{self.name} on expects a single producer")

        morsels = self._producers[0]  # type:ignore

        if morsels is None:
            return None

        if not (self._full or self._extended):
            # if it's not full or extended, do just get the list of columns and their
            # types
            yield _simple_collector(next(morsels.execute()))
            return

        if self._full and not self._extended:
            # we're going to read the full table, so we can count stuff
            yield _full_collector(morsels.execute())
            return

        if self._extended:
            # get everything we can reasonable get
            yield _extended_collector(morsels.execute())
            return
