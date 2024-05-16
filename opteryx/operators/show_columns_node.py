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
from typing import Generator

import pyarrow

from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


def _simple_collector(schema):
    """
    We've been given the schema, so just translate to a table
    """
    buffer = []
    for column in schema.columns:
        new_row = {
            "name": column.name,
            "type": column.type,
            "nullable": column.nullable,
            "aliases": column.aliases,
        }
        buffer.append(new_row)

    table = pyarrow.Table.from_pylist(buffer)
    return table


def _extended_collector(morsels):
    """
    Collect summary statistics about each column

    We use orso, which means converting to an orso DataFrame and then converting back
    to a PyArrow table.
    """
    import orso

    profile = None
    for morsel in morsels:
        df = orso.DataFrame.from_arrow(morsel)
        if profile is None:
            profile = df.profile
        else:
            profile += df.profile

    return profile.to_dicts()


class ShowColumnsNode(BasePlanNode):

    operator_type = OperatorType.PRODUCER

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self._full = config.get("full")
        self._extended = config.get("extended")
        self._schema = config.get("schema")
        self._column_map = {c.schema_column.identity: c.source_column for c in config["columns"]}

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "Show Columns"

    @property
    def config(self):  # pragma: no cover
        return ""

    def rename_column(self, dic: dict, renames) -> dict:
        dic["name"] = renames[dic["name"]]
        return dic

    def execute(self) -> Generator:
        morsels = self._producers[0]  # type:ignore

        if morsels is None:
            return None

        if not (self._full or self._extended):
            # if it's not full or extended, do just get the list of columns and their
            # types
            yield _simple_collector(self._schema)
            return

        if self._full and not self._extended:
            # we're going to read the full table, so we can count stuff
            dicts = _extended_collector(morsels.execute())
            dicts = [self.rename_column(d, self._column_map) for d in dicts]
            yield pyarrow.Table.from_pylist(dicts)
            return

        if self._extended:
            # get everything we can reasonable get
            dicts = _extended_collector(morsels.execute())
            dicts = [self.rename_column(d, self._column_map) for d in dicts]
            yield pyarrow.Table.from_pylist(dicts)
            return
