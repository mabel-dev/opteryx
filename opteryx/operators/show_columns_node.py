# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Show Columns Node

This is a SQL Query Execution Plan Node.

Gives information about a dataset's columns
"""

import pyarrow

from opteryx import EOS
from opteryx.models import QueryProperties

from . import BasePlanNode


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

    return pyarrow.Table.from_pylist(buffer)


class ShowColumnsNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._full = parameters.get("full")
        self._extended = parameters.get("extended")
        self._schema = parameters.get("schema")
        self._column_map = {
            c.schema_column.identity: c.source_column for c in parameters["columns"]
        }
        self.collector = None
        self.seen = False

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

    def execute(self, morsel: pyarrow.Table, **kwargs) -> pyarrow.Table:
        from orso import DataFrame

        if self.seen:
            yield None
            return

        if not (self._full or self._extended):
            # if it's not full or extended, do just get the list of columns and their
            # types
            self.seen = True
            yield _simple_collector(self._schema)
            return

        if self._full or self._extended:
            # we're going to read the full table, so we can count stuff

            if morsel == EOS:
                dicts = self.collector.to_dicts()
                dicts = [self.rename_column(d, self._column_map) for d in dicts]
                self.seen = True
                yield pyarrow.Table.from_pylist(dicts)
                return

            df = DataFrame.from_arrow(morsel)

            if self.collector is None:
                self.collector = df.profile
            else:
                self.collector += df.profile

            yield None
