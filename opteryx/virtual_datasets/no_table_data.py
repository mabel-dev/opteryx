# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
no table
---------

This is used to prepresent no table.

It actually is a table, with one row and one column.
"""

import pyarrow
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.models import RelationStatistics

__all__ = ("read", "schema")


def read(*args) -> pyarrow.Table:
    # Create a PyArrow table with one column and one row
    arrow_schema = pyarrow.schema([pyarrow.field("$column", pyarrow.int64())])
    return pyarrow.Table.from_arrays(
        [pyarrow.array([0], type=pyarrow.int64())], schema=arrow_schema
    )


def schema():
    # fmt:off
    return RelationSchema(name="$no_table", columns=[FlatColumn(name="$column", type=OrsoTypes.INTEGER)])
    # fmt:on


def statistics() -> RelationStatistics:
    return RelationStatistics()
