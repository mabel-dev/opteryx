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

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

__all__ = ("read", "schema")


def read(*args):
    import pyarrow

    # Create a PyArrow table with one column and one row

    return pyarrow.Table.from_arrays([[0]], ["$column"])  # schema=_schema)


def schema():
    # fmt:off
    return RelationSchema(name="$no_table", columns=[FlatColumn(name="$column", type=OrsoTypes.INTEGER)])
    # fmt:on
