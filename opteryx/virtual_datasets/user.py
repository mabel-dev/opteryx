# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the user attributes collection.
"""

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.models import RelationStatistics

__all__ = ("read", "schema")


def read(end_date=None, variables={}):
    import pyarrow

    buffer = []

    for value in variables["user_memberships"]:
        buffer.append({"attribute": "membership", "value": str(value), "type": "VARCHAR"})

    return pyarrow.Table.from_pylist(buffer)


def schema():
    # fmt:off
    return  RelationSchema(
        name="$user",
        columns=[
            FlatColumn(name="attribute", type=OrsoTypes.VARCHAR),
            FlatColumn(name="value", type=OrsoTypes.VARCHAR),
            FlatColumn(name="type", type=OrsoTypes.VARCHAR)
        ],
    )
    # fmt:on


def statistics() -> RelationStatistics:
    return RelationStatistics()
