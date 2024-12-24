# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the system variables collection.
"""

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

__all__ = ("read", "schema")


def read(end_date=None, variables={}):
    import pyarrow

    buffer = []
    for variable in variables:
        variable_type, variable_value, variable_owner, variable_visibility = variables.details(
            variable
        )
        buffer.append(
            {
                "name": variable,
                "value": str(variable_value),
                "type": variable_type,
                "owner": variable_owner.name,
                "visibility": variable_visibility.name,
            }
        )

    return pyarrow.Table.from_pylist(buffer)


def schema():
    # fmt:off
    return  RelationSchema(
        name="$variables",
        columns=[
            FlatColumn(name="name", type=OrsoTypes.VARCHAR),
            FlatColumn(name="value", type=OrsoTypes.VARCHAR),
            FlatColumn(name="type", type=OrsoTypes.VARCHAR),
            FlatColumn(name="owner", type=OrsoTypes.VARCHAR),
            FlatColumn(name="visibility", type=OrsoTypes.VARCHAR),
        ],
    )
    # fmt:on
