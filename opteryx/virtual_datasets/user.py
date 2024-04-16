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
This is a virtual dataset which is calculated at access time.

It is the user attributes collection.
"""

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

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
