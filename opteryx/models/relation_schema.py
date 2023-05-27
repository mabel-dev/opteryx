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

import datetime
import typing
from dataclasses import dataclass
from dataclasses import field

from opteryx.constants.attribute_types import OPTERYX_TYPES


@dataclass
class FlatColumn:
    # This is a standard column, backed by PyArrow
    name: str
    data_type: OPTERYX_TYPES
    description: str = None
    aliases: typing.List[str] = field(default_factory=list)


@dataclass
class ConstantColumn(FlatColumn):
    # Rather than pass around columns of constant values, where we can we should
    # replace them with this column type.
    length: int = 0
    value: typing.Any = None


@dataclass
class RelationSchema:
    table_name: str
    aliases: typing.List[str] = field(default_factory=list)
    columns: typing.List[FlatColumn] = field(default_factory=list)
    temporal_start: typing.Optional[datetime.datetime] = None
    temporal_end: typing.Optional[datetime.datetime] = None

    def find_column(self, column_name: str):
        # this is a simple match, this returns the column object
        for column in self.columns:
            if column.name == column_name:
                return column
            if column_name in column.aliases:
                return column
        return None

    def get_all_columns(self):
        # this returns all the names for columns in this relation
        for column in self.columns:
            yield column.name
            yield from column.aliases
