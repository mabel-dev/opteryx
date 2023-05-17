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
    aliases: typing.List[str] = None


@dataclass
class ConstantColumn(FlatColumn):
    # Rather than pass around columns of constant values, where we can
    # replace them with this column type
    type: OPTERYX_TYPES = OPTERYX_TYPES.OTHER
    length: int = 0
    value: typing.Any = None


@dataclass
class RelationSchema:
    table_name: str
    alias: typing.Optional[str] = None
    columns: typing.List[FlatColumn] = None
    temporal_start: typing.Optional[datetime.datetime] = None
    temporal_end: typing.Optional[datetime.datetime] = None

    def __post_init__(self):
        if self.columns is None:
            object.__setattr__(self, "columns", [])

    def find_column(self, column_name: str):
        return None
