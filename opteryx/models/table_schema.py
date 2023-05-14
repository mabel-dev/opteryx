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

from dataclasses import dataclass
from typing import List

from opteryx.constants.attribute_types import OPTERYX_TYPES


@dataclass
class Column:
    name: str
    data_type: OPTERYX_TYPES
    aliases: List[str] = None


@dataclass
class TableSchema:
    table_name: str
    columns: List[Column]
