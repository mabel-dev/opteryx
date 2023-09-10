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
Build Statistics Node

This is a SQL Query Execution Plan Node.

Gives information about a dataset's columns
"""
from typing import Iterable

from opteryx.exceptions import SqlError
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode


class BuildStatisticsNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)

    @property
    def name(self):  # pragma: no cover
        return "Analyze Table"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Iterable:
        morsels = self._producers[0]  # type:ignore

        if morsels is None:  # pragma: no cover
            return None

        import orso

        for morsel in morsels.execute():
            from orso import converters

            from opteryx import utils

            rows, schema = converters.from_arrow(utils.arrow.rename_columns([morsel]))
            df = orso.DataFrame(rows=rows, schema=schema)
            yield df.profile.arrow()
