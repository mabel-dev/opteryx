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
Join Node

This is a SQL Query Execution Plan Node.

This handles most of the join types as a wrapper for pyarrow's JOIN functions, 
only CROSS JOINs are not handled here.
"""
from typing import Iterable

import pyarrow

from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode

INTERNAL_BATCH_SIZE = 500  # config


def calculate_batch_size(cardinality):
    """dynamically work out the processing batch size for the USING JOIN"""
    # - HIGH_CARDINALITY_BATCH_SIZE (over 90% unique) = INTERNAL_BATCH_SIZE
    # - MEDIUM_CARDINALITY_BATCH_SIZE (5% > n < 90%) = INTERNAL_BATCH_SIZE * n
    # - LOW_CARDINALITY_BATCH_SIZE (less than 5% unique) = 5
    # These numbers have had very little science put into them, they are unlikely
    # to be optimal
    if cardinality < 0.05:
        return 5
    if cardinality > 0.9:
        return INTERNAL_BATCH_SIZE
    return INTERNAL_BATCH_SIZE * cardinality


class JoinNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self._join_type = config["type"]
        self._on = config.get("on")
        self._using = config.get("using")

        self._left_columns = config.get("left_columns")
        self._left_relation = config.get("left_relation")

        self._right_columns = config.get("right_columns")
        self._right_relation = config.get("right_relation")

    @property
    def name(self):  # pragma: no cover
        return f"{self._join_type} Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Iterable:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        right_table = pyarrow.concat_tables(right_node.execute(), mode="default")

        for morsel in left_node.execute():
            # do the join
            new_morsel = morsel.join(
                right_table,
                keys=self._right_columns,
                right_keys=self._left_columns,
                join_type=self._join_type,
                coalesce_keys=self._using is not None,
            )

            # need to ensure we put the right column back if we need it
            if (
                self._join_type in ("right anti", "right semi")
                and new_morsel.column_names != right_table.column_names
            ):
                columns = [
                    col
                    if col not in self._left_columns
                    else self._right_columns[self._left_columns.index(col)]
                    for col in new_morsel.column_names
                ]
                new_morsel = new_morsel.rename_columns(columns)

            yield new_morsel
