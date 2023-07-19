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


- I need:
    - to know if any of the fields in the ON need evaluating
    - to know what the identity of the columns in the join are

    - to do any evalulations
    - to do the join
"""
from typing import Iterable

import numpy
import pyarrow

from opteryx.exceptions import SqlError
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.third_party import pyarrow_ops

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

        if self._on is None and self._using is None:
            raise SqlError("Missing JOIN 'ON' condition.")

        if self._using is not None and self._join_type.lower() != "left outer":
            raise SqlError("JOIN `USING` only valid for `INNER` and `LEFT` joins.")

    @property
    def name(self):  # pragma: no cover
        return f"{self._join_type} Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Iterable:
        if len(self._producers) != 2:  # pragma: no cover
            raise SqlError(f"{self.name} expects two producers")

        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        right_table = pyarrow.concat_tables(right_node.execute(), promote=True)

        right_columns = right_table.column_names
        left_columns = None

        for morsel in left_node.execute():
            if self._using:
                right_join_columns = [
                    right_columns.get_column_from_alias(col, only_one=True) for col in self._using
                ]

                for morsel in left_node.execute():
                    # we estimate the cardinality of the left table to inform the
                    # batch size for the joins. Cardinality here is the ratio of
                    # unique values in the set. Although we're working it out, we'll
                    # refer to this as an estimate because it may be different per
                    # chunk of data - we're assuming it's not very different.
                    cols = pyarrow_ops.columns_to_array_denulled(morsel, left_join_columns)
                    if morsel.num_rows > 0:
                        card = len(numpy.unique(cols)) / morsel.num_rows
                    else:
                        card = 0
                    batch_size = calculate_batch_size(card)

                # we break this into small chunks otherwise we very quickly run into memory issues
                for batch in morsel.to_batches(max_chunksize=batch_size):
                    # blocks don't have column_names, so we need to wrap in a table
                    batch = pyarrow.Table.from_batches([batch], schema=morsel.schema)

                    new_morsel = pyarrow_ops.left_join(
                        right_table, batch, right_join_columns, left_join_columns
                    )
                    new_morsel = new_metadata.apply(new_morsel)
                    yield new_morsel

            elif self._on:
                # do the join
                new_morsel = morsel.join(
                    right_table,
                    keys=[self._on.left.schema_column.identity],
                    right_keys=[self._on.right.schema_column.identity],
                    join_type=self._join_type,
                    coalesce_keys=False,
                )
                # update the metadata
                yield new_morsel
