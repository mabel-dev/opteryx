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
Left Join Node

This is a SQL Query Execution Plan Node.

This performs a LEFT (OUTER) JOIN
"""
from typing import Iterable

import numpy
import pyarrow

from opteryx.exceptions import ColumnNotFoundError, SqlError
from opteryx.models import Columns, QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.third_party import pyarrow_ops
from opteryx.utils import arrow

OUTER_JOINS = {
    "FullOuter": "Full Outer",
    "LeftOuter": "Left Outer",
    "RightOuter": "Right Outer",
}

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


class OuterJoinNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self._join_type = OUTER_JOINS[config.get("join_type")]
        self._on = config.get("join_on")
        self._using = config.get("join_using")

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

        if len(self._producers) != 2:
            raise SqlError(f"{self.name} expects two producers")

        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        right_table = pyarrow.concat_tables(right_node.execute(), promote=True)

        right_columns = Columns(right_table)
        left_columns = None

        for page in left_node.execute():

            if self._using:

                right_join_columns = [
                    right_columns.get_column_from_alias(col, only_one=True)
                    for col in self._using
                ]

                for page in left_node.execute():

                    if left_columns is None:
                        left_columns = Columns(page)
                        left_join_columns = [
                            left_columns.get_column_from_alias(col, only_one=True)
                            for col in self._using
                        ]
                        new_metadata = left_columns + right_columns

                        # we estimate the cardinality of the left table to inform the
                        # batch size for the joins. Cardinality here is the ratio of
                        # unique values in the set. Although we're working it out, we'll
                        # refer to this as an estimate because it may be different per
                        # page of data - we're assuming it's not very different.
                        cols = pyarrow_ops.columns_to_array_denulled(
                            page, left_join_columns
                        )
                        if page.num_rows > 0:
                            card = len(numpy.unique(cols)) / page.num_rows
                        else:
                            card = 0
                        batch_size = calculate_batch_size(card)

                    # we break this into small chunks otherwise we very quickly run into memory issues
                    for batch in page.to_batches(max_chunksize=batch_size):

                        # blocks don't have column_names, so we need to wrap in a table
                        batch = pyarrow.Table.from_batches([batch], schema=page.schema)

                        new_page = pyarrow_ops.left_join(
                            right_table, batch, right_join_columns, left_join_columns
                        )
                        new_page = new_metadata.apply(new_page)
                        yield new_page

            elif self._on:

                if left_columns is None:
                    left_columns = Columns(page)
                    try:
                        right_join_column = right_columns.get_column_from_alias(
                            self._on.right.value, only_one=True
                        )
                        left_join_column = left_columns.get_column_from_alias(
                            self._on.left.value, only_one=True
                        )
                    except ColumnNotFoundError:
                        # the ON condition may not always be in the order of the tables
                        # we purposefully reference the columns incorrectly
                        right_join_column = right_columns.get_column_from_alias(
                            self._on.left.value, only_one=True
                        )
                        left_join_column = left_columns.get_column_from_alias(
                            self._on.right.value, only_one=True
                        )

                    # ensure the types are compatible for joining by coercing numerics
                    right_table = arrow.coerce_columns(right_table, right_join_column)

                new_metadata = right_columns + left_columns

                page = arrow.coerce_columns(page, left_join_column)

                # do the join
                new_page = page.join(
                    right_table,
                    keys=[left_join_column],
                    right_keys=[right_join_column],
                    join_type=self._join_type.lower(),
                    coalesce_keys=False,
                )
                # update the metadata
                new_page = new_metadata.apply(new_page)
                yield new_page
