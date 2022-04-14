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

This performs a JOIN
"""
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import numpy
import pyarrow
from typing import Iterable
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
from opteryx.third_party import pyarrow_ops
from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.utils.columns import Columns
from opteryx import config
from opteryx.exceptions import SqlError

def cartesian_product(*arrays):
    """
    Cartesian product of arrays creates every combination of the elements in the arrays
    """
    la = len(arrays)
    arr = numpy.empty([len(a) for a in arrays] + [la], dtype=numpy.int64)
    for i, a in enumerate(numpy.ix_(*arrays)):
        arr[..., i] = a
    return numpy.hsplit(arr.reshape(-1, la), la)


def _cross_join(left, right):
    """
    A cross join is the cartesian product of two tables - this usually isn't very
    useful, but it does allow you to the theta joins (non-equi joins)
    """
    from opteryx.third_party.pyarrow_ops import align_tables

    if isinstance(left, pyarrow.Table):
        left = [left]

    right_columns = Columns(right)
    left_columns = None

    for left_page in left:

        if left_columns is None:
            left_columns = Columns(left_page)
            new_columns = left_columns + right_columns

        # we break this into small chunks, each cycle will have 100 * rows in the right table
        for left_block in left_page.to_batches(max_chunksize=config.INTERNAL_BATCH_SIZE):

            # blocks don't have column_names, so we need to wrap in a table
            left_block = pyarrow.Table.from_batches(
                [left_block], schema=left_page.schema
            )

            # build two lists, 0 to num_rows for each table
            left_array = numpy.arange(left_block.num_rows, dtype=numpy.int64)
            right_array = numpy.arange(right.num_rows, dtype=numpy.int64)

            # build the cartesian product of the two lists
            left_align, right_align = cartesian_product(left_array, right_array)

            # now build the resultant table
            table = align_tables(
                left_block, right, left_align.flatten(), right_align.flatten()
            )
            yield new_columns.apply(table)


def _cross_join_unnest(left, column, alias):
    """
    This is a specific instance the CROSS JOIN, where instead of joining on another
    table, we're joining on a field in the current row.

    This means we need to read a row, create the dataset to join with, do the join
    repeat.

    This is done by collecting the values together and creating them into a new column.

    This column-based approach benchmarked roughly 33% faster than the row-based
    approach, where each row was read as a dictionary, new dictionaries created for
    each UNNESTed value and the dictionaries combined to a table.
    """
    if column[1] != TOKEN_TYPES.IDENTIFIER:
        raise NotImplementedError("Can only CROSS JOIN UNNEST on a field")

    if isinstance(left, pyarrow.Table):
        left = [left]

    metadata = None

    for left_page in left:

        if metadata is None:
            metadata = Columns(left_page)
            metadata.add_column(alias)
            unnest_column = metadata.get_column_from_alias(column[0], only_one=True)
        
        # we break this into small chunks otherwise we very quickly run into memory issues
        for left_block in left_page.to_batches(max_chunksize=config.INTERNAL_BATCH_SIZE):

            # Get the column we're going to UNNEST
            column_data = left_block[unnest_column]

            # Create a list of indexes, this will look something like this:
            # [1,1,1,2,2,2,3,3,3] 
            # Where the number of times a number is repeated, is the length of the list
            # we're going to UNNEST for that row
            indexes = []
            for i, value in enumerate(column_data):
                indexes.extend([i] * len(value))

            # Create the new column by converting a list of lists, into one list
            new_column =  [item for sublist in column_data for item in sublist]
            # Strings need special treatment to avoid them being coerced into a list
            # of characters
            if len(new_column) > 0 and isinstance(new_column[0], (pyarrow.lib.StringScalar)):
                new_column = [[v.as_py() for v in new_column]]

            # Using the indexes above, repeat the rows of the source data
            new_block = left_block.take(indexes)
            # We can't append columns to batches, so we need to convert to a table
            new_block = pyarrow.Table.from_batches([new_block])
            # Append the column we created above, to the table with the repeated rows
            new_block = pyarrow.Table.append_column(new_block, alias, new_column)
            new_block = metadata.apply(new_block)
            yield new_block


class JoinNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._right_table = config.get("right_table")
        self._join_type = config.get("join_type", "CrossJoin")
        self._on = config.get("join_on")
        self._using = config.get("join_using")

    @property
    def name(self):
        return f"({self._join_type}) Join"

    def __repr__(self):
        return ""

    def execute(self, data_pages: Iterable) -> Iterable:

        from opteryx.engine.planner.operations import DatasetReaderNode
        from opteryx.utils.columns import Columns

        if isinstance(self._right_table, DatasetReaderNode):
            self._right_table = pyarrow.concat_tables(
                self._right_table.execute(None)
            )  # type:ignore

        if self._join_type == "CrossJoin":
            yield from _cross_join(data_pages, self._right_table)

        elif self._join_type == "CrossJoinUnnest":
            yield from _cross_join_unnest(
                left=data_pages,
                column=self._right_table[1][1][0],
                alias=self._right_table[0],
            )

        elif self._join_type == "Inner":

            if self._using:

                right_columns = Columns(self._right_table)
                left_columns = None
                right_join_columns = [right_columns.get_column_from_alias(col, only_one=True) for col in self._using]

                for page in data_pages:

                    if left_columns is None:
                        left_columns = Columns(page)
                        left_join_columns = [left_columns.get_column_from_alias(col, only_one=True) for col in self._using]
                        new_metadata = left_columns + right_columns

                    new_page = pyarrow_ops.inner_join(self._right_table, page, right_join_columns, left_join_columns)
                    new_page = new_metadata.apply(new_page)
                    yield new_page

            elif self._on:

                right_columns = Columns(self._right_table)
                left_columns = None
                right_join_column = right_columns.get_column_from_alias(self._on[2][0], only_one=True)

                for page in data_pages:

                    if left_columns is None:
                        left_columns = Columns(page)
                        left_join_column = left_columns.get_column_from_alias(self._on[0][0], only_one=True)
                        new_metadata = right_columns + left_columns

                    new_page = pyarrow_ops.inner_join(self._right_table, page, right_join_column, left_join_column)
                    new_page = new_metadata.apply(new_page)
                    yield new_page

        elif self._join_type == "LeftOuter":

            right_columns = Columns(self._right_table)
            left_columns = None
            right_join_column = right_columns.get_column_from_alias(self._on[2][0], only_one=True)

            for page in data_pages:

                if left_columns is None:
                    left_columns = Columns(page)
                    left_join_column = left_columns.get_column_from_alias(self._on[0][0], only_one=True)
                    new_metadata = right_columns + left_columns

                new_page = pyarrow_ops.left_join(self._right_table, page, right_join_column, left_join_column)
                new_page = new_metadata.apply(new_page)
                yield new_page

        else:
            raise SqlError(f"Unsupported Join type, {self._join_type}")

if __name__ == "__main__":

    import sys
    import os

    sys.path.insert(1, os.path.join(sys.path[0], "../../../../.."))

    from opteryx import samples
    from opteryx.third_party import pyarrow_ops
    from opteryx.utils.display import ascii_table
    from opteryx.utils.arrow import fetchmany
    from mabel.utils.timer import Timer

    planets = samples.astronauts()
    satellites = samples.satellites()

    #    print(planets.column_names)
    #    print(planets.to_string(preview_cols=10))
    #    planets.rename_columns(['id', 'planet_name', 'mass', 'diameter', 'density', 'gravity', 'escapeVelocity', 'rotationPeriod', 'lengthOfDay', 'distanceFromSun', 'perihelion', 'aphelion', 'orbitalPeriod', 'orbitalVelocity', 'orbitalInclination', 'orbitalEccentricity', 'obliquityToOrbit', 'meanTemperature', 'surfacePressure', 'numberOfMoons'])

    with Timer():
        joint = _cross_join([planets], satellites)  # 0.39 seconds - 63189
        c = 0
        for e, j in enumerate(joint):
            c += j.num_rows
        print(e, c)

    # right_columns = planets.column_names
    # right_columns = [f"planets.{name}" for name in right_columns]
    # planets = planets.rename_columns(right_columns)

    from opteryx.third_party.pyarrow_ops import join

    print("---")

    #    joint = join(planets, satellites, on=["id"])

    #    print(joint.to_string())
    joint = _cross_join(planets, satellites)
    print(ascii_table(fetchmany(joint, limit=10), limit=10))

    # print(joint.column_names)
