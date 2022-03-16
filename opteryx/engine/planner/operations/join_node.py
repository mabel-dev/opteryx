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

import io
import numpy as np
import orjson
import pyarrow
from typing import Iterable
from opteryx.utils.arrow import fetchall, get_metadata
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
from opteryx.third_party import pyarrow_ops

JOIN_TYPES = ('CrossJoin', 'INNER', 'LEFT') 

JSON_TYPES = {np.bool_: bool, np.int64: int, np.float64: float}


def _serializer(obj):
    return JSON_TYPES[type(obj)](obj)

def _cross_join(left, right):
    """
    cross joins are the cartesian product of two tables, that is, the returned dataset
    is a full copy of the right dataset for every row in the left dataset - resulting
    in len(left) x len(right) number of records. It's the simplest to do.

    For all joins, we assume the right table is in memory and can be reiterated as
    needed so we're going to step the left table. 
    """
    
    pylist = []

    # if there's collisions, rename the fields
    # start with a naive rename
    left_metadata = None
    right_metadata = get_metadata(right) or {}

    right_columns = right.column_names
    right_columns = [f"{right_metadata.get('_name', 'r')}.{name}" for name in right_columns]
    right = right.rename_columns(right_columns)

    for page in left:

        #if left_metadata is None:
        #    left_metadata = get_metadata(left)

        for batch in page.to_batches(max_chunksize=100):
            for left_row in batch.to_pylist():
                for right_row in fetchall([right]):
                    new_row = {**left_row, **right_row}
                    if len(pylist) > 5000:
                        table = pyarrow.Table.from_pylist(pylist)
                        #table = arrow.set_metadata(table, metadata)
                        yield table
                        pylist = []

                    pylist.append(new_row)

    if len(pylist) > 1000:
        table = pyarrow.Table.from_pylist(pylist)
        #table = arrow.set_metadata(table, metadata)
        yield table



class JoinNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._right_table = config.get("right_table")
        self._join_type = config.get("join_type", "CrossJoin")
        self._on = config.get("join_on")

    @property
    def name(self):
        return f"({self._join_type}) Join"

    def __repr__(self):
        return ""

    def execute(self, data_pages: Iterable) -> Iterable:
        
        if not isinstance(self._right_table, pyarrow.Table):
            self._right_table = pyarrow.concat_tables(self._right_table.execute(None))  # type:ignore

        if self._join_type == "CrossJoin":
          yield from _cross_join(data_pages, self._right_table)

        elif self._join_type == "InnerUsing":
            for page in data_pages:
                yield pyarrow_ops.join(self._right_table, page, self._on)


if __name__ == "__main__":

    import sys
    import os

    sys.path.insert(1, os.path.join(sys.path[0], "../../../../.."))

    from opteryx import samples
    from opteryx.third_party import pyarrow_ops
    from opteryx.utils.display import ascii_table
    from opteryx.utils.arrow import fetchmany

    planets = samples.astronauts()
    satellites = samples.satellites()

#    print(planets.column_names)
#    print(planets.to_string(preview_cols=10))
#    planets.rename_columns(['id', 'planet_name', 'mass', 'diameter', 'density', 'gravity', 'escapeVelocity', 'rotationPeriod', 'lengthOfDay', 'distanceFromSun', 'perihelion', 'aphelion', 'orbitalPeriod', 'orbitalVelocity', 'orbitalInclination', 'orbitalEccentricity', 'obliquityToOrbit', 'meanTemperature', 'surfacePressure', 'numberOfMoons'])

    joint = _cross_join([planets], satellites)

    #right_columns = planets.column_names
    #right_columns = [f"planets.{name}" for name in right_columns]
    #planets = planets.rename_columns(right_columns)

    from opteryx.third_party.pyarrow_ops import join

    print('---')

#    joint = join(planets, satellites, on=["id"])

#    print(joint.to_string())

    print(ascii_table(fetchmany(joint, limit=10), limit=10))

    #print(joint.column_names)