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
Morsel Defragment Node

This is a SQL Query Execution Plan Node.

    Orignally implemented to test if datasets have any records as they pass through the DAG, this
    function normalizes the number of bytes per morsel.

    This is to balance two competing demands:
        - operate in a low memory environment, if the morsels are too large they may cause the
          process to fail.
        - operate quickly, if we spend our time doing Vecorization/SIMD on morsel with few records
           we're not working as fast as we can.

    The low-water mark is 75% of the target size, less than this we look to merge morsels together.
    This is more common following the implementation of projection push down, one column doesn't
    take up a lot of memory so we consolidate tens of morsels into a single morsel.

    The high-water mark is 199% of the target size, more than this we split the morsel. Splitting
    at a size any less than this will end up with morsels less that the target morsel size.

    We also have a record count limit, this is because of quirks with PyArrow, it changes long
    arrays into ChunkedArrays which behave differently to Arrays in some circumstances.
"""
import time
from typing import Iterable

import pyarrow

from opteryx.operators import BasePlanNode

MORSEL_SIZE_BYTES: int = 64 * 1024 * 1024  # 64Mb
MORSEL_SIZE_COUNT: int = 500000  # hard record count limit, half a million
HIGH_WATER: float = 1.99  # Split morsels over 199% of MORSEL_SIZE
LOW_WATER: float = 0.75  # Merge morsels under 75% of MORSEL_SIZE


class MorselDefragmentNode(BasePlanNode):
    @property
    def name(self):  # pragma: no cover
        return "Morsel Defragment"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Iterable:
        morsels = self._producers[0]  # type:ignore

        # we can disable this function in the properties
        if not self.properties.enable_morsel_defragmentation:
            yield from morsels
            return

        row_counter = 0
        collected_rows = None
        at_least_one_morsel = False

        for morsel in morsels.execute():
            if morsel.num_rows > 0:
                start = time.monotonic_ns()
                # add what we've collected before to the table
                if collected_rows:  # pragma: no cover
                    self.statistics.chunk_merges += 1
                    morsel = pyarrow.concat_tables([collected_rows, morsel], mode="default")
                    collected_rows = None
                self.statistics.time_defragmenting += time.monotonic_ns() - start

                # work out some stats about what we have
                morsel_bytes = morsel.nbytes
                morsel_records = morsel.num_rows

                # if we're more than double the target size, let's do something
                if (
                    morsel_bytes > (MORSEL_SIZE_BYTES * HIGH_WATER)
                    or morsel_records > MORSEL_SIZE_COUNT
                ):  # pragma: no cover
                    start = time.monotonic_ns()

                    average_record_size = morsel_bytes / morsel_records
                    new_row_count = min(
                        int(MORSEL_SIZE_BYTES / average_record_size), MORSEL_SIZE_COUNT
                    )
                    row_counter += new_row_count
                    self.statistics.chunk_splits += 1
                    new_morsel = morsel.slice(offset=0, length=new_row_count)
                    at_least_one_morsel = True
                    collected_rows = morsel.slice(offset=new_row_count)

                    self.statistics.time_defragmenting += time.monotonic_ns() - start

                    yield new_morsel
                # if we're less than 75% of the morsel size, save hold what we have so far and go
                # collect the next morsel
                elif morsel_bytes < (MORSEL_SIZE_BYTES * LOW_WATER):
                    collected_rows = morsel
                # otherwise the morsel size is okay so we can emit the current morsel
                else:
                    row_counter += morsel_records
                    yield morsel
                    at_least_one_morsel = True
            elif not at_least_one_morsel:
                # we have to emit something to the next step, but don't emit multiple empty morsels
                yield morsel
                at_least_one_morsel = True

        # if we're at the end and haven't emitted all the records, emit them now
        if collected_rows:
            row_counter += collected_rows.num_rows
            yield collected_rows


"""
untested cython implementation


# morsel_defragment_node.pyx

from cython import boundscheck, wraparound, cdivision
import time
from typing import Iterable
import pyarrow
from opteryx.operators import BasePlanNode

cdef int MORSEL_SIZE_BYTES = 64 * 1024 * 1024  # 64Mb
cdef int MORSEL_SIZE_COUNT = 500000  # hard record count limit, half a million
cdef float HIGH_WATER = 1.99  # Split morsels over 199% of MORSEL_SIZE
cdef float LOW_WATER = 0.75  # Merge morsels under 75% of MORSEL_SIZE

@boundscheck(False)
@wraparound(False)
@cdivision(True)
class MorselDefragmentNode(BasePlanNode):
    @property
    def name(self):  # pragma: no cover
        return "Morsel Defragment"

    @property
    def config(self):  # pragma: no cover
        return ""

    cpdef execute(self) -> Iterable:
        cdef long start
        cdef int row_counter = 0, morsel_bytes, morsel_records, new_row_count
        cdef float average_record_size
        cdef object morsels = self._producers[0]  # type:ignore
        cdef object collected_rows = None
        cdef bool at_least_one_morsel = False

        if not self.properties.enable_morsel_defragmentation:
            yield from morsels
            return

        for morsel in morsels.execute():
            if morsel.num_rows > 0:
                start = time.monotonic_ns()
                if collected_rows:
                    self.statistics.chunk_merges += 1
                    morsel = pyarrow.concat_tables([collected_rows, morsel], mode='default')
                    collected_rows = None
                self.statistics.time_defragmenting += time.monotonic_ns() - start

                morsel_bytes = morsel.nbytes
                morsel_records = morsel.num_rows

                if (morsel_bytes > (MORSEL_SIZE_BYTES * HIGH_WATER)) or (morsel_records > MORSEL_SIZE_COUNT):
                    start = time.monotonic_ns()
                    average_record_size = morsel_bytes / morsel_records
                    new_row_count = min(
                        int(MORSEL_SIZE_BYTES / average_record_size), MORSEL_SIZE_COUNT
                    )
                    row_counter += new_row_count
                    self.statistics.chunk_splits += 1
                    new_morsel = morsel.slice(offset=0, length=new_row_count)
                    at_least_one_morsel = True
                    collected_rows = morsel.slice(offset=new_row_count)
                    self.statistics.time_defragmenting += time.monotonic_ns() - start
                    yield new_morsel
                elif morsel_bytes < (MORSEL_SIZE_BYTES * LOW_WATER):
                    collected_rows = morsel
                else:
                    row_counter += morsel_records
                    yield morsel
                    at_least_one_morsel = True
            elif not at_least_one_morsel:
                yield morsel
                at_least_one_morsel = True

        if collected_rows:
            row_counter += collected_rows.num_rows
            yield collected_rows

"""
