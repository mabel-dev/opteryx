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
Page Defragment Node

This is a SQL Query Execution Plan Node.

    Orignally implemented to test if datasets have any records as they pass through
    the DAG, this function normalizes the number of bytes per page.

    This is to balance two competing demands:
        - operate in a low memory environment, if the pages are too large they may
          cause the process to fail.
        - operate quickly, if we spend our time doing SIMD on pages with few records
          we're not working as fast as we can.

    The low-water mark is 75% of the target size, less than this we look to merge
    pages together. This is more common following the implementation of projection
    push down, one column doesn't take up a lot of memory so we consolidate tens of
    pages into a single page.

    The high-water mark is 199% of the target size, more than this we split the page.
    Splitting at a size any less than this will end up with pages less that the target
    page size.

"""
import time

from typing import Iterable

import pyarrow

from opteryx.operators import BasePlanNode

PAGE_SIZE = 64 * 1024 * 1024  # 64Mb
HIGH_WATER: float = 1.99  # Split pages over 199% of PAGE_SIZE
LOW_WATER: float = 0.75  # Merge pages under 75% of PAGE_SIZE


class PageDefragmentNode(BasePlanNode):
    @property
    def name(self):  # pragma: no cover
        return "Page Defragment"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Iterable:

        data_pages = self._producers[0]  # type:ignore
        if isinstance(data_pages, pyarrow.Table):
            data_pages = (data_pages,)

        # we can disable this function in the properties
        if not self.properties.enable_page_defragmentation:
            yield from data_pages
            return

        row_counter = 0
        collected_rows = None
        at_least_one_page = False

        for page in data_pages.execute():

            if page.num_rows > 0:

                start = time.monotonic_ns()
                # add what we've collected before to the table
                if collected_rows:  # pragma: no cover
                    self.statistics.page_merges += 1
                    page = pyarrow.concat_tables([collected_rows, page], promote=True)
                    collected_rows = None
                self.statistics.time_defragmenting += time.monotonic_ns() - start

                # work out some stats about what we have
                page_bytes = page.nbytes
                page_records = page.num_rows

                # if we're more than double the target size, let's do something
                if page_bytes > (PAGE_SIZE * HIGH_WATER):  # pragma: no cover
                    start = time.monotonic_ns()

                    average_record_size = page_bytes / page_records
                    new_row_count = int(PAGE_SIZE / average_record_size)
                    row_counter += new_row_count
                    self.statistics.page_splits += 1
                    new_page = page.slice(offset=0, length=new_row_count)
                    at_least_one_page = True
                    collected_rows = page.slice(offset=new_row_count)

                    self.statistics.time_defragmenting += time.monotonic_ns() - start

                    yield new_page
                # if we're less than 75% of the page size, save hold what we have so
                # far and go collect the next page
                elif page_bytes < (PAGE_SIZE * LOW_WATER):
                    collected_rows = page
                # otherwise the page size is okay so we can emit the current page
                else:
                    row_counter += page_records
                    yield page
                    at_least_one_page = True
            elif not at_least_one_page:
                # we have to emit something to the next step, but don't emit
                # multiple empty pages
                yield page
                at_least_one_page = True

        # if we're at the end and haven't emitted all the records, emit them now
        if collected_rows:
            row_counter += collected_rows.num_rows
            yield collected_rows
