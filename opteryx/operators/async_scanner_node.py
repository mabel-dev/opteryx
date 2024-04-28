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
Async Scanner Node

This is the SQL Query Execution Plan Node responsible for the reading of data.

It wraps different internal readers (e.g. GCP Blob reader, SQL Reader), 
normalizes the data into the format for internal processing. 
"""
import asyncio
import queue
import threading
import time
from typing import Generator

import aiohttp
import pyarrow
import pyarrow.parquet
from orso.schema import RelationSchema

from opteryx.operators.scanner_node import ScannerNode
from opteryx.shared import AsyncMemoryPool
from opteryx.shared import MemoryPool
from opteryx.utils.file_decoders import get_decoder

CONCURRENT_READS = 4
MAX_BUFFER_SIZE_MB = 256


def normalize_morsel(schema: RelationSchema, morsel: pyarrow.Table) -> pyarrow.Table:
    if len(schema.columns) == 0:
        one_column = pyarrow.array([1] * morsel.num_rows, type=pyarrow.int8())
        morsel = morsel.append_column("*", one_column)
        return morsel.select(["*"])

    # rename columns for internal use
    target_column_names = []
    # columns in the data but not in the schema, droppable
    droppable_columns = []

    # find which columns to drop and which columns we already have
    for i, column in enumerate(morsel.column_names):
        column_name = schema.find_column(column)
        if column_name is None:
            droppable_columns.append(i)
        else:
            target_column_names.append(str(column_name))

    # remove from the end otherwise we'll remove the wrong columns after the first one
    droppable_columns.reverse()
    for droppable in droppable_columns:
        morsel = morsel.remove_column(droppable)

    # remane columns to the internal names (identities)
    morsel = morsel.rename_columns(target_column_names)

    # add columns we don't have
    for column in schema.columns:
        if column.identity not in target_column_names:
            null_column = pyarrow.array([None] * morsel.num_rows)
            morsel = morsel.append_column(column.identity, null_column)

    # ensure the columns are in the right order
    return morsel.select([col.identity for col in schema.columns])


async def fetch_data(blob_names, pool, reader, queue, statistics):
    semaphore = asyncio.Semaphore(
        CONCURRENT_READS
    )  # Adjust based on memory and expected data sizes

    session = aiohttp.ClientSession()

    async def fetch_and_process(blob_name):
        async with semaphore:
            start_clock = time.monotonic_ns()
            reference = await reader(
                blob_name=blob_name, pool=pool, session=session, statistics=statistics
            )
            statistics.time_reading_blobs += time.monotonic_ns() - start_clock
            queue.put((blob_name, reference))  # Put data onto the queue

    tasks = (fetch_and_process(blob) for blob in blob_names)

    await asyncio.gather(*tasks)
    queue.put(None)
    await session.close()


class AsyncScannerNode(ScannerNode):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pool = MemoryPool(MAX_BUFFER_SIZE_MB * 1024 * 1024, "read_buffer")

    def execute(self) -> Generator:
        """Perform this step, time how long is spent doing work"""
        morsel = None
        orso_schema = self.parameters["schema"]
        reader = self.parameters["connector"]

        orso_schema_cols = []
        for col in orso_schema.columns:
            if col.identity in [c.identity for c in self.columns]:
                orso_schema_cols.append(col)
        orso_schema.columns = orso_schema_cols
        arrow_schema = None
        start_clock = time.monotonic_ns()

        morsel = None

        blob_names = reader.partition_scheme.get_blobs_in_partition(
            start_date=reader.start_date,
            end_date=reader.end_date,
            blob_list_getter=reader.get_list_of_blob_names,
            prefix=reader.dataset,
        )

        data_queue = queue.Queue()

        t = time.monotonic()
        loop = asyncio.new_event_loop()
        threading.Thread(
            target=lambda: loop.run_until_complete(
                fetch_data(
                    blob_names,
                    AsyncMemoryPool(self.pool),
                    reader.async_read_blob,
                    data_queue,
                    self.statistics,
                )
            ),
            daemon=True,
        ).start()

        while True:
            item = data_queue.get()
            if item is None:
                break
            blob_name, reference = item

            decoder = get_decoder(blob_name)
            blob_bytes = self.pool.read_and_release(reference)
            decoded = decoder(blob_bytes, projection=self.columns, selection=self.predicates)
            num_rows, num_columns, morsel = decoded
            self.statistics.rows_seen += num_rows

            morsel = normalize_morsel(orso_schema, morsel)

            self.statistics.blobs_read += 1
            self.statistics.rows_read += morsel.num_rows
            self.statistics.bytes_processed += morsel.nbytes

            yield morsel

        if morsel:
            self.statistics.columns_read += morsel.num_columns

        print(time.monotonic() - t)
