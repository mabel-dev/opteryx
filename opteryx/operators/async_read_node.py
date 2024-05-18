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
from dataclasses import dataclass
from typing import Generator

import aiohttp
import pyarrow
import pyarrow.parquet
from orso.schema import RelationSchema

from opteryx import config
from opteryx.operators.base_plan_node import BasePlanDataObject
from opteryx.operators.read_node import ReaderNode
from opteryx.shared import AsyncMemoryPool
from opteryx.shared import MemoryPool
from opteryx.utils.file_decoders import get_decoder

CONCURRENT_READS = config.CONCURRENT_READS
MAX_READ_BUFFER_CAPACITY = config.MAX_READ_BUFFER_CAPACITY


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


async def fetch_data(blob_names, pool, reader, reply_queue, statistics):
    semaphore = asyncio.Semaphore(CONCURRENT_READS)
    session = aiohttp.ClientSession()

    async def fetch_and_process(blob_name):
        async with semaphore:
            start_per_blob = time.monotonic_ns()
            reference = await reader(
                blob_name=blob_name, pool=pool, session=session, statistics=statistics
            )
            reply_queue.put((blob_name, reference))  # Put data onto the queue
            statistics.time_reading_blobs += time.monotonic_ns() - start_per_blob

    tasks = (fetch_and_process(blob) for blob in blob_names)

    await asyncio.gather(*tasks)
    reply_queue.put(None)
    await session.close()


@dataclass
class AsyncReaderDataObject(BasePlanDataObject):
    pass


class AsyncReaderNode(ReaderNode):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pool = MemoryPool(MAX_READ_BUFFER_CAPACITY, f"ReadBuffer <{self.parameters['alias']}>")

        self.do = AsyncReaderDataObject()

    @classmethod
    def from_dict(cls, dic: dict) -> "AsyncReaderNode":  # pragma: no cover
        raise NotImplementedError()

    def execute(self) -> Generator:
        """Perform this step, time how long is spent doing work"""
        orso_schema = self.parameters["schema"]
        reader = self.parameters["connector"]

        blob_names = reader.partition_scheme.get_blobs_in_partition(
            start_date=reader.start_date,
            end_date=reader.end_date,
            blob_list_getter=reader.get_list_of_blob_names,
            prefix=reader.dataset,
        )

        data_queue: queue.Queue = queue.Queue()

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

        orso_schema_cols = []
        for col in orso_schema.columns:
            if col.identity in [c.identity for c in self.columns]:
                orso_schema_cols.append(col)
        orso_schema.columns = orso_schema_cols

        morsel = None
        arrow_schema = None

        while True:
            try:
                # Attempt to get an item with a timeout.
                item = data_queue.get(timeout=0.1)
            except queue.Empty:
                # Increment stall count if the queue is empty.
                self.statistics.stalls_reading_from_read_buffer += 1
                continue  # Skip the rest of the loop and try to get an item again.

            if item is None:
                # Break out of the loop if the item is None, indicating a termination condition.
                break

            blob_name, reference = item

            decoder = get_decoder(blob_name)
            # This pool is being used by async processes in another thread, using
            # zero copy versions occassionally results in data getting corrupted
            blob_bytes = self.pool.read_and_release(reference, zero_copy=False)
            try:
                start = time.monotonic_ns()
                # the sync readers include the decode time as part of the read time
                decoded = decoder(blob_bytes, projection=self.columns, selection=self.predicates)
                self.statistics.time_reading_blobs += time.monotonic_ns() - start
                num_rows, _, morsel = decoded
                self.statistics.rows_seen += num_rows

                morsel = normalize_morsel(orso_schema, morsel)
                if arrow_schema:
                    morsel = morsel.cast(arrow_schema)
                else:
                    arrow_schema = morsel.schema

                self.statistics.blobs_read += 1
                self.statistics.rows_read += morsel.num_rows
                self.statistics.bytes_processed += morsel.nbytes

                yield morsel
            except Exception as err:
                self.statistics.add_message(f"failed to read {blob_name}")
                self.statistics.failed_reads += 1
                print(f"[READER] Cannot read blob {blob_name} due to {err}")
                raise err

        if morsel:
            self.statistics.columns_read += morsel.num_columns
