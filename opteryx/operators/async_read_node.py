# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

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

import pyarrow
from orso.schema import convert_orso_schema_to_arrow_schema

from opteryx import EOS
from opteryx import config
from opteryx.exceptions import DataError
from opteryx.models import QueryProperties
from opteryx.shared import AsyncMemoryPool
from opteryx.shared import MemoryPool
from opteryx.utils.file_decoders import get_decoder

from .read_node import ReaderNode
from .read_node import normalize_morsel
from .read_node import struct_to_jsonb

CONCURRENT_READS = config.CONCURRENT_READS
MAX_READ_BUFFER_CAPACITY = config.MAX_READ_BUFFER_CAPACITY
ZERO_COPY = config.features.enable_zero_copy_buffer_reads


async def fetch_data(blob_names, pool, reader, reply_queue, statistics):
    import aiohttp

    semaphore = asyncio.Semaphore(CONCURRENT_READS)
    session = aiohttp.ClientSession(
        headers={"Accept-Encoding": "gzip, br"},
    )

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


class AsyncReaderNode(ReaderNode):
    def __init__(self, properties: QueryProperties, **parameters):
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.pool = MemoryPool(MAX_READ_BUFFER_CAPACITY, f"ReadBuffer <{self.parameters['alias']}>")

        self.predicates = parameters.get("predicates")

        self.rows_seen = 0
        self.blobs_seen = 0

    @property
    def name(self):  # pragma: no cover
        """friendly name for this step"""
        return "Async Read"

    def execute(self, morsel, **kwargs) -> Generator:
        if morsel == EOS:
            yield None
            return

        from opteryx import system_statistics

        # Perform this step, time how long is spent doing work
        orso_schema = self.parameters["schema"]
        reader = self.parameters["connector"]

        orso_schema_cols = []
        for col in orso_schema.columns:
            if col.identity in [c.schema_column.identity for c in self.columns]:
                orso_schema_cols.append(col)
        orso_schema.columns = orso_schema_cols

        self.statistics.columns_read += len(orso_schema.columns)

        blob_names = reader.partition_scheme.get_blobs_in_partition(
            start_date=reader.start_date,
            end_date=reader.end_date,
            blob_list_getter=reader.get_list_of_blob_names,
            prefix=reader.dataset,
            predicates=self.predicates,
        )

        if len(blob_names) == 0:
            # if we don't have any matching blobs, create an empty dataset
            from orso import DataFrame

            as_arrow = DataFrame(rows=[], schema=orso_schema).arrow()
            renames = [orso_schema.column(col).identity for col in as_arrow.column_names]
            as_arrow = as_arrow.rename_columns(renames)
            yield as_arrow

        data_queue: queue.Queue = queue.Queue()

        loop = asyncio.new_event_loop()
        read_thread = threading.Thread(
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
        )
        read_thread.start()

        morsel = None
        arrow_schema = convert_orso_schema_to_arrow_schema(orso_schema, use_identities=True)

        while True:
            try:
                # Attempt to get an item with a timeout.
                item = data_queue.get(timeout=0.1)
            except queue.Empty:
                # Increment stall count if the queue is empty.
                self.statistics.stalls_reading_from_read_buffer += 1
                system_statistics.io_wait_seconds += 0.1
                continue  # Skip the rest of the loop and try to get an item again.

            if item is None:
                # Break out of the loop if the item is None, indicating a termination condition.
                break

            blob_name, reference = item

            decoder = get_decoder(blob_name)

            try:
                # the sync readers include the decode time as part of the read time
                try:
                    # zero copy reduces copy overhead, but we need to latch the segment
                    # to ensure it is not overwritten while we are reading it.
                    start = time.monotonic_ns()
                    blob_memory_view = self.pool.read(
                        reference, zero_copy=ZERO_COPY, latch=ZERO_COPY
                    )
                    self.statistics.bytes_read += len(blob_memory_view)
                    decoded = decoder(
                        blob_memory_view, projection=self.columns, selection=self.predicates
                    )
                    if ZERO_COPY:
                        # if we used zero copy, we need to release the latch
                        self.pool.release(reference)
                    self.pool.release(reference)  # release also unlatches the segment
                except Exception as err:
                    from pyarrow import ArrowInvalid

                    if isinstance(err, ArrowInvalid) and "No match for" in str(err):
                        raise DataError(
                            f"Unable to read blob {blob_name} - this error is likely caused by a blob having an significantly different schema to previously handled blobs, or the data catalog."
                        )
                    raise DataError(f"Unable to read blob {blob_name} - error {err}") from err
                self.statistics.time_reading_blobs += time.monotonic_ns() - start
                num_rows, _, morsel = decoded
                self.statistics.rows_seen += num_rows

                morsel = struct_to_jsonb(morsel)
                morsel = normalize_morsel(orso_schema, morsel)
                if morsel.column_names != ["*"]:
                    morsel = morsel.cast(arrow_schema)

                self.statistics.blobs_read += 1
                self.statistics.rows_read += morsel.num_rows
                self.statistics.bytes_processed += morsel.nbytes

                self.rows_seen += morsel.num_rows
                self.blobs_seen += 1

                yield morsel
            except Exception as err:
                self.statistics.add_message(f"failed to read {blob_name}")
                self.statistics.failed_reads += 1
                import warnings

                warnings.warn(f"failed to read {blob_name} - {err}")

        # Ensure the thread is closed
        read_thread.join()

        if morsel is None:
            self.statistics.empty_datasets += 1
            arrow_schema = convert_orso_schema_to_arrow_schema(orso_schema, use_identities=True)
            yield pyarrow.Table.from_arrays(
                [pyarrow.array([]) for _ in arrow_schema], schema=arrow_schema
            )
