"""
Blob Reader Node

This is a SQL Query Execution Plan Node.

This node performs the lower-level reading of each blob. It should be called by the
PartitionReader which takes care of things like working out the if and how a blob
should be read.
"""
from typing import Iterable
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.storage.adapters.local.disk_store import DiskStorage


class BlobReaderNode(BasePlanNode):

    def __init__(self, statistics:QueryStatistics, **config):

        self._statistics = statistics

        self._reader = config.get("reader", DiskStorage())
        self._decoder = config.get("decoder")

        # pushed down projection
        self._projection = config.get("projection")
        # pushed down selection
        self._selection = config.get("selection")

    def __repr__(self):
        return self._blob_name

    def execute(self, data_pages:Iterable) -> Iterable:

        for page in data_pages:

            # Read the blob from storage, it's just a stream of bytes at this point
            blob_bytes = self._reader.read_blob(page)

            # record the number of bytes we're reading
            self._statistics.bytes_read_data += blob_bytes.getbuffer().nbytes

            # interpret the raw bytes into entries
            pyarrow_blob = self._decoder(blob_bytes, self._projection)

            # we should know the number of entries
            self._statistics.rows_read += pyarrow_blob.num_rows

            # return the result
            yield pyarrow_blob

