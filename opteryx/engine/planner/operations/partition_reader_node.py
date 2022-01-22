"""
Partition Reader Node

This is a SQL Query Execution Plan Node.

This Node reads and parses the data from a partition into a Relation.

We plan to do the following:

USE THE ZONEMAP:
- Respond to simple aggregations using the zonemap, such as COUNT(*)
- Use BRIN and selections to filter out blobs from being read that don't contain
  records which can match the selections.
"""
from enum import Enum
from typing import Iterable
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.storage import file_decoders
from opteryx.storage.adapters.local.disk_store import DiskStorage
from opteryx.utils import paths


class EXTENSION_TYPE(str, Enum):
    # labels for the file extentions
    DATA = "DATA"
    CONTROL = "CONTROL"


do_nothing = lambda x: x

KNOWN_EXTENSIONS = {
    "complete": (do_nothing, EXTENSION_TYPE.CONTROL),
    "ignore": (do_nothing, EXTENSION_TYPE.CONTROL),
    "jsonl": (file_decoders.jsonl_decoder, EXTENSION_TYPE.DATA),
    "metadata": (do_nothing, EXTENSION_TYPE.CONTROL),
    "orc": (file_decoders.orc_decoder, EXTENSION_TYPE.DATA),
    "parquet": (file_decoders.parquet_decoder, EXTENSION_TYPE.DATA),
    "zstd": (file_decoders.zstd_decoder, EXTENSION_TYPE.DATA),
}


class PartitionReaderNode(BasePlanNode):
    def __init__(self, statistics:QueryStatistics, **config):
        """
        The Partition Reader Node is responsible for reading a complete partition
        and returning a Relation.
        """
        self._partition = config.get("partition", "").replace(".", "/") + "/"
        self._reader = config.get("reader", DiskStorage())
        self._partition_scheme = config.get("partition_scheme")

        self._statistics = statistics

        # pushed down projection
        self._projection = config.get("projection")
        # pushed down selection
        self._selection = config.get("selection")

    def __repr__(self):
        return self._partition

    def execute(self, data_pages:Iterable) -> Iterable:

        # Get a list of all of the blobs in the partition.
        blob_list = self._reader.get_blob_list(self._partition)

        # remove folders, end with '/'
        blob_list = [blob for blob in blob_list if not blob.endswith("/")]

        # Track how many blobs we found
        self._statistics.count_blobs_found += len(blob_list)

        # Filter the blob list to just the frame we're interested in
        if self._partition_scheme is not None:
            blob_list = self._partition_scheme.filter_blobs(blob_list)

        # If there's a zonemap for the partition, read it
#        zonemap = {}
#        zonemap_files = [blob for blob in blob_list if blob.endswith("/frame.metadata")]
#        if len(zonemap_files) == 1:
#            # read the zone map into a dictionary
#            try:
#                import orjson
#                zonemap = orjson.loads(self._reader.read_blob(zonemap_files[0]))
#            except:
#                pass

        for blob_name in blob_list:

            # the the blob filename extension
            extension = blob_name.split(".")[-1]

            # find out how to read this blob
            decoder, file_type = KNOWN_EXTENSIONS.get(extension, (None, None))
            # if it's not a known data file, skip reading it
            if file_type != EXTENSION_TYPE.DATA:
                continue

            # can we eliminate this blob using the BRIN?
#            pass

            # we're going to open this blob
            self._statistics.count_data_blobs_read += 1

            # Read the blob from storage, it's just a stream of bytes at this point
            blob_bytes = self._reader.read_blob(blob_name)

            # record the number of bytes we're reading
            self._statistics.bytes_read_data += blob_bytes.getbuffer().nbytes

            # interpret the raw bytes into entries
            pyarrow_blob = decoder(blob_bytes, self._projection)

            # we should know the number of entries
            self._statistics.rows_read += pyarrow_blob.num_rows
            self._statistics.bytes_processed_data += pyarrow_blob.nbytes

            # yield this blob
            print(f"reader yielding {blob_name} {pyarrow_blob.shape}")
            yield pyarrow_blob

