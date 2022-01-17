"""
Partition Reader Node

This is a SQL Query Execution Plan Node.

This Node reads and parses the data from a partition into a Relation.

We plan to do the following:

- Pass the columns needed by any other part of the query so we can apply a projection
  at the point of reading.
- Pass the columns used in selections, so we can pass along, any index information we
  have.
- Use BRIN and selections to filter out blobs from being read that don't contain
  records which can match the selections. 
- Pass along statistics about the read so it can be logged for analysis and debugging.

"""
from enum import Enum
from typing import Optional
from pyarrow import concat_tables

from opteryx import Relation
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.engine.reader_statistics import ReaderStatistics
from opteryx.storage import file_decoders
from opteryx.storage.adapters.local.disk_store import DiskStorage
from opteryx.utils import paths


class EXTENSION_TYPE(str, Enum):
    # labels for the file extentions
    DATA = "DATA"
    CONTROL = "CONTROL"
    INDEX = "INDEX"


do_nothing = lambda x: x

KNOWN_EXTENSIONS = {
    ".complete": (do_nothing, EXTENSION_TYPE.CONTROL),
    ".ignore": (do_nothing, EXTENSION_TYPE.CONTROL),
    ".index": (do_nothing, EXTENSION_TYPE.INDEX),
    ".jsonl": (file_decoders.jsonl_decoder, EXTENSION_TYPE.DATA),
    ".metadata": (do_nothing, EXTENSION_TYPE.CONTROL),
    ".orc": (file_decoders.orc_decoder, EXTENSION_TYPE.DATA),
    ".parquet": (file_decoders.parquet_decoder, EXTENSION_TYPE.DATA),
    ".zstd": (file_decoders.zstd_decoder, EXTENSION_TYPE.DATA),
}


class PartitionReaderNode(BasePlanNode):
    def __init__(self, **config):
        """
        The Partition Reader Node is responsible for reading a complete partition
        and returning a Relation.
        """
        self._partition = config.get("partition", "").replace(".", "/") + "/"
        self._reader = config.get("reader", DiskStorage())

        # pushed down projection
        self._projection = config.get("projection")
        # pushed down selection
        self._selection = config.get("selection")

    def __repr__(self):
        return self._partition

    def execute(self, relation: Relation = None) -> Optional[Relation]:

        # Create a statistics object to record what happens
        stats = ReaderStatistics()

        # Create the container for the resultant dataset
        pyarrow_blobs = []

        # Get a list of all of the blobs in the partition.
        pass
        import glob

        blob_list = glob.glob(self._partition + "**", recursive=True)

        # Work out which frame we should read.
        pass

        # If there's a zonemap for the partition, read it
        if any(blob.endswith("frame.metadata") for blob in blob_list):
            # read the zone map into a dictionary
            zonemap = {}
        else:
            # create an empty zone map
            zonemap = {}

        # Filter the blob list to just the frame we're interested in
        pass

        for blob_name in blob_list:

            # work out the parts of the blob name
            bucket, path, stem, extension = paths.get_parts(blob_name)

            # find out how to read this blob
            decoder, file_type = KNOWN_EXTENSIONS.get(extension, (None, None))
            # if it's not a known data file, skip reading it
            if file_type != EXTENSION_TYPE.DATA:
                continue

            # we have a data blob, add it to the stats
            stats.count_data_blobs_found += 1

            # can we eliminate this blob using the BRIN?
            pass

            # we're going to open this blob
            stats.count_data_blobs_read += 1

            # Read the blob from storage, it's just a stream of bytes at this point
            blob_bytes = self._reader.read_blob(blob_name)

            # record the number of bytes we're reading
            stats.bytes_read_data += blob_bytes.getbuffer().nbytes

            # interpret the raw bytes into entries
            pyarrow_blob = decoder(blob_bytes, self._projection)

            # we should know the number of entries
            stats.rows_read += pyarrow_blob.num_rows

            # add this blob to the set to be returned
            pyarrow_blobs.append(pyarrow_blob)

        return concat_tables(pyarrow_blobs)
