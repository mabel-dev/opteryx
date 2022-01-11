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

from opteryx import Relation
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.engine.reader_statistics import ReaderStatistics
from opteryx.storage import file_decoders

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

    def __init__(self, config):
        """
        The Partition Reader Node is responsible for reading a complete partition
        and returning a Relation.
        """

    def execute(self, relation: Relation = None) -> Optional[Relation]:

        stats = ReaderStatistics()

        # Get a list of all of the blobs in the partition.

        # Work out which frame we should read.

        # Filter the blob list to just the frame we're interested in

        # If there's a zonemap, read it
        if any(blob.endswith("frame.metadata") for blob in self.partition):
            pass
            # read the zone map into a dictionary
            zonemap = {}
        else:
            zonemap = {}

        for blob_name in self.partition:

            # find out how to read this blob
            decoder, file_type = KNOWN_EXTENSIONS.get(
                extention, (None, None, None)
            )
            if file_type != EXTENSION_TYPE.DATA:
                # if it's not a data file, skip reading it
                continue

            # we have a data blob, we may not actually read it
            stats.total_data_blobs += 1

            # can we eliminate this blob using the BRIN?

            # get the list of columns and types for this blob

            bucket, path, stem, extention = paths.get_parts(blob_name)

            # we're going to open this blob
            stats.data_blobs_read += 1

            # Read the blob from storage, it's just a stream of bytes at this point
            blob_bytes = self.reader.read_blob(blob_name)
            stats.data_bytes_read += len(blob_bytes)

            # interpret the raw bytes into entries, these may not be records yet
            # push down the projection
            schema, record_iterator = decompressor(blob_bytes)

            # we should know the number of entries
            stats.data_rows_read += 1  # TODO

            # interpret the entries into records
        #    record_iterator = map(parser, record_iterator)

            # if we don't have a min/max index, create one
        #    min_max_index = IndexMinMax().build(record_iterator)

            return stats, schema, record_iterator
