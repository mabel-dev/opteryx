"""
Partition Reader Node

This is a SQL Query Execution Plan Node.

This Node reads and parses the data from a blob into a Relation.

If relevant Indexes exist we will try to elininate rows from the read if a
selection has been pushed down.

We create BRIN (Min/Max) indexes for columns we're going to filter on - we won't
perform full profiling as this is currently too slow for real-time use. 
"""
from enum import Enum
from mabel import Relation
from mabel.data.internals.data_containers import relation
from mabel.data.readers.internals.reader_statistics import ReaderStatistics
from mabel.data.readers.sql.planner.operations import BasePlanNode
from mabel.data.readers.internals import decompressors, parsers
from mabel.data.readers.internals.parallel_reader import empty_list
from mabel.utils import paths


class EXTENSION_TYPE(str, Enum):
    # labels for the file extentions
    DATA = "DATA"
    CONTROL = "CONTROL"
    INDEX = "INDEX"


KNOWN_EXTENSIONS = {
    ".complete": (empty_list, empty_list, EXTENSION_TYPE.CONTROL),
    ".csv": (decompressors.csv, parsers.pass_thru, EXTENSION_TYPE.DATA),
    ".ignore": (empty_list, empty_list, EXTENSION_TYPE.CONTROL),
    ".index": (empty_list, empty_list, EXTENSION_TYPE.INDEX),
    ".json": (decompressors.block, parsers.json, EXTENSION_TYPE.DATA),
    ".jsonl": (decompressors.lines, parsers.json, EXTENSION_TYPE.DATA),
    ".lxml": (decompressors.lines, parsers.xml, EXTENSION_TYPE.DATA),
    ".lzma": (decompressors.lzma, parsers.json, EXTENSION_TYPE.DATA),
    ".metadata": (empty_list, empty_list, EXTENSION_TYPE.CONTROL),
    ".orc": (decompressors.orc, parsers.pass_thru, EXTENSION_TYPE.DATA),
    ".parquet": (decompressors.parquet, parsers.pass_thru, EXTENSION_TYPE.DATA),
    ".txt": (decompressors.block, parsers.pass_thru, EXTENSION_TYPE.DATA),
    ".xml": (decompressors.block, parsers.xml, EXTENSION_TYPE.DATA),
    ".zip": (decompressors.unzip, parsers.pass_thru, EXTENSION_TYPE.DATA),
    ".zstd": (decompressors.zstd, parsers.json, EXTENSION_TYPE.DATA),
}


class PartitionReaderNode(BasePlanNode):
    def __init__(self, **kwargs):
        """
        The Partition Reader Node is responsible for reading a complete partition
        and returning a Relation.

        Parameters:
            partition: list or tuple
                The files
            projection: list or tuple
                The list of columns to return
            selection: list or tuple
                DNF formatted query
        """
        self.partition = kwargs.get("partition", [])  # string
        self.projection = kwargs.get("projection", ["*"])  # tuple or list
        self.selection = kwargs.get("selection", [])  # in DNF

        # which reader to use

    def execute(self):

        stats = ReaderStatistics()

        # if there's a zonemap, read it
        if any(blob.endswith("frame.metadata") for blob in self.partition):
            pass
            # read the zone map into a dictionary
            zonemap = {}
        else:
            zonemap = {}

        for blob_name in self.partition:

            # find out how to read this blob
            decompressor, parser, file_type = KNOWN_EXTENSIONS.get(
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
            schema, record_iterator = decompressor(blob_bytes, projection)

            # we should know the number of entries
            stats.data_rows_read += 1  # TODO

            # interpret the entries into records
            record_iterator = map(parser, record_iterator)

            # if we weren't able to push down the projection, do it now
            # TODO

            # if we don't have a min/max index, create one
            min_max_index = IndexMinMax().build(record_iterator)

            return stats, schema, record_iterator
