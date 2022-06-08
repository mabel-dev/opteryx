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
Blob Reader Node

This is a SQL Query Execution Plan Node.

This Node reads and parses the data from blob, object and file stores such as local
disk, GCS or Minio, into PyArrow Tables.
"""
import datetime

from enum import Enum
from typing import Iterable, Optional
from cityhash import CityHash64

import time

from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.exceptions import DatabaseError, SqlError
from opteryx.storage import file_decoders
from opteryx.storage.adapters import DiskStorage
from opteryx.storage.schemes import MabelPartitionScheme
from opteryx.utils.columns import Columns


class ExtentionType(str, Enum):
    """labels for the file extentions"""

    DATA = "DATA"
    CONTROL = "CONTROL"


do_nothing = lambda x, y: x


KNOWN_EXTENSIONS = {
    "complete": (do_nothing, ExtentionType.CONTROL),
    "ignore": (do_nothing, ExtentionType.CONTROL),
    "arrow": (file_decoders.arrow_decoder, ExtentionType.DATA),  # feather
    "jsonl": (file_decoders.jsonl_decoder, ExtentionType.DATA),
    "orc": (file_decoders.orc_decoder, ExtentionType.DATA),
    "parquet": (file_decoders.parquet_decoder, ExtentionType.DATA),
    "zstd": (file_decoders.zstd_decoder, ExtentionType.DATA),  # jsonl/zstd
}


class BlobReaderNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        """
        The Blob Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        super().__init__(statistics=statistics, **config)

        from opteryx.engine.planner.planner import QueryPlanner

        today = datetime.datetime.utcnow().date()

        self._statistics = statistics
        self._alias, self._dataset = config.get("dataset", [None, None])

        self._dataset = self._dataset.replace(".", "/") + "/"
        self._reader = config.get("reader", DiskStorage())
        self._cache = config.get("cache")
        self._partition_scheme = config.get("partition_scheme", MabelPartitionScheme())

        self._start_date = config.get("start_date", today)
        self._end_date = config.get("end_date", today)

        # pushed down projection
        self._projection = config.get("projection")
        # pushed down selection
        self._selection = config.get("selection")

    @property
    def config(self):  # pragma: no cover
        if self._alias:
            return f"{self._dataset} => {self._alias}"
        if isinstance(self._dataset, str):
            return self._dataset
        return "<complex dataset>"

    @property
    def name(self):  # pragma: no cover
        return "Blob Dataset Reader"

    def execute(self, data_pages: Optional[Iterable]) -> Iterable:

        # datasets from storage
        partitions = self._reader.get_partitions(
            dataset=self._dataset,
            partitioning=self._partition_scheme.partition_format(),
            start_date=self._start_date,
            end_date=self._end_date,
        )

        self._statistics.partitions_found += len(partitions)

        partition_structure: dict = {}
        expected_rows = 0

        # Build the list of blobs we're going to read and collect summary statistics
        # so we can use them for decisions later.
        for partition in partitions:

            partition_structure[partition] = {}
            partition_structure[partition]["blob_list"] = []
            self._statistics.partitions_scanned += 1

            # Get a list of all of the blobs in the partition.
            time_scanning_partitions = time.time_ns()
            blob_list = self._reader.get_blob_list(partition)
            self._statistics.time_scanning_partitions = (
                time.time_ns() - time_scanning_partitions
            )

            # remove folders, that's items ending with '/'
            blob_list = [blob for blob in blob_list if not blob.endswith("/")]

            # Track how many blobs we found
            count_blobs_found = len(blob_list)
            self._statistics.count_blobs_found += count_blobs_found

            # Filter the blob list to just the frame we're interested in
            if self._partition_scheme is not None:
                blob_list = self._partition_scheme.filter_blobs(
                    blob_list, self._statistics
                )
                self._statistics.count_blobs_ignored_frames += count_blobs_found - len(
                    blob_list
                )

            for blob_name in blob_list:

                # the the blob filename extension
                extension = blob_name.split(".")[-1]

                # find out how to read this blob
                decoder, file_type = KNOWN_EXTENSIONS.get(extension, (None, None))

                if file_type == ExtentionType.DATA:
                    partition_structure[partition]["blob_list"].append(
                        (
                            blob_name,
                            decoder,
                        )
                    )
                elif file_type == ExtentionType.CONTROL:
                    self._statistics.count_control_blobs_found += 1
                else:
                    self._statistics.count_unknown_blob_type_found += 1

        metadata = None

        import pyarrow.plasma as plasma
        from opteryx.storage import multiprocessor
        from opteryx import config

        with plasma.start_plasma_store(
            config.BUFFER_PER_SUB_PROCESS * config.MAX_SUB_PROCESSES
        ) as plasma_store:
            plasma_channel = plasma_store[0]

            for partition in partitions:

                # we're reading this partition now
                if len(blob_list) > 0:
                    self._statistics.partitions_read += 1

                def _read_and_parse(config):
                    path, reader, parser, cache = config

                    # print(f"start {path}")

                    start_read = time.time_ns()

                    # if we have a cache set
                    if cache:
                        # hash the blob name for the look up
                        blob_hash = format(CityHash64(path), "X")
                        # try to read the cache
                        blob_bytes = cache.get(blob_hash)

                        # if the item was a miss, get it from storage and add it to the cache
                        if blob_bytes is None:
                            self._statistics.cache_misses += 1
                            blob_bytes = reader(path)
                            cache.set(blob_hash, blob_bytes)
                        else:
                            self._statistics.cache_hits += 1
                    else:
                        blob_bytes = reader(path)

                    table = parser(blob_bytes, None)

                    # print(f"read  {path} - {(time.time_ns() - start_read) / 1e9}")

                    time_to_read = time.time_ns() - start_read
                    return time_to_read, blob_bytes.getbuffer().nbytes, table, path

                for (
                    time_to_read,
                    blob_bytes,
                    pyarrow_blob,
                    path,
                ) in multiprocessor.processed_reader(
                    _read_and_parse,
                    [
                        (path, self._reader.read_blob, parser, self._cache)
                        for path, parser in partition_structure[partition]["blob_list"]
                    ],
                    plasma_channel,
                ):

                    # we're going to open this blob
                    self._statistics.count_data_blobs_read += 1

                    # extract stats from reader
                    self._statistics.bytes_read_data += blob_bytes
                    self._statistics.time_data_read += time_to_read

                    # we should know the number of entries
                    self._statistics.rows_read += pyarrow_blob.num_rows
                    self._statistics.bytes_processed_data += pyarrow_blob.nbytes

                    if metadata is None:
                        pyarrow_blob = Columns.create_table_metadata(
                            table=pyarrow_blob,
                            expected_rows=expected_rows,
                            name=self._dataset.replace("/", ".")[:-1],
                            table_aliases=[self._alias],
                        )
                        metadata = Columns(pyarrow_blob)
                    else:
                        try:
                            pyarrow_blob = metadata.apply(pyarrow_blob, source=path)
                        except:

                            self._statistics.read_errors += 1

                            import pyarrow

                            pyarrow_blob = pyarrow.Table.from_pydict(
                                pyarrow_blob.to_pydict()
                            )
                            pyarrow_blob = metadata.apply(pyarrow_blob)

                    # yield this blob
                    yield pyarrow_blob
