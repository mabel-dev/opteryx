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
Dataset Reader Node

This is a SQL Query Execution Plan Node.

This Node reads and parses the data from a dataset into a Table.

We plan to do the following:

USE THE ZONEMAP:
- Respond to simple aggregations using the zonemap, such as COUNT(*)
- Use BRIN and selections to filter out blobs from being read that don't contain
  records which can match the selections.

PARALLELIZE READING:
- As one blob is read, the next is immediately cached for reading
"""
import datetime
import time
from enum import Enum
from typing import Iterable, Optional

from cityhash import CityHash64
from orjson import loads

from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.exceptions import DatabaseError
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


def _get_sample_dataset(dataset, alias):
    # we do this like this so the datasets are not loaded into memory unless
    # they are going to be used
    from opteryx import samples

    sample_datasets = {
        "$satellites/": samples.satellites,
        "$planets/": samples.planets,
        "$astronauts/": samples.astronauts,
        "$no_table/": samples.no_table,
    }
    dataset = dataset.lower()
    if dataset in sample_datasets:
        table = sample_datasets[dataset]()
        table = Columns.create_table_metadata(
            table=table,
            expected_rows=table.num_rows,
            name=dataset[:-1],
            table_aliases=[alias],
        )
        return table
    raise DatabaseError(f"Dataset not found `{dataset}`.")


class DatasetReaderNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        """
        The Dataset Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        super().__init__(statistics=statistics, **config)

        from opteryx.engine.planner.planner import QueryPlanner

        today = datetime.date.today()

        self._statistics = statistics
        self._alias, self._dataset = config.get("dataset", [None, None])

        if isinstance(self._dataset, (list, QueryPlanner)):
            return

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
    def config(self):
        if self._alias:
            return f"{self._dataset} => {self._alias}"
        if isinstance(self._dataset, str):
            return self._dataset
        return "<complex dataset>"

    @property
    def name(self):
        return "Reader"

    def execute(self, data_pages: Optional[Iterable]) -> Iterable:

        from opteryx.engine.planner.planner import QueryPlanner

        # literal datasets
        if isinstance(self._dataset, list):
            import pyarrow

            table = pyarrow.Table.from_pylist(self._dataset)
            table = Columns.create_table_metadata(
                table=table,
                expected_rows=table.num_rows,
                name=self._alias,
                table_aliases=[self._alias],
            )

            yield table
            return

        # query plans
        if isinstance(self._dataset, QueryPlanner):
            yield from self._dataset.execute()
            return

        # sample datasets
        if self._dataset[0] == "$":
            yield _get_sample_dataset(self._dataset, self._alias)
            return

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
            blob_list = self._reader.get_blob_list(partition)

            # remove folders, that's items ending with '/'
            blob_list = [blob for blob in blob_list if not blob.endswith("/")]

            # Track how many blobs we found
            count_blobs_found = len(blob_list)
            self._statistics.count_blobs_found += count_blobs_found

            # Filter the blob list to just the frame we're interested in
            if self._partition_scheme is not None:
                blob_list = self._partition_scheme.filter_blobs(blob_list)
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

        for partition in partitions:

            # we're reading this partition now
            if len(blob_list) > 0:
                self._statistics.partitions_read += 1

            for blob_name, decoder in partition_structure[partition]["blob_list"]:

                # we're going to open this blob
                self._statistics.count_data_blobs_read += 1

                start_read = time.time_ns()

                # Read the blob from storage, it's just a stream of bytes at this point

                # if we have a cache set
                if self._cache:
                    # hash the blob name for the look up
                    blob_hash = format(CityHash64(blob_name), "X")
                    # try to read the cache
                    blob_bytes = self._cache.get(blob_hash)

                    # if the item was a miss, get it from storage and add it to the cache
                    if blob_bytes is None:
                        self._statistics.cache_misses += 1
                        blob_bytes = self._reader.read_blob(blob_name)
                        self._cache.set(blob_hash, blob_bytes)
                    else:
                        self._statistics.cache_hits += 1
                else:
                    blob_bytes = self._reader.read_blob(blob_name)

                # record the number of bytes we're reading
                self._statistics.bytes_read_data += blob_bytes.getbuffer().nbytes

                # interpret the raw bytes into entries
                pyarrow_blob = decoder(blob_bytes, self._projection)  # type:ignore

                self._statistics.time_data_read += time.time_ns() - start_read

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
                        pyarrow_blob = metadata.apply(pyarrow_blob)
                    except:

                        self._statistics.read_errors += 1

                        import pyarrow

                        pyarrow_blob = pyarrow.Table.from_pydict(
                            pyarrow_blob.to_pydict()
                        )
                        pyarrow_blob = metadata.apply(pyarrow_blob)

                # yield this blob
                yield pyarrow_blob
