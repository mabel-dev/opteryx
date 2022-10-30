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

This Node reads and parses blob (and file) stores into a PyArrow Table.

Blob stores have some features not available to other stores such as caching.
"""
import datetime
import time

from typing import Iterable
from enum import Enum
from cityhash import CityHash64

import pyarrow

from opteryx import config
from opteryx.exceptions import DatasetNotFoundError
from opteryx.managers.schemes import MabelPartitionScheme
from opteryx.managers.schemes import DefaultPartitionScheme
from opteryx.models import Columns, QueryProperties, ExecutionTree
from opteryx.operators import BasePlanNode
from opteryx.shared import BufferPool
from opteryx.utils import file_decoders


class ExtentionType(str, Enum):
    """labels for the file extentions"""

    DATA = "DATA"
    CONTROL = "CONTROL"


def do_nothing(stream, projection=None):
    return stream


MAX_SIZE_SINGLE_CACHE_ITEM = config.MAX_SIZE_SINGLE_CACHE_ITEM
PARTITION_SCHEME = config.PARTITION_SCHEME
MAX_CACHE_EVICTIONS = config.MAX_CACHE_EVICTIONS

BUFFER_POOL = BufferPool()

KNOWN_EXTENSIONS = {
    "complete": (do_nothing, ExtentionType.CONTROL),
    "ignore": (do_nothing, ExtentionType.CONTROL),
    "arrow": (file_decoders.arrow_decoder, ExtentionType.DATA),  # feather
    "csv": (file_decoders.csv_decoder, ExtentionType.DATA),
    "jsonl": (file_decoders.jsonl_decoder, ExtentionType.DATA),
    "orc": (file_decoders.orc_decoder, ExtentionType.DATA),
    "parquet": (file_decoders.parquet_decoder, ExtentionType.DATA),
    "zstd": (file_decoders.zstd_decoder, ExtentionType.DATA),  # jsonl/zstd
}


def _normalize_to_types(table):
    """
    Normalize types e.g. all numbers are decimal128 and dates
    """
    schema = table.schema

    for index, column_name in enumerate(schema.names):
        type_name = str(schema.types[index])
        if type_name in ("date32[day]", "date64", "timestamp[s]", "timestamp[ms]"):
            schema = schema.set(
                index,
                pyarrow.field(
                    name=column_name,
                    type=pyarrow.timestamp("us"),
                    metadata=table.field(column_name).metadata,
                ),
            )
        if type_name == ("list<item: null>"):
            schema = schema.set(
                index,
                pyarrow.field(
                    name=column_name,
                    type=pyarrow.list_(pyarrow.string()),
                    metadata=table.field(column_name).metadata,
                ),
            )

    return table.cast(target_schema=schema), schema


class BlobReaderNode(BasePlanNode):

    _disable_cache = False

    def __init__(self, properties: QueryProperties, **config):
        """
        The Blob Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        super().__init__(properties=properties)

        today = datetime.datetime.utcnow().date()

        self._dataset: str = config.get("dataset", None)
        self._alias: list = config.get("alias", None)

        if isinstance(self._dataset, (list, ExecutionTree, dict)):
            return

        self._dataset = self._dataset.replace(".", "/") + "/"
        self._reader = config.get("reader")()  # type:ignore

        # WITH hint can turn off caching
        self._disable_cache = "NO_CACHE" in config.get("hints", [])
        if self._disable_cache:
            self._cache = None  # type:ignore
        else:
            self._cache = config.get("cache")  # type:ignore

        # WITH hint can turn off partitioning, oatherwise get it from config
        if "NO_PARTITION" in config.get("hints", []) or PARTITION_SCHEME is None:
            self._partition_scheme = DefaultPartitionScheme("")  # type:ignore
        elif PARTITION_SCHEME == "mabel":
            self._partition_scheme = MabelPartitionScheme()  # type:ignore
        else:
            self._partition_scheme = DefaultPartitionScheme(
                PARTITION_SCHEME
            )  # type:ignore

        self._start_date = config.get("start_date", today)
        self._end_date = config.get("end_date", today)

        # pushed down selection/filter
        if "NO_PUSH_PROJECTION" in config.get("hints", []):
            self._selection: Iterable = None
        else:
            self._selection = config.get("selection")
            if isinstance(self._selection, list):
                self._selection = set(self._selection)

        # this is the list of blobs we're going to read
        self._reading_list: dict = self._scanner()

        # row count estimate
        self._row_count_estimate: int = None

    @property
    def config(self):  # pragma: no cover
        use_cache = ""
        if self._disable_cache:
            use_cache = " (NO_CACHE)"
        if self._alias:
            return f"{self._dataset} => {self._alias}{use_cache}"
        if isinstance(self._dataset, str):
            return f"{self._dataset}{use_cache}"
        return "<complex dataset>"

    @property
    def name(self):  # pragma: no cover
        return "Blob Reader"

    def execute(self) -> Iterable:

        # This is here for legacy reasons and should be moved to a DAG rather than
        # a Node (as per the other reader nodes)
        if isinstance(self._dataset, ExecutionTree):
            metadata = None

            for table in self._dataset.execute():
                if metadata is None:
                    metadata = Columns(table)
                    metadata.rename_table(self._alias)
                table = metadata.apply(table)
                yield table

            return

        metadata = None
        schema = None

        if not metadata:

            for partition in self._reading_list.values():

                # we're reading this partition now
                self.statistics.partitions_read += 1

                for (time_to_read, blob_bytes, pyarrow_blob, path,) in [
                    self._read_and_parse(
                        (
                            path,
                            self._reader.read_blob,
                            parser,
                            self._cache,
                            self._selection,
                        )
                    )
                    for path, parser in partition["blob_list"]
                ]:

                    # we've read a blob
                    self.statistics.count_data_blobs_read += 1

                    # extract stats from reader
                    self.statistics.bytes_read_data += blob_bytes
                    self.statistics.time_data_read += time_to_read

                    # we should know the number of entries
                    self.statistics.rows_read += pyarrow_blob.num_rows
                    self.statistics.bytes_processed_data += pyarrow_blob.nbytes

                    if self._row_count_estimate is None:
                        # This is really rough - it assumes all of the blobs have about
                        # the same number of records, which is almost never correct.
                        self._row_count_estimate = pyarrow_blob.num_rows * (
                            self.statistics.count_blobs_found
                            - self.statistics.count_blobs_ignored_frames
                            - self.statistics.count_control_blobs_found
                            - self.statistics.count_unknown_blob_type_found
                        )

                    if metadata is None:
                        pyarrow_blob = Columns.create_table_metadata(
                            table=pyarrow_blob,
                            expected_rows=self._row_count_estimate,
                            name=self._dataset.replace("/", ".")[:-1],
                            table_aliases=[self._alias],
                        )
                        metadata = Columns(pyarrow_blob)
                        self.statistics.columns_read += len(pyarrow_blob.column_names)
                    else:
                        try:
                            pyarrow_blob = metadata.apply(pyarrow_blob, source=path)
                        except:  # pragma:no cover

                            self.statistics.read_errors += 1

                            import pyarrow

                            pyarrow_blob = pyarrow.Table.from_pydict(
                                pyarrow_blob.to_pydict()
                            )
                            pyarrow_blob = metadata.apply(pyarrow_blob)

                    # if we've never run before, collect the schema
                    if schema is None:
                        schema = pyarrow_blob.schema
                    else:
                        # remove unwanted columns
                        pyarrow_blob = pyarrow_blob.select(
                            [
                                name
                                for name in schema.names
                                if name in pyarrow_blob.schema.names
                            ]
                        )

                    pyarrow_blob, schema = _normalize_to_types(pyarrow_blob)

                    # yield this blob
                    yield pyarrow_blob

    def _read_and_parse(self, config):
        path, reader, parser, cache, projection = config
        start_read = time.time_ns()

        # hash the blob name for the look up
        blob_hash = format(CityHash64(path), "X")
        # try to read the cache
        try:
            blob_bytes = None
            if not self._disable_cache:
                blob_bytes = BUFFER_POOL.get(blob_hash, cache)
        except Exception as e:  # pragma: no cover
            print(e)
            cache = None
            blob_bytes = None

        # if the item was a miss, get it from storage and add it to the cache
        if blob_bytes is None:  # pragma: no cover
            if not self._disable_cache:
                self.statistics.cache_misses += 1
            blob_bytes = reader(path)
            # limit the number of evictions a single query can do to prevent
            # sequential floods of the cache
            if (
                cache
                and (self.statistics.cache_evictions < MAX_CACHE_EVICTIONS)
                and (blob_bytes.getbuffer().nbytes < MAX_SIZE_SINGLE_CACHE_ITEM)
                and (not self._disable_cache)
            ):
                try:
                    evicted = BUFFER_POOL.set(blob_hash, blob_bytes, cache)
                    if evicted is not None:
                        self.statistics.cache_evictions += 1
                except (ConnectionResetError, BrokenPipeError):  # pragma: no-cover
                    self.statistics.cache_errors += 1
            elif cache:  # pragma: no-cover
                self.statistics.cache_oversize += 1
            else:  # pragma: no-cover
                self.statistics.cache_errors += 1
        else:
            self.statistics.cache_hits += 1

        table = parser(blob_bytes, projection)

        time_to_read = time.time_ns() - start_read
        return time_to_read, blob_bytes.getbuffer().nbytes, table, path

    def _scanner(self):
        """
        The scanner works out what blobs/files should be read
        """
        # datasets from storage
        partitions = self._reader.get_partitions(
            dataset=self._dataset,
            partitioning=self._partition_scheme.partition_format(),
            start_date=self._start_date,
            end_date=self._end_date,
        )

        self.statistics.partitions_found += len(partitions)

        partition_structure: dict = {}

        # Build the list of blobs we're going to read and collect summary statistics
        # so we can use them for decisions later.

        for partition in partitions:

            partition_structure[partition] = {}
            partition_structure[partition]["blob_list"] = []
            self.statistics.partitions_scanned += 1

            # Get a list of all of the blobs in the partition.
            time_scanning_partitions = time.time_ns()
            blob_list = self._reader.get_blob_list(partition)
            self.statistics.time_scanning_partitions = (
                time.time_ns() - time_scanning_partitions
            )

            # remove folders, that's items ending with '/'
            blob_list = [blob for blob in blob_list if not blob.endswith("/")]

            # Track how many blobs we found
            count_blobs_found = len(blob_list)
            self.statistics.count_blobs_found += count_blobs_found

            # Filter the blob list to just the frame we're interested in
            if self._partition_scheme is not None:
                blob_list = self._partition_scheme.filter_blobs(
                    blob_list, self.statistics
                )
                self.statistics.count_blobs_ignored_frames += count_blobs_found - len(
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
                    self.statistics.count_control_blobs_found += 1
                else:
                    self.statistics.count_unknown_blob_type_found += 1

            if len(partition_structure[partition]["blob_list"]) == 0:
                partition_structure.pop(partition)

        if len(partition_structure) == 0:
            raise DatasetNotFoundError(
                f"The requested dataset could not be found {str(partition).replace('/', '.')}."
            )

        return partition_structure
