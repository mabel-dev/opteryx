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
import time
import datetime
from enum import Enum
from typing import Iterable

import orjson
from opteryx.exceptions import DatabaseError
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.engine import QueryStatistics
from opteryx.storage import file_decoders
from opteryx.storage.adapters import DiskStorage
from opteryx.storage.schemes import MabelPartitionScheme


class EXTENSION_TYPE(str, Enum):
    # labels for the file extentions
    DATA = "DATA"
    CONTROL = "CONTROL"

do_nothing = lambda x, y: x

KNOWN_EXTENSIONS = {
    "complete": (do_nothing, EXTENSION_TYPE.CONTROL),
    "ignore": (do_nothing, EXTENSION_TYPE.CONTROL),
    "arrow": (file_decoders.arrow_decoder, EXTENSION_TYPE.DATA),  # feather
    "jsonl": (file_decoders.jsonl_decoder, EXTENSION_TYPE.DATA),
    "orc": (file_decoders.orc_decoder, EXTENSION_TYPE.DATA),
    "parquet": (file_decoders.parquet_decoder, EXTENSION_TYPE.DATA),
    "zstd": (file_decoders.zstd_decoder, EXTENSION_TYPE.DATA),  # jsonl/zstd
}


def _get_sample_dataset(dataset):
    # we do this like this so the datasets are not loaded into memory unless
    # they are going to be used
    from opteryx import samples

    SAMPLE_DATASETS = {
        "$satellites/": samples.satellites(),
        "$planets/": samples.planets(),
        "$astronauts/": samples.astronauts(),
        "$no_table/": samples.no_table(),
    }
    dataset = dataset.lower()
    if dataset in SAMPLE_DATASETS:
        return SAMPLE_DATASETS[dataset]
    raise DatabaseError(f"Dataset not found `{dataset}`.")

class DatasetReaderNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        """
        The Dataset Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        from opteryx.engine.planner.planner import QueryPlanner

        TODAY = datetime.date.today()

        self._statistics = statistics
        self._alias, self._dataset = config.get("dataset", [None, None])

        if isinstance(self._dataset, (bytearray, QueryPlanner)):
            return

        self._dataset = self._dataset.replace(".", "/") + "/"
        self._reader = config.get("reader", DiskStorage())
        self._partition_scheme = config.get("partition_scheme", MabelPartitionScheme())

        self._start_date = config.get("start_date", TODAY)
        self._end_date = config.get("end_date", TODAY)

        # pushed down projection
        self._projection = config.get("projection")
        # pushed down selection
        self._selection = config.get("selection")

    def __repr__(self):
        if self._alias:
            return f"{self._dataset} => {self._alias}"
        return self._dataset

    @property
    def name(self):
        return "Reader"

    def execute(self, data_pages: Iterable) -> Iterable:

        from opteryx.engine.planner.planner import QueryPlanner
        from opteryx.utils.arrow import set_metadata, create_table_metadata

        # literal datasets
        if isinstance(self._dataset, bytearray):
            import io
            import pyarrow.json

            yield pyarrow.json.read_json(io.BytesIO(self._dataset))
            return

        # query plans
        if isinstance(self._dataset, QueryPlanner):
            yield from self._dataset.execute()
            return

        # sample datasets
        if self._dataset[0] == "$":
            yield _get_sample_dataset(self._dataset)
            return

        # datasets from storage
        partitions = self._reader.get_partitions(
            dataset=self._dataset,
            partitioning=self._partition_scheme.partition_format(),
            start_date=self._start_date,
            end_date=self._end_date,
        )

        self._statistics.partitions_found += len(partitions)

        partition_structure:dict = {}
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

                if file_type == EXTENSION_TYPE.CONTROL:
                    self._statistics.count_control_blobs_read += 1
                    # read the control blob
                    control_blob = self._reader.read_blob(blob_name)
                    # record the number of bytes we're reading
                    self._statistics.bytes_read_control += control_blob.getbuffer().nbytes
                    try:
                        control_dict = {}
                        control_dict = orjson.loads(control_blob.read())
                        expected_rows += control_dict.get("records", 0)
                    except (ValueError, TypeError):
                        pass
                elif file_type == EXTENSION_TYPE.DATA:
                    partition_structure[partition]["blob_list"].append((blob_name, decoder,))
                else:
                    self._statistics.count_unknown_blob_type_found += 1 

        for partition in partitions:

            # we're reading this partition now
            if len(blob_list) > 0:
                self._statistics.partitions_read += 1

            for blob_name, decoder in partition_structure[partition]["blob_list"]:

                # we're going to open this blob
                self._statistics.count_data_blobs_read += 1

                start_read = time.time_ns()

                # Read the blob from storage, it's just a stream of bytes at this point
                blob_bytes = self._reader.read_blob(blob_name)

                # record the number of bytes we're reading
                self._statistics.bytes_read_data += blob_bytes.getbuffer().nbytes

                # interpret the raw bytes into entries
                pyarrow_blob = decoder(blob_bytes, self._projection)  # type:ignore

                self._statistics.time_data_read += time.time_ns() - start_read

                # we should know the number of entries
                self._statistics.rows_read += pyarrow_blob.num_rows
                self._statistics.bytes_processed_data += pyarrow_blob.nbytes

                # write dataset information to the metadata
                metadata = create_table_metadata(
                    pyarrow_blob.column_names,
                    expected_rows,
                    self._dataset.replace("/", ".")[:-1],
                    [self._alias]
                )
                pyarrow_blob = set_metadata(pyarrow_blob, metadata)

                # yield this blob
                yield pyarrow_blob
