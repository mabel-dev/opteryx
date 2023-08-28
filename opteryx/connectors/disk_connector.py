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
The 'direct disk' connector provides the reader for when a dataset file is
given directly in a query.

As such it assumes 
"""
import os
from array import array
from typing import List

import pyarrow
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import single_item_cache

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Cacheable
from opteryx.connectors.capabilities import Partitionable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import EmptyDatasetError
from opteryx.exceptions import UnsupportedFileTypeError
from opteryx.utils.file_decoders import VALID_EXTENSIONS
from opteryx.utils.file_decoders import get_decoder


class DiskConnector(BaseConnector, Cacheable, Partitionable):
    __mode__ = "Blob"

    def __init__(self, **kwargs):
        BaseConnector.__init__(self, **kwargs)
        Partitionable.__init__(self, **kwargs)
        Cacheable.__init__(self, **kwargs)

        self.dataset = self.dataset.replace(".", "/")
        # we're going to cache the first blob as the schema and dataset reader
        # sometimes both start here
        self.cached_first_blob = None

    def read_blob(self, *, blob_name, **kwargs):
        with open(blob_name, mode="br") as file:
            file_stream = file.read()
        return array("B", file_stream)

    @single_item_cache
    def get_list_of_blob_names(self, *, prefix: str) -> List[str]:
        files = [
            os.path.join(root, file)
            for root, _, files in os.walk(prefix)
            for file in files
            if os.path.splitext(file)[1] in VALID_EXTENSIONS
        ]
        return files

    def read_dataset(self) -> pyarrow.Table:
        blob_names = self.partition_scheme.get_blobs_in_partition(
            start_date=self.start_date,
            end_date=self.end_date,
            blob_list_getter=self.get_list_of_blob_names,
            prefix=self.dataset,
        )

        # Check if the first blob was cached earlier
        if self.cached_first_blob is not None:
            yield self.cached_first_blob  # Use cached blob
            blob_names = blob_names[1:]  # Skip first blob
        self.cached_first_blob = None

        for blob_name in blob_names:
            try:
                decoder = get_decoder(blob_name)
                blob_bytes = self.read_blob(blob_name=blob_name, statistics=self.statistics)
                yield decoder(blob_bytes)
            except UnsupportedFileTypeError:
                pass

    def get_dataset_schema(self) -> RelationSchema:
        # Try to read the schema from the metastore
        self.schema = self.read_schema_from_metastore()
        if self.schema:
            return self.schema

        # Read first blob for schema inference and cache it
        record = next(self.read_dataset(), None)
        self.cached_first_blob = record

        if record is None:
            if os.path.isdir(self.dataset):
                raise EmptyDatasetError(dataset=self.dataset)
            raise DatasetNotFoundError(dataset=self.dataset)

        arrow_schema = record.schema

        self.schema = RelationSchema(
            name=self.dataset,
            columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
        )

        return self.schema
