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
The file connector provides the reader for when a file name is provided as the
dataset name in a query.
"""
from typing import Optional

import pyarrow
from orso.schema import RelationSchema

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.exceptions import DatasetNotFoundError
from opteryx.utils.file_decoders import get_decoder


class FileConnector(BaseConnector):
    __mode__ = "Blob"
    _byte_array: Optional[bytes] = None  # Instance attribute to store file bytes

    @property
    def interal_only(self):
        return True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if ".." in self.dataset or self.dataset[0] in ("/", "~"):
            # Don't find any datasets which look like path traversal
            raise DatasetNotFoundError(dataset=self.dataset)
        self.decoder = get_decoder(self.dataset)

    def _read_file(self) -> None:
        """
        Reads the dataset file and stores its content in _byte_array attribute.
        """
        if self._byte_array is None:
            with open(self.dataset, mode="br") as file:
                self._byte_array = bytes(file.read())

    def read_dataset(self, columns: list = None) -> pyarrow.Table:
        """
        Reads the dataset file and decodes it.

        Returns:
            An iterator containing a single decoded pyarrow.Table.
        """
        self._read_file()
        return iter([self.decoder(self._byte_array, projection=columns)])

    def get_dataset_schema(self) -> RelationSchema:
        """
        Retrieves the schema from the dataset file.

        Returns:
            The schema of the dataset.
        """
        if self.schema is not None:
            return self.schema

        self._read_file()
        self.schema = self.decoder(self._byte_array, just_schema=True)
        return self.schema
