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
The 'sample' connector provides readers for the internal sample datasets,
$planets, $astronauts, and $satellites.

- $no_table is used in queries where there is no relation specified 'SELECT 1'
- $derived is used as a schema to align virtual columns to
"""

import typing

import pyarrow
from orso.schema import RelationSchema

from opteryx import samples
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.base.base_connector import DatasetReader
from opteryx.exceptions import DatasetNotFoundError


def get_decoder(dataset):
    from opteryx.exceptions import UnsupportedFileTypeError
    from opteryx.utils import file_decoders

    ext = dataset.split(".")[-1].lower()
    if ext not in file_decoders.KNOWN_EXTENSIONS:
        raise UnsupportedFileTypeError(f"Unsupported file type - {ext}")
    file_decoder, file_type = file_decoders.KNOWN_EXTENSIONS[ext]
    if file_type != file_decoders.ExtentionType.DATA:
        raise UnsupportedFileTypeError(f"File is not a data file - {ext}")
    return file_decoder


class LocalDiskConnector(BaseConnector):
    __mode__ = "Blob"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.decoder = get_decoder(self.dataset)

    def read_dataset(self) -> pyarrow.Table:
        with open(self.dataset, mode="br") as file:
            return iter([self.decoder(file)])

    def get_dataset_schema(self) -> RelationSchema:
        with open(self.dataset, mode="br") as file:
            return self.decoder(file, just_schema=True)
