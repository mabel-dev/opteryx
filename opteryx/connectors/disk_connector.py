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
Disk Connector reads files from a locally addressable file system (local disk,
NFS etc).
"""

import io
import os

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.base.base_connector import DatasetReader
from orso.schema import RelationSchema
from opteryx.utils import file_decoders
from opteryx.utils import paths


class DiskConnector(BaseConnector):
    __mode__ = "blob"

    def get_dataset_schema(self, dataset_name: str) -> RelationSchema:
        pass

    def read_dataset(self, dataset_name: str) -> DatasetReader:
        if not paths.is_file(dataset_name):
            raise Exception(f"{dataset_name} is not a file")
        parts = paths.get_parts(dataset_name)
        with open(dataset_name, "rb") as blob:
            # wrap in a BytesIO so we can close the file
            return io.BytesIO(blob.read())

    def get_blob_list(self, partition):
        import glob

        files = glob.glob(str(partition / "**"), recursive=True)
        return [str(f).replace("\\", "/") for f in files if os.path.isfile(f)]
