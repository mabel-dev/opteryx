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

import pyarrow
from orso.schema import RelationSchema

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.utils.file_decoders import get_decoder


class DirectDiskConnector(BaseConnector):
    __mode__ = "Blob"

    @property
    def interal_only(self):
        return True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.decoder = get_decoder(self.dataset)

    def read_dataset(self) -> pyarrow.Table:
        with open(self.dataset, mode="br") as file:
            return iter([self.decoder(file)])

    def get_dataset_schema(self) -> RelationSchema:
        with open(self.dataset, mode="br") as file:
            return self.decoder(file, just_schema=True)
