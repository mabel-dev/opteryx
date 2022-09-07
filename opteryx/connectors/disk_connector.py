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

import io
import os
from opteryx.connectors import BaseBlobStorageAdapter


class DiskConnector(BaseBlobStorageAdapter):
    def read_blob(self, blob_name):

        with open(blob_name, "rb") as blob:
            # wrap in a BytesIO so we can close the file
            return io.BytesIO(blob.read())

    def get_blob_list(self, partition):
        import glob

        files = glob.glob(str(partition / "**"), recursive=True)
        return (f for f in files if os.path.isfile(f))
