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
from opteryx.config import DATASET_PREFIX_MAPPING
from opteryx.storage.adapters.blob import DiskStorage
from opteryx.storage.adapters.blob import GcsStorage
from opteryx.storage.adapters.blob import MinIoStorage

from .base_buffer_cache import BaseBufferCache
from .base_partition_scheme import BasePartitionScheme

WELL_KNOWN_ADAPTERS = {
    "disk": DiskStorage,
    "gcs": GcsStorage,
    "firestore": None,
    "minio": MinIoStorage,
    "mongodb": None,
    "s3": MinIoStorage,
}

_storage_prefixes = {}

if not isinstance(DATASET_PREFIX_MAPPING, dict):
    _storage_prefixes = {"_": "disk"}
else:
    for _prefix, _adapter_name in DATASET_PREFIX_MAPPING.items():
        _storage_prefixes[_prefix] = WELL_KNOWN_ADAPTERS.get(
            _adapter_name.lower(), None
        )


def register_prefix(prefix, adapter):
    """add a prefix"""
    _storage_prefixes[prefix] = adapter


def get_adapter(dataset):
    prefix = dataset.split(".")[0]
    if prefix in _storage_prefixes:
        return _storage_prefixes[prefix]
    return _storage_prefixes.get("_", None)
