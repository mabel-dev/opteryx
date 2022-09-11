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

from .base.base_document_storage_adapter import BaseDocumentStorageAdapter
from .base.base_blob_storage_adapter import BaseBlobStorageAdapter


from opteryx.config import DATASET_PREFIX_MAPPING
from opteryx.connectors.aws_s3_connector import AwsS3Connector
from opteryx.connectors.disk_connector import DiskConnector
from opteryx.connectors.gcp_firestore_connector import GcpFireStoreConnector
from opteryx.connectors.gcp_cloudstorage_connector import GcpCloudStorageConnector
from opteryx.connectors.mongodb_connector import MongoDbConnector

WELL_KNOWN_ADAPTERS = {
    "disk": DiskConnector,
    "gcs": GcpCloudStorageConnector,
    "firestore": GcpFireStoreConnector,
    "minio": AwsS3Connector,
    "mongodb": MongoDbConnector,
    "s3": AwsS3Connector,
}

_storage_prefixes = {}

if not isinstance(DATASET_PREFIX_MAPPING, dict):  # pragma: no cover
    _storage_prefixes = {"_": "disk"}
else:
    for _prefix, _adapter_name in DATASET_PREFIX_MAPPING.items():
        _storage_prefixes[_prefix] = WELL_KNOWN_ADAPTERS.get(
            _adapter_name.lower(), None
        )


def register_store(prefix, adapter):
    """add a prefix"""
    _storage_prefixes[prefix] = adapter


def connector_factory(dataset):
    prefix = dataset.split(".")[0]
    if prefix in _storage_prefixes:
        return _storage_prefixes[prefix]
    return _storage_prefixes.get("_", None)
