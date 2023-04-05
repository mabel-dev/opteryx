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
from .base.base_blob_storage_adapter import BaseBlobStorageAdapter  # isort: skip
from .base.base_document_storage_adapter import BaseDocumentStorageAdapter  # isort: skip

import pyarrow

from opteryx.config import DATASET_PREFIX_MAPPING
from opteryx.connectors.arrow_connector import ArrowConnector
from opteryx.connectors.aws_s3_connector import AwsS3Connector
from opteryx.connectors.disk_connector import DiskConnector
from opteryx.connectors.gcp_cloudstorage_connector import GcpCloudStorageConnector
from opteryx.connectors.gcp_firestore_connector import GcpFireStoreConnector
from opteryx.connectors.hadro_connector import HadroConnector
from opteryx.connectors.mongodb_connector import MongoDbConnector
from opteryx.connectors.sql_connector import SqlConnector
from opteryx.shared import MaterializedDatasets

WELL_KNOWN_ADAPTERS = {
    "disk": DiskConnector,
    "gcs": GcpCloudStorageConnector,
    "hadro": HadroConnector,
    "firestore": GcpFireStoreConnector,
    "minio": AwsS3Connector,
    "mongodb": MongoDbConnector,
    "s3": AwsS3Connector,
    "sql": SqlConnector,
}

_storage_prefixes = {"information_schema": "InformationSchema"}

if not isinstance(DATASET_PREFIX_MAPPING, dict):  # pragma: no cover
    _storage_prefixes["_"] = "disk"
else:
    for _prefix, _adapter_name in DATASET_PREFIX_MAPPING.items():
        _storage_prefixes[_prefix] = WELL_KNOWN_ADAPTERS.get(
            _adapter_name.lower(), None
        )  # type:ignore


def register_store(prefix, connector, *, remove_prefix: bool = False, **kwargs):
    """add a prefix"""
    if not isinstance(connector, type):  # type: ignore
        # uninstantiated classes aren't a type
        raise ValueError("connectors registered with `register_store` must be uninstantiated.")
    _storage_prefixes[prefix] = {
        "connector": connector,  # type: ignore
        "prefix": prefix,
        "remove_prefix": remove_prefix,
        **kwargs,
    }


def register_df(name, frame):
    """register a pandas or Polars dataframe"""
    # polars (maybe others) - the polars to arrow API is a mess
    if hasattr(frame, "_df"):
        frame = frame._df
    if hasattr(frame, "to_arrow"):
        arrow = frame.to_arrow()
        if not isinstance(arrow, pyarrow.Table):
            arrow = pyarrow.Table.from_batches(arrow)
        register_arrow(name, arrow)
        return
    # pandas
    frame_type = str(type(frame))
    if "pandas" in frame_type:
        register_arrow(name, pyarrow.Table.from_pandas(frame))
        return
    raise ValueError("Unable to register unknown frame type.")


def register_arrow(name, table):
    """register an arrow table"""
    materialized_datasets = MaterializedDatasets()
    materialized_datasets[name] = table
    register_store(name, ArrowConnector)


def connector_factory(dataset):
    prefix = dataset.split(".")[0]
    connector_entry: dict = {}
    if prefix in _storage_prefixes:
        connector_entry = _storage_prefixes[prefix].copy()  # type: ignore
        connector = connector_entry.pop("connector")
    else:
        connector = _storage_prefixes.get("_")

    prefix = connector_entry.pop("prefix", "")
    remove_prefix = connector_entry.pop("remove_prefix", False)

    return connector(prefix=prefix, remove_prefix=remove_prefix, **connector_entry)
