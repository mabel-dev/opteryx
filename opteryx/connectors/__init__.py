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

import os

import pyarrow

from opteryx.connectors.arrow_connector import ArrowConnector
from opteryx.connectors.aws_s3_connector import AwsS3Connector
from opteryx.connectors.cql_connector import CqlConnector
from opteryx.connectors.disk_connector import DiskConnector
from opteryx.connectors.gcp_cloudstorage_connector import GcpCloudStorageConnector
from opteryx.connectors.gcp_firestore_connector import GcpFireStoreConnector
from opteryx.connectors.mongodb_connector import MongoDbConnector
from opteryx.connectors.sql_connector import SqlConnector
from opteryx.shared import MaterializedDatasets

# load the base set of prefixes
_storage_prefixes = {"information_schema": "InformationSchema"}

__all__ = (
    "ArrowConnector",
    "AwsS3Connector",
    "CqlConnector",
    "DiskConnector",
    "GcpCloudStorageConnector",
    "GcpFireStoreConnector",
    "MongoDbConnector",
    "SqlConnector",
)


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
    """register a orso, pandas or Polars dataframe"""
    # polars (maybe others) - the polars to arrow API is a mess
    if hasattr(frame, "_df"):
        frame = frame._df
    if hasattr(frame, "to_arrow"):
        arrow = frame.to_arrow()
        if not isinstance(arrow, pyarrow.Table):
            arrow = pyarrow.Table.from_batches(arrow)
        register_arrow(name, arrow)
        return
    # orso
    if hasattr(frame, "arrow"):
        arrow = frame.arrow()
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


def connector_factory(dataset, statistics, **config):
    """
    Work out which connector will service the access to this dataset.
    """

    # if it starts with a $, it's a special internal dataset
    if dataset[0] == "$":
        from opteryx.connectors import virtual_data

        return virtual_data.SampleDataConnector(dataset=dataset, statistics=statistics)

    # Look up the prefix from the registered prefixes
    connector_entry: dict = config
    for prefix, storage_details in _storage_prefixes.items():
        if dataset == prefix or dataset.startswith(prefix + "."):
            connector_entry = storage_details.copy()  # type: ignore
            connector = connector_entry.pop("connector")
            break
    else:
        if os.path.isfile(dataset):
            from opteryx.connectors import file_connector

            return file_connector.FileConnector(dataset=dataset, statistics=statistics)

        # fall back to the default connector (local disk if not set)
        connector = _storage_prefixes.get("_default", DiskConnector)

    prefix = connector_entry.pop("prefix", "")
    remove_prefix = connector_entry.pop("remove_prefix", False)
    if prefix and remove_prefix and dataset.startswith(prefix):
        dataset = dataset[len(prefix) + 1 :]

    return connector(dataset=dataset, statistics=statistics, **connector_entry)
