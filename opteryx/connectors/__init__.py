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

from opteryx.shared import MaterializedDatasets

from .disk_connector import DiskConnector
from .sql_connector import SqlConnector

# load the base set of prefixes
_storage_prefixes = {"information_schema": "InformationSchema"}


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
    """
    Work out which connector will service the access to this dataset.
    """

    # if it starts with a $, it's a special internal dataset
    if dataset[0] == "$":
        from opteryx.connectors import sample_data

        return sample_data.SampleDataConnector(dataset=dataset)

    # otherwise look up the prefix from the registered prefixes
    prefix = dataset.split(".")[0]
    connector_entry: dict = {}
    if prefix in _storage_prefixes:
        connector_entry = _storage_prefixes[prefix].copy()  # type: ignore
        connector = connector_entry.pop("connector")
    elif os.path.isfile(dataset):
        from opteryx.connectors import file_connector

        return file_connector.FileConnector(dataset=dataset)
    else:
        # fall back to the detault connector (local disk if not set)
        connector = _storage_prefixes.get("_default", DiskConnector)

    prefix = connector_entry.pop("prefix", "")
    remove_prefix = connector_entry.pop("remove_prefix", False)
    if prefix and remove_prefix and dataset.startswith(prefix):
        dataset = dataset[len(prefix) + 1 :]

    return connector(dataset=dataset, **connector_entry)
