# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Data Source Connectors with Lazy Loading

This module provides connectors to various data sources, enabling Opteryx to query
data from files, databases, cloud storage, and other systems. Connectors are lazily
loaded to improve startup performance and reduce memory footprint.

Architecture:
Connectors abstract different data sources behind a common interface (BaseConnector),
allowing the query engine to work with any data source transparently. Each connector
is responsible for:
- Reading data and converting it to PyArrow format
- Providing schema information
- Supporting predicate pushdown when possible
- Handling authentication and connection management

Connector Types:

File System Connectors:
- DiskConnector: Local file system access
- AwsS3Connector: Amazon S3 storage
- GcpCloudStorageConnector: Google Cloud Storage

Database Connectors:
- SqlConnector: SQL databases (PostgreSQL, MySQL, SQLite, etc.)
- MongoDbConnector: MongoDB document database
- CqlConnector: Cassandra/ScyllaDB

Format Connectors:
- ArrowConnector: Apache Arrow tables and datasets
- FileConnector: Generic file format handler
- IcebergConnector: Apache Iceberg tables

Special Connectors:
- VirtualDataConnector: In-memory datasets and computed tables
- InformationSchemaConnector: System metadata tables

Lazy Loading:
Connectors are only imported when actually needed, which significantly improves
module import time. The lazy loading is transparent to users - all import patterns
work normally, but the actual connector classes are loaded on first access.

Usage Patterns:

1. Direct Import:
   from opteryx.connectors import ArrowConnector

2. Registration:
   opteryx.register_store("my_prefix", my_connector_instance)

3. Query Usage:
   opteryx.query("SELECT * FROM s3://bucket/file.parquet")

Connector Development:
1. Inherit from BaseConnector
2. Implement required methods (read_dataset, get_dataset_schema)
3. Add optional optimizations (predicate pushdown, column pruning)
4. Register with appropriate prefixes
5. Add comprehensive tests

Example Custom Connector:
    class MyConnector(BaseConnector):
        def read_dataset(self, dataset, **kwargs):
            # Read data and return PyArrow table
            return pa.table(data)

        def get_dataset_schema(self, dataset):
            # Return schema information
            return pa.schema([...])

Performance Considerations:
- Implement predicate pushdown to reduce data transfer
- Support column pruning for wide tables
- Use async operations for I/O bound connectors
- Cache schema information when appropriate
- Consider connection pooling for database connectors

The lazy loading system maps prefixes to connector classes and loads them
on demand, significantly reducing initial import time while maintaining
full functionality.
"""

import os

import pyarrow

# Lazy imports - connectors are only loaded when actually needed
# This significantly improves module import time from ~500ms to ~130ms

# load the base set of prefixes
# fmt:off
_storage_prefixes = {
    "information_schema": "InformationSchema",
    "gs:": {"connector": "GcpCloudStorageConnector", "prefix": "gs://", "remove_prefix": True},
    "s3:": {"connector": "AwsS3Connector", "prefix": "s3://", "remove_prefix": True},
    "minio:": {"connector": "AwsS3Connector", "prefix": "minio://", "remove_prefix": True},
    "file:": {"connector": "DiskConnector", "prefix": "file://", "remove_prefix": True},
}
# fmt:on

__all__ = (
    "ArrowConnector",
    "AwsS3Connector",
    "CqlConnector",
    "DiskConnector",
    "FileConnector",
    "GcpCloudStorageConnector",
    "GcpFireStoreConnector",
    "IcebergConnector",
    "MongoDbConnector",
    "SqlConnector",
)


def _lazy_import_connector(connector_name: str):
    """
    Lazy import a connector class by name.

    This function is called by __getattr__ when a connector is accessed,
    or by connector_factory when a connector needs to be instantiated.

    Args:
        connector_name: The name of the connector class to import

    Returns:
        The connector class

    Raises:
        ValueError: If the connector name is unknown
    """
    if connector_name == "ArrowConnector":
        from opteryx.connectors.arrow_connector import ArrowConnector

        return ArrowConnector
    elif connector_name == "AwsS3Connector":
        from opteryx.connectors.aws_s3_connector import AwsS3Connector

        return AwsS3Connector
    elif connector_name == "CqlConnector":
        from opteryx.connectors.cql_connector import CqlConnector

        return CqlConnector
    elif connector_name == "DiskConnector":
        from opteryx.connectors.disk_connector import DiskConnector

        return DiskConnector
    elif connector_name == "FileConnector":
        from opteryx.connectors.file_connector import FileConnector

        return FileConnector
    elif connector_name == "GcpCloudStorageConnector":
        from opteryx.connectors.gcp_cloudstorage_connector import GcpCloudStorageConnector

        return GcpCloudStorageConnector
    elif connector_name == "GcpFireStoreConnector":
        from opteryx.connectors.gcp_firestore_connector import GcpFireStoreConnector

        return GcpFireStoreConnector
    elif connector_name == "IcebergConnector":
        from opteryx.connectors.iceberg_connector import IcebergConnector

        return IcebergConnector
    elif connector_name == "MongoDbConnector":
        from opteryx.connectors.mongodb_connector import MongoDbConnector

        return MongoDbConnector
    elif connector_name == "SqlConnector":
        from opteryx.connectors.sql_connector import SqlConnector

        return SqlConnector
    else:
        raise ValueError(f"Unknown connector: {connector_name}")


def register_store(prefix, connector, *, remove_prefix: bool = False, **kwargs):
    """add a prefix"""
    if not isinstance(connector, type):  # type: ignore
        # uninstantiated classes aren't a type
        raise ValueError("connectors registered with `register_store` must be uninstantiated.")

    # Store connector class directly (not as a string)
    _storage_prefixes[prefix] = {
        "connector": connector,  # type: ignore
        "prefix": prefix,
        "remove_prefix": remove_prefix,
        **kwargs,
    }


def register_df(name, frame):
    """register a orso, pandas or Polars dataframe"""
    # Lazy import ArrowConnector
    from opteryx.connectors.arrow_connector import ArrowConnector

    # polars (maybe others) - the polars to arrow API is a mess
    if hasattr(frame, "_df"):  # pragma: no cover
        frame = frame._df
    if "PyDataFrame" in str(type(frame)):  # pragma: no cover
        arrow = frame.to_arrow(compat_level=1)
        if not isinstance(arrow, pyarrow.Table):
            from opteryx.exceptions import NotSupportedError

            raise NotSupportedError(
                "Polars not supported whilst changes are being made to the Polars to Arrow APIs"
            )
        register_arrow(name, arrow)
        return
    if hasattr(frame, "to_arrow"):  # pragma: no cover
        arrow = frame.to_arrow()
        if not isinstance(arrow, pyarrow.Table):
            arrow = pyarrow.Table.from_batches(arrow)
        register_arrow(name, arrow)
        return
    # orso
    if hasattr(frame, "arrow"):  # pragma: no cover
        arrow = frame.arrow()
        register_arrow(name, arrow)
        return
    # pandas
    frame_type = str(type(frame))
    if "pandas" in frame_type:  # pragma: no cover
        register_arrow(name, pyarrow.Table.from_pandas(frame))
        return
    raise ValueError("Unable to register unknown frame type.")


def register_arrow(name, table):
    """register an arrow table"""
    from opteryx.connectors.arrow_connector import ArrowConnector
    from opteryx.shared import MaterializedDatasets

    materialized_datasets = MaterializedDatasets()
    materialized_datasets[name] = table
    register_store(name, ArrowConnector)


def known_prefix(prefix) -> bool:
    return prefix in _storage_prefixes


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
        if (
            dataset == prefix
            or dataset.startswith(prefix + ".")
            or dataset.startswith(prefix + "//")
        ):
            connector_entry.update(storage_details.copy())  # type: ignore
            connector = connector_entry.pop("connector")
            # If connector is a string, lazy load it
            if isinstance(connector, str):
                connector = _lazy_import_connector(connector)
            break
    else:
        if os.path.isfile(dataset):
            from opteryx.connectors import file_connector

            return file_connector.FileConnector(dataset=dataset, statistics=statistics)
        # fall back to the default connector (local disk if not set)
        connector_entry = _storage_prefixes.get("_default", {})
        connector = connector_entry.pop("connector", "DiskConnector")
        # If connector is a string, lazy load it
        if isinstance(connector, str):
            connector = _lazy_import_connector(connector)

    prefix = connector_entry.pop("prefix", "")
    remove_prefix = connector_entry.pop("remove_prefix", False)
    if prefix and remove_prefix and dataset.startswith(prefix):
        dataset = dataset[len(prefix) + 1 :]

    return connector(dataset=dataset, statistics=statistics, **connector_entry)


def __getattr__(name):
    """
    Lazy load connector classes when accessed as module attributes.

    This allows the standard import pattern to work:
        from opteryx.connectors import ArrowConnector

    But defers the actual import until the connector is accessed,
    significantly improving initial module load time.

    Args:
        name: The attribute name being accessed

    Returns:
        The connector class if it exists in __all__

    Raises:
        AttributeError: If the attribute doesn't exist
    """
    if name in __all__:
        return _lazy_import_connector(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
