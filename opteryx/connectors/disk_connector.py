# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The 'direct disk' connector provides the reader for when a dataset is
given as a folder on local disk
"""

import mmap
import os
from typing import Dict
from typing import List

import pyarrow
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import LimitPushable
from opteryx.connectors.capabilities import Partitionable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import DataError
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import EmptyDatasetError
from opteryx.exceptions import UnsupportedFileTypeError
from opteryx.utils import is_windows
from opteryx.utils.file_decoders import TUPLE_OF_VALID_EXTENSIONS
from opteryx.utils.file_decoders import get_decoder

OS_SEP = os.sep
IS_WINDOWS = is_windows()

# Define os.O_BINARY for non-Windows platforms if it's not already defined
if not hasattr(os, "O_BINARY"):
    os.O_BINARY = 0  # Value has no effect on non-Windows platforms
if not hasattr(os, "O_DIRECT"):
    os.O_DIRECT = 0  # Value has no effect on non-Windows platforms

mmap_config = {}
if not IS_WINDOWS:
    mmap_config["flags"] = mmap.MAP_PRIVATE
    mmap_config["prot"] = mmap.PROT_READ
else:
    mmap_config["access"] = mmap.ACCESS_READ


def read_blob(
    *, blob_name: str, decoder, statistics, just_schema=False, projection=None, selection=None
):
    """
    Read a blob (binary large object) from disk using memory-mapped file access.

    This method uses low-level file reading with memory-mapped files to
    improve performance. It reads the entire file into memory and then
    decodes it using the provided decoder function.

    Parameters:
        blob_name (str):
            The name of the blob file to read.
        decoder (callable):
            A function to decode the memory-mapped file content.
        just_schema (bool, optional):
            If True, only the schema of the data is returned. Defaults to False.
        projection (list, optional):
            A list of fields to project. Defaults to None.
        selection (dict, optional):
            A dictionary of selection criteria. Defaults to None.
        **kwargs:
            Additional keyword arguments.

    Returns:
        The decoded blob content.

    Raises:
        FileNotFoundError:
            If the blob file does not exist.
        OSError:
            If an I/O error occurs while reading the file.
    """
    try:
        file_descriptor = os.open(blob_name, os.O_RDONLY | os.O_BINARY)
        if hasattr(os, "posix_fadvise"):
            os.posix_fadvise(file_descriptor, 0, 0, os.POSIX_FADV_WILLNEED)
        size = os.fstat(file_descriptor).st_size
        _map = mmap.mmap(file_descriptor, length=size, **mmap_config)
        result = decoder(
            _map,
            just_schema=just_schema,
            projection=projection,
            selection=selection,
            use_threads=True,
        )
        statistics.bytes_read += size
        return result
    finally:
        os.close(file_descriptor)


class DiskConnector(BaseConnector, Partitionable, PredicatePushable, LimitPushable):
    """
    Connector for reading datasets from files on local storage.
    """

    __mode__ = "Blob"
    __type__ = "LOCAL"

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
    }

    PUSHABLE_TYPES = {
        OrsoTypes.BLOB,
        OrsoTypes.BOOLEAN,
        OrsoTypes.DOUBLE,
        OrsoTypes.INTEGER,
        OrsoTypes.VARCHAR,
        OrsoTypes.TIMESTAMP,
        OrsoTypes.DATE,
    }

    def __init__(self, **kwargs):
        """
        Initialize the DiskConnector, which reads datasets directly from disk.

        Parameters:
            kwargs: Dict
                Arbitrary keyword arguments.
        """
        BaseConnector.__init__(self, **kwargs)
        Partitionable.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        LimitPushable.__init__(self, **kwargs)

        self.dataset = self.dataset.replace(".", OS_SEP)
        self.cached_first_blob = None  # Cache for the first blob in the dataset
        self.blob_list = {}
        self.rows_seen = 0
        self.blobs_seen = 0

    def get_list_of_blob_names(self, *, prefix: str) -> List[str]:
        """
        List all blob files in the given directory path.

        Parameters:
            prefix: str
                The directory path.

        Returns:
            A list of blob filenames.
        """
        # only fetch once per prefix (partition)
        if prefix in self.blob_list:
            return self.blob_list[prefix]

        blobs = sorted(
            os.path.join(root, file)
            for root, _, files in os.walk(prefix + OS_SEP)
            for file in files
            if file.endswith(TUPLE_OF_VALID_EXTENSIONS)
        )

        self.blob_list[prefix] = blobs
        return blobs

    def read_dataset(
        self,
        columns: list = None,
        predicates: list = None,
        just_schema: bool = False,
        limit: int = None,
        **kwargs,
    ) -> pyarrow.Table:
        """
        Read the entire dataset from disk.

        Yields:
            Each blob's content as a PyArrow Table.
        """
        blob_names = self.partition_scheme.get_blobs_in_partition(
            start_date=self.start_date,
            end_date=self.end_date,
            blob_list_getter=self.get_list_of_blob_names,
            prefix=self.dataset,
        )

        remaining_rows = limit if limit is not None else float("inf")

        for blob_name in blob_names:
            decoder = get_decoder(blob_name)
            try:
                if not just_schema:
                    num_rows, _, decoded = read_blob(
                        blob_name=blob_name,
                        statistics=self.statistics,
                        decoder=decoder,
                        just_schema=False,
                        projection=columns,
                        selection=predicates,
                    )

                    # push limits to the reader
                    if decoded.num_rows > remaining_rows:
                        decoded = decoded.slice(0, remaining_rows)
                    remaining_rows -= decoded.num_rows

                    self.statistics.rows_seen += num_rows
                    self.rows_seen += num_rows
                    self.blobs_seen += 1
                    yield decoded

                    # if we have read all the rows we need to stop
                    if remaining_rows <= 0:
                        break
                else:
                    schema = read_blob(
                        blob_name=blob_name,
                        statistics=self.statistics,
                        decoder=decoder,
                        just_schema=True,
                    )
                    # if we have more than one blob we need to estimate the row count
                    blob_count = len(blob_names)
                    if schema.row_count_metric and blob_count > 1:
                        schema.row_count_estimate = schema.row_count_metric * blob_count
                        schema.row_count_metric = None
                        self.statistics.estimated_row_count += schema.row_count_estimate
                    yield schema

            except UnsupportedFileTypeError:
                pass  # Skip unsupported file types
            except pyarrow.ArrowInvalid:
                self.statistics.unreadable_data_blobs += 1
            except Exception as err:
                raise DataError(f"Unable to read file {blob_name} ({err})") from err

    def get_dataset_schema(self) -> RelationSchema:
        """
        Retrieve the schema of the dataset either from the metastore or infer it from the first blob.

        Returns:
            The schema of the dataset.
        """
        if self.schema:
            return self.schema

        for schema in self.read_dataset(just_schema=True):
            self.schema = schema
            break

        if self.schema is None:
            if os.path.isdir(self.dataset):
                raise EmptyDatasetError(dataset=self.dataset.replace(OS_SEP, "."))
            raise DatasetNotFoundError(dataset=self.dataset)

        return self.schema
