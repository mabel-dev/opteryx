# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The file connector provides the reader for when a file name is provided as the
dataset name in a query.
"""

import mmap
import os
from typing import Dict
from typing import Optional

import pyarrow
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import LimitPushable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.connectors.capabilities import Statistics
from opteryx.exceptions import DatasetNotFoundError
from opteryx.utils import is_windows
from opteryx.utils.file_decoders import get_decoder

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
    *, blob_name: str, decoder, just_schema=False, statistics=None, projection=None, selection=None
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


class FileConnector(BaseConnector, PredicatePushable, Statistics, LimitPushable):
    """
    Connector for reading datasets from a file.
    """

    __mode__ = "Blob"
    __type__ = "FILE"
    _byte_array: Optional[bytes] = None  # Instance attribute to store file bytes

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

    @property
    def interal_only(self):
        return True

    def __init__(self, *args, **kwargs):
        BaseConnector.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        Statistics.__init__(self, **kwargs)
        LimitPushable.__init__(self, **kwargs)

        if ".." in self.dataset or self.dataset[0] in ("\\", "/", "~"):
            # Don't find any datasets which look like path traversal
            raise DatasetNotFoundError(dataset=self.dataset)
        self.decoder = get_decoder(self.dataset)

    def read_dataset(
        self, columns: list = None, predicates: list = None, limit: int = None, **kwargs
    ) -> pyarrow.Table:
        morsel = read_blob(
            blob_name=self.dataset,
            decoder=self.decoder,
            statistics=self.statistics,
            projection=columns,
            selection=predicates,
        )[2]

        if limit is not None:
            morsel = morsel.slice(offset=0, length=limit)

        yield morsel

    def get_dataset_schema(self) -> RelationSchema:
        """
        Retrieves the schema from the dataset file.

        Returns:
            The schema of the dataset.
        """
        import mmap

        if self.schema is not None:
            return self.schema

        file_descriptor = os.open(self.dataset, os.O_RDONLY | os.O_BINARY)
        size = os.path.getsize(self.dataset)
        _map = mmap.mmap(file_descriptor, size, access=mmap.ACCESS_READ)
        self.schema = self.decoder(_map, just_schema=True)
        self.relation_statistics = self.decoder(_map, just_statistics=True)
        return self.schema
