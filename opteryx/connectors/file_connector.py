# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The file connector provides the reader for when a file name is provided as the
dataset name in a query.
"""

import os
from typing import Dict
from typing import Optional

import pyarrow
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.connectors.capabilities import Statistics
from opteryx.connectors.disk_connector import read_blob
from opteryx.exceptions import DatasetNotFoundError
from opteryx.utils.file_decoders import get_decoder


class FileConnector(BaseConnector, PredicatePushable, Statistics):
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
        if ".." in self.dataset or self.dataset[0] in ("\\", "/", "~"):
            # Don't find any datasets which look like path traversal
            raise DatasetNotFoundError(dataset=self.dataset)
        self.decoder = get_decoder(self.dataset)

    def read_dataset(
        self, columns: list = None, predicates: list = None, **kwargs
    ) -> pyarrow.Table:
        yield read_blob(
            blob_name=self.dataset,
            decoder=self.decoder,
            statistics=self.statistics,
            projection=columns,
            selection=predicates,
        )[2]

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
