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

"""
A MongoDB Reader
This is a light-weight MongoDB reader.

Based on the now deprecated Mabel MongoDB reader
https://github.com/mabel-dev/mabel/blob/6bcd978b90870187d5eff939be3f5845a3cdf900/mabel/adapters/mongo/mongodb_reader.py
"""

import os
from typing import Generator

from orso.schema import FlatColumn
from orso.schema import RelationSchema

from opteryx.connectors.base.base_connector import INITIAL_CHUNK_SIZE
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError


class MongoDbConnector(BaseConnector):
    __mode__ = "Collection"

    def __init__(self, *args, database: str = None, connection: str = None, **kwargs):
        super().__init__(*args, **kwargs)

        try:
            import pymongo  # type:ignore
        except ImportError as err:  # pragma: no cover
            raise MissingDependencyError(err.name) from err

        # establish the connection to mongodb
        if connection:
            self.connection = connection
        else:
            self.connection = os.environ.get("MONGODB_CONNECTION")

        if self.connection is None:  # pragma: no cover
            raise UnmetRequirementError(
                "MongoDB connector requires 'connection' set in register_store, or MONGODB_CONNECTION set in environment variables."
            )

        if database:
            self.database = database
        else:
            self.database = os.environ.get("MONGODB_DATABASE")

        if self.database is None:  # pragma: no cover
            raise UnmetRequirementError(
                "MongoDB connector requires 'database' set in register_stpre or MONGODB_DATABASE set in environment variables."
            )

    def read_dataset(
        self, columns: list = None, chunk_size: int = INITIAL_CHUNK_SIZE, **kwargs
    ) -> Generator:
        import pymongo

        client = pymongo.MongoClient(self.connection)  # type:ignore
        database = client[self.database]
        documents = database[self.dataset].find()
        for morsel in self.chunk_dictset(documents, columns=columns, initial_chunk_size=chunk_size):
            yield morsel

    def get_dataset_schema(self) -> RelationSchema:
        if self.schema:
            return self.schema

        # Try to read the schema from the metastore
        self.schema = self.read_schema_from_metastore()
        if self.schema:
            return self.schema

        # onlt read one record
        record = next(self.read_dataset(chunk_size=1), None)

        if record is None:
            raise DatasetNotFoundError(dataset=self.dataset)

        arrow_schema = record.schema

        self.schema = RelationSchema(
            name=self.dataset,
            columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
        )

        return self.schema
