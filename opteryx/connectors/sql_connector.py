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

"""
from decimal import Decimal

from orso import DataFrame
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import PYTHON_TO_ORSO_MAP

from opteryx.connectors.base.base_connector import DEFAULT_MORSEL_SIZE
from opteryx.connectors.base.base_connector import INITIAL_CHUNK_SIZE
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError


class SqlConnector(BaseConnector):
    __mode__ = "Sql"

    def __init__(self, *args, connection: str = None, engine=None, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            from sqlalchemy import MetaData
            from sqlalchemy import create_engine
        except ImportError as err:  # pragma: nocover
            raise MissingDependencyError(err.name) from err

        if engine is None and connection is None:
            raise UnmetRequirementError(
                "SQL Connections require either a SQL Alchemy connection string in the 'connection' parameter, or a SQL Alchemy Engine in the 'engine' parameter."
            )

        # create the SqlAlchemy engine
        if engine is None:
            self._engine = create_engine(connection)
        else:
            self._engine = engine

        self.schema = None
        self.metadata = MetaData()

    def read_dataset(
        self, columns: list = None, chunk_size: int = INITIAL_CHUNK_SIZE
    ) -> "DatasetReader":
        from sqlalchemy import Table
        from sqlalchemy import select

        self.chunk_size = chunk_size

        # get the schema from the dataset
        table = Table(self.dataset, self.metadata, autoload_with=self._engine)
        print("SQL push projection")
        query = select(table)
        morsel = DataFrame(schema=self.schema)

        with self._engine.connect() as conn:
            for row in conn.execute(query):
                morsel._rows.append(row)
                if len(morsel) == self.chunk_size:
                    yield morsel.arrow()

                    if morsel.nbytes > 0:
                        self.chunk_size = int(len(morsel) // (morsel.nbytes / DEFAULT_MORSEL_SIZE))

                    morsel = DataFrame(schema=self.schema)

        if len(morsel) > 0:
            yield morsel.arrow()

    def get_dataset_schema(self) -> RelationSchema:
        from sqlalchemy import Table

        if self.schema:
            return self.schema

        # Try to read the schema from the metastore
        self.schema = self.read_schema_from_metastore()
        if self.schema:
            return self.schema

        # get the schema from the dataset
        table = Table(self.dataset, self.metadata, autoload_with=self._engine)

        self.schema = RelationSchema(
            name=table.name,
            columns=[
                FlatColumn(
                    name=column.name,
                    type=PYTHON_TO_ORSO_MAP[column.type.python_type],
                    precision=None if column.type.python_type != Decimal else column.type.precision,
                    scale=None if column.type.python_type != Decimal else column.type.scale,
                    nullable=column.nullable,
                )
                for column in table.columns
            ],
        )

        return self.schema
