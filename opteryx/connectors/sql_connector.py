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
import pyarrow
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import PYTHON_TO_ORSO_MAP

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.exceptions import DatasetNotFoundError
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

    def read_dataset(self) -> "DatasetReader":
        from sqlalchemy import text

        sql = f"SELECT * FROM '{self.dataset}'"

        with self._engine.connect() as conn:
            result = conn.execute(text(sql))

        morsel_size = 10000
        if morsel_size is None:
            morsel_size = 100000
        chunk_size = 500

        batch = result.fetchmany(chunk_size)
        while batch:
            arrays = [pyarrow.array(column) for column in zip(*batch)]
            morsel = pyarrow.Table.from_arrays(arrays, self.schema.column_names)
            # from 500 records, estimate the number of records to fill the morsel size
            if chunk_size == 500 and morsel.nbytes > 0:
                chunk_size = int(morsel_size // (morsel.nbytes / 500))
            yield morsel
            batch = result.fetchmany(chunk_size)

        result.close()

    def get_dataset_schema(self) -> RelationSchema:
        if self.schema:
            return self.schema

        # Try to read the schema from the metastore
        self.schema = self.read_schema_from_metastore()
        if self.schema:
            return self.schema

        # get the schema from the dataset
        from sqlalchemy import MetaData
        from sqlalchemy import Table
        from sqlalchemy import create_engine
        from sqlalchemy import select
        from sqlalchemy import text
        from sqlalchemy.exc import NoSuchTableError

        table = Table(self.dataset, self.metadata, autoload_with=self._engine)
        query = select(table).limit(1)

        try:
            with self._engine.connect() as conn:
                result = conn.execute(query)
        except NoSuchTableError as err:
            raise DatasetNotFoundError(dataset=self.dataset) from err

        columns = [column[0] for column in result.cursor.description]
        record = result.fetchone()

        # Close the result object
        result.close()

        if record is None:
            raise DatasetNotFoundError(dataset=self.dataset)

        self.schema = RelationSchema(
            name=self.dataset,
            columns=[
                FlatColumn(name=column_name, type=PYTHON_TO_ORSO_MAP[type(column_value)])
                for column_name, column_value in zip(columns, record)
            ],
        )

        return self.schema
