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
Inner Reader for SQL stores

This currently isn't a base class because we're assuming a standard
functionality of SQL engines.

This relies on SQLAlchemy
"""
import typing
from functools import lru_cache

import pyarrow

from opteryx import config
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import MissingDependencyError


class BaseSQLStorageAdapter:  # this is used by the SHOW STORES statement
    pass


def _write_predicate(predicate):
    column, operator, literal = predicate

    operator_map = {"==": "="}
    operator = operator_map.get(operator, operator)

    if isinstance(literal, str):
        literal = "'" + literal.replace("'", "''") + "'"

    return f"{column} {operator} {literal}"


@lru_cache(8)
def _get_engine(connection):
    """take advantage of pooling"""
    from sqlalchemy import create_engine

    return create_engine(connection)


class SqlConnector(BaseSQLStorageAdapter, PredicatePushable):
    __mode__ = "SQL"

    def __init__(
        self, prefix: str = "", remove_prefix: bool = False, connection: str = None
    ) -> None:
        super(BaseSQLStorageAdapter, self).__init__()
        super(PredicatePushable, self).__init__()
        # we're just testing we can import here
        try:
            from sqlalchemy import create_engine
        except ImportError as err:  # pragma: nocover
            raise MissingDependencyError(err.name) from err

        self._connection = connection

        self._remove_prefix = remove_prefix
        self._prefix = prefix

    def read_records(
        self,
        dataset,
        selection: typing.Union[list, None] = None,
        morsel_size: typing.Union[int, None] = None,
    ):  # pragma: no cover
        """
        Return a morsel of documents
        """
        from sqlalchemy import text

        from opteryx.third_party.query_builder import Query

        if morsel_size is None:
            morsel_size = config.MORSEL_SIZE
        chunk_size = 500

        queried_relation = dataset
        if self._remove_prefix:
            if dataset.startswith(f"{self._prefix}."):
                queried_relation = dataset[len(self._prefix) + 1 :]

        # we're using a query builder to prevent hand-crafting SQL
        query_builder = Query()
        query_builder.FROM(queried_relation)
        if selection is None:
            query_builder.SELECT("*")
        else:
            query_builder.SELECT(*selection)

        for predicate in self._predicates:
            query_builder.WHERE(_write_predicate(predicate))

        engine = _get_engine(self._connection)
        with engine.connect() as conn:
            result = conn.execute(text(str(query_builder)))

            batch = result.fetchmany(chunk_size)
            while batch:
                morsel = pyarrow.Table.from_pylist([b._asdict() for b in batch])
                # from 500 records, estimate the number of records to fill the morsel size
                if chunk_size == 500 and morsel.nbytes > 0:
                    chunk_size = int(morsel_size // (morsel.nbytes / 500))
                yield morsel
                batch = result.fetchmany(chunk_size)
