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
The CQL Connector downloads data from remote servers and converts them
to pyarrow tables so they can be processed as per any other data source.

CQL is Cassandra Query Language, it looks at lot like SQL.
"""
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Tuple

import pyarrow
from orso import DataFrame
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import PYTHON_TO_ORSO_MAP
from orso.types import OrsoTypes

from opteryx.connectors.base.base_connector import INITIAL_CHUNK_SIZE
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError
from opteryx.managers.expression import Node
from opteryx.managers.expression import NodeType
from opteryx.third_party.query_builder import Query


def _handle_operand(operand: Node, parameters: list) -> Tuple[Any, list]:
    if operand.node_type == NodeType.IDENTIFIER:
        return f'"{operand.source_column}"', parameters

    literal = operand.value
    if hasattr(literal, "item"):
        literal = literal.item()

    parameters.append(literal)
    return "?", parameters


class CqlConnector(BaseConnector, PredicatePushable):
    __mode__ = "Cql"

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
    }

    OPS_XLAT: Dict[str, str] = {
        "Eq": ":left = :right",
        "NotEq": ":left != :right",
        "Gt": ":left > :right",
        "GtEq": ":left >= :right",
        "Lt": ":left < :right",
        "LtEq": ":left <= :right",
        "Like": ":left LIKE :right",
        "NotLike": "NOT (:left LIKE :right)",
    }

    def __init__(
        self,
        *args,
        nodes: List[str] = None,
        username: str = None,
        password: str = None,
        cluster=None,
        **kwargs,
    ):
        BaseConnector.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)

        try:
            from cassandra.auth import PlainTextAuthProvider
            from cassandra.cluster import Cluster
        except ImportError as err:  # pragma: nocover
            raise MissingDependencyError(err.name) from err

        if cluster is None and username is None:  # pragma: no cover
            raise UnmetRequirementError(
                "CQL Connections require either a Cassandra Cluster in the 'cluster' parameter, or a Cassandra cluster nodes in the 'nodes' parameter, usually with the 'username' and 'password' parameters set."
            )

        if username:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            self.cluster = Cluster(nodes, auth_provider=auth_provider)
        else:
            self.cluster = cluster

        self.single_column = None

    def read_dataset(  # type:ignore
        self,
        *,
        columns: list = None,
        predicates: list = None,
        chunk_size: int = INITIAL_CHUNK_SIZE,  # type:ignore
    ) -> Generator[pyarrow.Table, None, None]:  # type:ignore

        self.chunk_size = chunk_size

        result_schema = self.schema

        query_builder = Query().FROM(self.dataset)

        # Update the SQL and the target morsel schema if we've pushed a projection
        if columns:
            column_names = [f'"{col.name}"' for col in columns]
            query_builder.add("SELECT", *column_names)
            result_schema.columns = [  # type:ignore
                col for col in self.schema.columns if f'"{col.name}"' in column_names  # type:ignore
            ]
        else:
            query_builder.add("SELECT", f'"{self.single_column.name}"')  # type:ignore
            self.schema.columns = [self.single_column]  # type:ignore

        # Update SQL if we've pushed predicates
        parameters: list = []
        for predicate in predicates:

            left_operand = predicate.left
            right_operand = predicate.right
            operator = self.OPS_XLAT[predicate.value]

            left_value, parameters = _handle_operand(left_operand, parameters)
            right_value, parameters = _handle_operand(right_operand, parameters)

            operator = operator.replace(":left", left_value)
            operator = operator.replace(":right", right_value)

            query_builder.WHERE(operator)

        session = self.cluster.connect()
        # DEBUG: log ("READ DATASET\n", str(query_builder))
        # DEBUG: log ("PARAMETERS\n", parameters)
        # Execution Options allows us to handle datasets larger than memory
        statement = session.prepare(str(query_builder) + " ALLOW FILTERING;")
        results = session.execute(statement, parameters=parameters)

        at_least_once = False
        for morsel in self.chunk_dictset(
            (doc._asdict() for doc in results),
            initial_chunk_size=chunk_size,
        ):
            at_least_once = True
            yield morsel

        if not at_least_once:
            yield DataFrame(schema=result_schema).arrow()

    def get_dataset_schema(self) -> RelationSchema:

        if self.schema:
            return self.schema

        # Try to read the schema from the metastore
        self.schema = self.read_schema_from_metastore()
        if self.schema:
            return self.schema

        # get the schema from the dataset

        session = self.cluster.connect()
        query = Query().SELECT("*").FROM(self.dataset).LIMIT("1")
        statement = session.prepare(str(query))
        # DEBUG: log ("READ 1 ROWS\n", str(query))
        results = session.execute(statement)[0]
        columns = list(results._asdict().keys())
        # DEBUG: log ("COLUMNS:", columns)
        self.schema = RelationSchema(
            name=self.dataset,
            columns=[
                FlatColumn(
                    name=column,
                    type=PYTHON_TO_ORSO_MAP.get(
                        type(getattr(results, column)), OrsoTypes._MISSING_TYPE
                    ),
                )
                for column in columns
            ],
        )

        self.single_column = self.schema.columns[0]  # type:ignore

        return self.schema
