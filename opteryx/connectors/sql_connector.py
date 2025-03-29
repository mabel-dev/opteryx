# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The SQL Connector downloads data from remote servers and converts them
to pyarrow tables so they can be processed as per any other data source.
"""

import time
from typing import Any
from typing import Dict
from typing import Generator
from typing import Optional
from typing import Tuple

import orjson
import pyarrow
from orso import DataFrame
from orso.schema import ConstantColumn
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import random_string
from orso.types import PYTHON_TO_ORSO_MAP
from orso.types import OrsoTypes

from opteryx.config import OPTERYX_DEBUG
from opteryx.connectors.base.base_connector import DEFAULT_MORSEL_SIZE
from opteryx.connectors.base.base_connector import INITIAL_CHUNK_SIZE
from opteryx.connectors.base.base_connector import MIN_CHUNK_SIZE
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import LimitPushable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError
from opteryx.managers.expression import Node
from opteryx.managers.expression import NodeType
from opteryx.third_party.query_builder import Query


def _handle_operand(operand: Node, parameters: dict) -> Tuple[Any, dict]:
    if operand.node_type == NodeType.IDENTIFIER:
        return operand.source_column, parameters

    literal = operand.value
    if hasattr(literal, "item"):
        literal = literal.item()

    name = random_string(8)
    parameters[name] = literal
    return f":{name}", parameters


class SqlConnector(BaseConnector, LimitPushable, PredicatePushable):
    __mode__ = "Sql"
    __type__ = "SQL"

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,  # not all databases handle nulls consistently
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
        "Like": True,
        "NotLike": True,
        "IsNull": True,
        "IsNotNull": True,
        "InStr": True,
        "NotInStr": True,
    }

    OPS_XLAT: Dict[str, str] = {
        "Eq": "=",
        "NotEq": "!=",
        "Gt": ">",
        "GtEq": ">=",
        "Lt": "<",
        "LtEq": "<=",
        "Like": "LIKE",
        "NotLike": "NOT LIKE",
        "IsNull": "IS NULL",
        "IsNotNull": "IS NOT NULL",
        "InStr": "LIKE",
        "NotInStr": "NOT LIKE",
    }

    def __init__(self, *args, connection: str = None, engine=None, **kwargs):
        BaseConnector.__init__(self, **kwargs)
        LimitPushable.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)

        try:
            from sqlalchemy import MetaData
            from sqlalchemy import create_engine
            from sqlalchemy.pool import NullPool
        except ImportError as err:  # pragma: nocover
            raise MissingDependencyError(err.name) from err

        if engine is None and connection is None:  # pragma: no cover
            raise UnmetRequirementError(
                "SQL Connections require either a SQL Alchemy connection string in the 'connection' parameter, or a SQL Alchemy Engine in the 'engine' parameter."
            )

        # create the SqlAlchemy engine
        if engine is None:
            self._engine = create_engine(connection, poolclass=NullPool, echo=OPTERYX_DEBUG)
        else:
            self._engine = engine

        self.schema = None  # type: ignore
        self.metadata = MetaData()

    def can_push(self, operator: Node, types: set = None) -> bool:
        if super().can_push(operator, types):
            return True
        if operator.condition.node_type == NodeType.UNARY_OPERATOR:
            return operator.condition.value in self.PUSHABLE_OPS
        return False

    def read_dataset(  # type:ignore
        self,
        *,
        columns: list = None,
        predicates: list = None,
        chunk_size: int = INITIAL_CHUNK_SIZE,  # type:ignore
        limit: Optional[int] = None,
    ) -> Generator[pyarrow.Table, None, None]:  # type:ignore
        from sqlalchemy.sql import text

        self.chunk_size = chunk_size
        result_schema = self.schema

        query_builder = Query().FROM(self.dataset)

        # Update the SQL and the target morsel schema if we've pushed a projection
        if columns:
            column_names = [col.schema_column.name for col in columns]
            query_builder.add("SELECT", *column_names)
            result_schema.columns = [  # type:ignore
                col
                for col in self.schema.columns  # type:ignore
                if col.name in column_names  # type:ignore
            ]
        else:
            query_builder.add("SELECT", "1")
            self.schema.columns = [ConstantColumn(name="1", value=1)]  # type:ignore

        # Update SQL if we've pushed predicates
        parameters: dict = {}
        for predicate in predicates:
            if predicate.node_type == NodeType.UNARY_OPERATOR:
                operand = predicate.centre.current_name
                operator = self.OPS_XLAT[predicate.value]

                query_builder.WHERE(f"{operand} {operator}")
            else:
                left_operand = predicate.left
                right_operand = predicate.right
                operator = self.OPS_XLAT[predicate.value]
                if predicate.value in {"InStr", "NotInStr"}:
                    right_operand.value = f"%{right_operand.value}%"

                left_value, parameters = _handle_operand(left_operand, parameters)
                right_value, parameters = _handle_operand(right_operand, parameters)

                query_builder.WHERE(f"{left_value} {operator} {right_value}")

        if limit is not None:
            query_builder.LIMIT(str(limit))

        at_least_once = False

        convert_time = 0.0

        with self._engine.connect() as conn:
            # DEBUG: print("READ DATASET\n", str(query_builder))
            # DEBUG: print("PARAMETERS\n", parameters)
            # Execution Options allows us to handle datasets larger than memory
            result = conn.execution_options(stream_results=True, max_row_buffer=25000).execute(
                text(str(query_builder)), parameters=parameters
            )

            while True:
                t = time.monotonic_ns()
                batch_rows = result.fetchmany(self.chunk_size)
                self.statistics.time_waiting_sql = time.monotonic_ns() - t
                if not batch_rows:
                    break

                # If we have a struct column, we need to convert the data to bytes
                if any(col.type == OrsoTypes.STRUCT for col in self.schema.columns):
                    batch_rows = list(batch_rows)
                    for i, row in enumerate(batch_rows):
                        batch_rows[i] = tuple(
                            orjson.dumps(field) if isinstance(field, dict) else field
                            for field in row
                        )

                # convert the SqlAlchemy Results to Arrow using Orso
                b = time.monotonic_ns()
                morsel = DataFrame(schema=result_schema, rows=batch_rows).arrow()
                convert_time += time.monotonic_ns() - b

                # Dynamically adjust chunk size based on the data size, we start by downloading
                # 500 records to get an idea of the row size, assuming these 500 are
                # representative, we work out how many rows fit into 16Mb (check setting).
                # Don't keep recalculating, this is not a cheap operation and it's predicting
                # the future so isn't going to ever be 100% correct
                if self.chunk_size == INITIAL_CHUNK_SIZE and morsel.nbytes > 0:
                    self.chunk_size = int(len(morsel) // (morsel.nbytes / DEFAULT_MORSEL_SIZE)) + 1
                    self.chunk_size = (self.chunk_size // MIN_CHUNK_SIZE) * MIN_CHUNK_SIZE
                    self.chunk_size = max(self.chunk_size, MIN_CHUNK_SIZE)
                    self.chunk_size = min(self.chunk_size, 1000000)  # cap at 1 million
                    # DEBUG: print(f"CHANGING CHUNK SIZE TO {self.chunk_size} was {INITIAL_CHUNK_SIZE} ({morsel.nbytes} bytes).")

                yield morsel
                at_least_once = True

        if not at_least_once:
            yield DataFrame(schema=result_schema).arrow()

        # DEBUG: print(f"time spent converting: {convert_time/1e9}s")

    def get_dataset_schema(self) -> RelationSchema:
        from sqlalchemy import Table

        if self.schema:
            return self.schema

        # get the schema from the dataset
        # DEBUG: print("GET SQL SCHEMA:", self.dataset)
        try:
            table = Table(self.dataset, self.metadata, autoload_with=self._engine)

            self.schema = RelationSchema(
                name=table.name,
                columns=[
                    FlatColumn(
                        name=column.name,
                        type=PYTHON_TO_ORSO_MAP[column.type.python_type],
                        precision=(
                            column.type.precision
                            if hasattr(column.type, "precision")
                            and column.type.precision is not None
                            else 38
                        ),
                        scale=(
                            column.type.scale
                            if hasattr(column.type, "scale") and column.type.scale is not None
                            else 14
                        ),
                        nullable=column.nullable,
                    )
                    for column in table.columns
                ],
            )
        except Exception as err:
            if not err:
                pass
            # Fall back to getting the schema from the first few rows, this is the column names,
            # and where possible, column types.
            # DEBUG: print(f"APPROXIMATING SCHEMA OF {self.dataset} BECAUSE OF {type(err).__name__}({err})")
            from sqlalchemy.sql import text

            try:
                with self._engine.connect() as conn:
                    query = Query().SELECT("*").FROM(self.dataset).LIMIT("25")
                    # DEBUG: print("READ ROW\n", str(query))
                    row = conn.execute(text(str(query))).fetchone()._asdict()
                    # DEBUG: print("ROW:", row)
                    self.schema = RelationSchema(
                        name=self.dataset,
                        columns=[
                            FlatColumn(
                                name=column,
                                type=OrsoTypes.NULL
                                if value is None
                                else PYTHON_TO_ORSO_MAP[type(value)],
                                precision=38,
                                scale=14,
                            )
                            for column, value in row.items()
                        ],
                    )
                    # DEBUG: print("SCHEMA:", self.schema)
            except Exception as err:
                raise DatasetReadError(f"Unable to read dataset '{self.dataset}'.") from err

        return self.schema
