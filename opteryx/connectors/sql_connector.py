# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The SQL Connector downloads data from remote servers and converts them
to pyarrow tables so they can be processed as per any other data source.
"""

# ensure json import present
import json
import time
from decimal import Decimal
from decimal import InvalidOperation
from decimal import localcontext
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

from opteryx.compiled.structures.relation_statistics import RelationStatistics
from opteryx.config import OPTERYX_DEBUG
from opteryx.config import features
from opteryx.connectors.base.base_connector import DEFAULT_MORSEL_SIZE
from opteryx.connectors.base.base_connector import INITIAL_CHUNK_SIZE
from opteryx.connectors.base.base_connector import MIN_CHUNK_SIZE
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import LimitPushable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.connectors.capabilities import Statistics
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


class SqlConnector(BaseConnector, LimitPushable, PredicatePushable, Statistics):
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

    def _quote_identifier(self, identifier: str) -> str:
        preparer = self._engine.dialect.identifier_preparer
        return preparer.quote(identifier)

    def _quote_dataset_name(self, dataset: str) -> str:
        parts = [part for part in dataset.split(".") if part]
        if parts:
            return ".".join(self._quote_identifier(part) for part in parts)
        return self._quote_identifier(dataset)

    def _get_declared_column_types(self, table_name: str) -> dict[str, str]:
        dialect = self._engine.dialect.name.lower()
        if dialect != "sqlite":
            return {}

        from sqlalchemy.sql import text

        try:
            pragma = text(f"PRAGMA main.table_info({self._quote_identifier(table_name)})")
            with self._engine.connect() as conn:
                rows = conn.execute(pragma).mappings().all()
            return {row["name"]: (row.get("type") or "").upper() for row in rows if row.get("name")}
        except Exception:
            return {}

    def _map_column_type(self, column, declared_type: str | None) -> Any:
        if declared_type and "ARRAY" in declared_type:
            return OrsoTypes.ARRAY
        return PYTHON_TO_ORSO_MAP.get(getattr(column.type, "python_type", None), OrsoTypes.VARCHAR)

    def _deserialize_array_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (list, tuple)):
            return list(value)
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
            try:
                arr = value.tolist()
            except (AttributeError, TypeError, ValueError):
                arr = None
            if isinstance(arr, (list, tuple)):
                return list(arr)
        if isinstance(value, bytes):
            try:
                text = value.decode("utf-8")
            except UnicodeDecodeError:
                return list(value)
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return list(value)
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return value
            if isinstance(parsed, (list, tuple)):
                return list(parsed)
        return value

    def __init__(self, *args, connection: str = None, engine=None, **kwargs):
        BaseConnector.__init__(self, **kwargs)
        LimitPushable.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        Statistics.__init__(self, **kwargs)

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

        # If the optimizer pushed an entire SQL fragment, use it directly.
        # `pushed_sql` should be a full SELECT ... FROM ... [WHERE ...] string
        # and `pushed_params` a dict of parameters.
        pushed_sql = getattr(self, "pushed_sql", None)
        pushed_params = getattr(self, "pushed_params", None) or {}

        if pushed_sql is not None:
            # When pushed_sql is present we ignore columns/predicates/limit
            # because the optimizer is responsible for correctness. We still
            # use the relation schema for conversion.
            query_builder = None
            sql_text = pushed_sql
        else:
            query_builder = Query().FROM(self._quote_dataset_name(self.dataset))

        # Update the SQL and the target morsel schema if we've pushed a projection
        if pushed_sql is None:
            if columns:
                quoted_column_names = [
                    self._quote_identifier(col.schema_column.name) for col in columns
                ]
                actual_column_names = [col.schema_column.name for col in columns]
                query_builder.add("SELECT", *quoted_column_names)
                result_schema.columns = [  # type:ignore
                    col
                    for col in self.schema.columns  # type:ignore
                    if col.name in actual_column_names  # type:ignore
                ]
            else:
                query_builder.add("SELECT", "1")
                self.schema.columns = [ConstantColumn(name="1", value=1, type=OrsoTypes.INTEGER)]  # type:ignore

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

        if pushed_sql is None and limit is not None:
            query_builder.LIMIT(str(limit))

        struct_column_indices = [
            idx
            for idx, column in enumerate(result_schema.columns)
            if getattr(column, "type", None) == OrsoTypes.STRUCT
        ]
        decimal_column_indices = [
            idx
            for idx, column in enumerate(result_schema.columns)
            if getattr(column, "type", None) == OrsoTypes.DECIMAL
        ]
        boolean_column_indices = [
            idx
            for idx, column in enumerate(result_schema.columns)
            if getattr(column, "type", None) == OrsoTypes.BOOLEAN
        ]
        array_column_indices = [
            idx
            for idx, column in enumerate(result_schema.columns)
            if getattr(column, "type", None) == OrsoTypes.ARRAY
        ]

        needs_struct_conversion = bool(struct_column_indices)
        decimal_adjustments = []
        for idx in decimal_column_indices:
            column = result_schema.columns[idx]
            scale = getattr(column, "scale", None)
            precision = getattr(column, "precision", None)
            quantizer = None

            if isinstance(scale, int):
                try:
                    quantizer = Decimal(1).scaleb(-scale)
                except Exception:  # pragma: no cover - defensive
                    quantizer = None
            elif scale is not None:
                try:
                    quantizer = Decimal(1).scaleb(-int(scale))
                except Exception:  # pragma: no cover - defensive
                    quantizer = None

            decimal_adjustments.append((idx, quantizer, precision))

        needs_decimal_conversion = bool(decimal_adjustments)
        needs_boolean_conversion = bool(boolean_column_indices)
        needs_array_conversion = bool(array_column_indices)
        null_cleanup_types = {
            OrsoTypes.INTEGER,
            OrsoTypes.DOUBLE,
            OrsoTypes.DECIMAL,
            OrsoTypes.TIMESTAMP,
            OrsoTypes.DATE,
            OrsoTypes.TIME,
        }
        null_like_strings = {"NULL", "NONE"}
        needs_null_string_cleanup = any(
            getattr(column, "type", None) in null_cleanup_types for column in result_schema.columns
        )

        at_least_once = False

        convert_time = 0.0

        with self._engine.connect() as conn:
            # DEBUG: print("READ DATASET\n", str(query_builder))
            # DEBUG: print("PARAMETERS\n", parameters)
            # Execution Options allows us to handle datasets larger than memory
            if pushed_sql is not None:
                # Use the optimizer-provided SQL and parameters.
                # We allow the optimizer to have provided parameter placeholders
                # compatible with SQLAlchemy text().
                result = conn.execution_options(stream_results=True, max_row_buffer=25000).execute(
                    text(sql_text), parameters=pushed_params
                )
            else:
                result = conn.execution_options(stream_results=True, max_row_buffer=25000).execute(
                    text(str(query_builder)), parameters=parameters
                )

            while True:
                t = time.monotonic_ns()
                batch_rows = result.fetchmany(self.chunk_size)
                self.statistics.time_waiting_sql = time.monotonic_ns() - t
                if not batch_rows:
                    break

                if (
                    needs_struct_conversion
                    or needs_decimal_conversion
                    or needs_boolean_conversion
                    or needs_array_conversion
                    or needs_null_string_cleanup
                ):
                    processed_rows = []
                    for row in batch_rows:
                        fields = list(row)
                        if needs_array_conversion:
                            for index in array_column_indices:
                                fields[index] = self._deserialize_array_value(fields[index])
                        if needs_null_string_cleanup:
                            for idx, column in enumerate(result_schema.columns):
                                value = fields[idx]
                                if (
                                    isinstance(value, str)
                                    and value.strip().upper() in null_like_strings
                                    and getattr(column, "type", None) in null_cleanup_types
                                ):
                                    fields[idx] = None
                        if needs_struct_conversion:
                            for index in struct_column_indices:
                                value = fields[index]
                                if isinstance(value, dict):
                                    fields[index] = orjson.dumps(value)
                        if needs_decimal_conversion:
                            for index, quantizer, precision in decimal_adjustments:
                                value = fields[index]
                                if value is None:
                                    continue
                                if isinstance(value, bytes):
                                    value = value.decode("utf-8", errors="ignore")
                                if not isinstance(value, Decimal):
                                    value = Decimal(str(value))
                                if quantizer is not None and not value.is_nan():
                                    digits = len(value.as_tuple().digits)
                                    with localcontext() as ctx:
                                        if precision:
                                            ctx.prec = max(ctx.prec, int(precision))
                                        else:
                                            ctx.prec = max(ctx.prec, digits)
                                        try:
                                            value = value.quantize(quantizer)
                                        except InvalidOperation:
                                            ctx.prec = max(ctx.prec, digits)
                                            value = value.quantize(quantizer)
                                fields[index] = value
                        if needs_boolean_conversion:
                            for index in boolean_column_indices:
                                value = fields[index]
                                if value is None or isinstance(value, bool):
                                    continue
                                if isinstance(value, (int, Decimal)):
                                    fields[index] = bool(value)
                                    continue
                                if isinstance(value, bytes):
                                    value = value.decode("utf-8", errors="ignore")
                                if isinstance(value, str):
                                    normalized = value.strip().lower()
                                    if normalized in {"true", "t", "1", "y", "yes"}:
                                        fields[index] = True
                                    elif normalized in {"false", "f", "0", "n", "no"}:
                                        fields[index] = False
                        processed_rows.append(tuple(fields))
                    rows = processed_rows
                else:
                    rows = map(tuple, batch_rows)

                # convert the SqlAlchemy Results to Arrow using Orso
                b = time.monotonic_ns()
                morsel = DataFrame(schema=result_schema, rows=rows).arrow()
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

    def collect_relation_stats(self) -> RelationStatistics:
        if features.disable_sql_statistics_gathering:
            return RelationStatistics()

        from sqlalchemy import inspect
        from sqlalchemy.sql import text

        stats = RelationStatistics()
        dialect = self._engine.dialect.name.lower()

        # Extract table name for stats queries (some need just the table name)
        table_name_only = self.dataset.split(".")[-1] if "." in self.dataset else self.dataset

        if dialect == "postgresql":
            with self._engine.connect() as conn:
                row_est = conn.execute(
                    text("SELECT reltuples::BIGINT FROM pg_class WHERE relname = :t"),
                    {"t": table_name_only},
                ).scalar()
                if row_est is not None:
                    stats.record_count_estimate = int(row_est)

                pg_stats = (
                    conn.execute(
                        text("""
                    SELECT attname, n_distinct, null_frac, histogram_bounds
                    FROM pg_stats
                    WHERE tablename = :t
                """),
                        {"t": table_name_only},
                    )
                    .mappings()
                    .all()
                )

            for row in pg_stats:
                col = row["attname"]
                stats.cardinality_estimate[col] = (
                    int(row["n_distinct"]) if row["n_distinct"] > 0 else 0
                )
                if row_est is not None:
                    stats.null_count[col] = int(row["null_frac"] * row_est)
                bounds = row["histogram_bounds"]
                if bounds and isinstance(bounds, list) and len(bounds) >= 2:
                    stats.lower_bounds[col] = bounds[0]
                    stats.upper_bounds[col] = bounds[-1]

        elif dialect in {"duckdb", "sqlite", "mysql"}:
            # fallback: query full stats for small/embedded engines
            try:
                # Try with full dataset name first
                columns = inspect(self._engine).get_columns(self.dataset)
            except Exception:
                try:
                    # Fall back to table name only
                    columns = inspect(self._engine).get_columns(table_name_only)
                except Exception:
                    # Some SQLAlchemy/DBAPI combinations (notably certain
                    # versions of the duckdb engine) attempt to run
                    # Postgres-specific catalog queries during reflection
                    # which may not be present in the underlying engine and
                    # will raise. In that case, gracefully fall back to a
                    # lightweight stats query (COUNT) so we can still
                    # provide basic relation statistics instead of failing
                    # the whole schema discovery.
                    try:
                        quoted_dataset = self._quote_dataset_name(self.dataset)
                        with self._engine.connect() as conn:
                            result = conn.execute(
                                text(f"SELECT COUNT(*) AS count FROM {quoted_dataset}")
                            ).fetchone()
                        count = result[0] if result is not None else None
                        if count is not None:
                            stats.record_count = int(count)
                            stats.record_count_estimate = int(count)
                        return stats
                    except Exception:
                        # Give up and return empty stats rather than raising
                        return stats

            declared_types = self._get_declared_column_types(table_name_only)

            numeric_cols = [
                col["name"]
                for col in columns
                if str(col["type"]).lower()
                in {"integer", "bigint", "float", "real", "numeric", "double"}
                and "ARRAY" not in declared_types.get(col["name"], "")
            ]

            # Build dynamic query with quoted identifiers
            parts = ["COUNT(*) AS count"]
            for col in numeric_cols:
                quoted_col = self._quote_identifier(col)
                alias_min = f"min_{col}"
                alias_max = f"max_{col}"
                parts.extend(
                    [f"MIN({quoted_col}) AS {alias_min}", f"MAX({quoted_col}) AS {alias_max}"]
                )
            quoted_dataset = self._quote_dataset_name(self.dataset)
            q = f"SELECT {', '.join(parts)} FROM {quoted_dataset}"
            with self._engine.connect() as conn:
                # DEBUG: print("READ STATS\n", str(q))
                result = conn.execute(text(q)).fetchone()._asdict()

            stats.record_count = result["count"]
            stats.record_count_estimate = result["count"]
            for col in numeric_cols:
                stats.lower_bounds[col] = result[f"min_{col}"]
                stats.upper_bounds[col] = result[f"max_{col}"]

        return stats

    def get_dataset_schema(self) -> RelationSchema:
        from sqlalchemy import Table

        if self.schema:
            return self.schema

        # get the schema from the dataset
        # DEBUG: print("GET SQL SCHEMA:", self.dataset)
        schema_name = None
        table_name = self.dataset
        if "." in self.dataset:
            parts = self.dataset.split(".")
            if len(parts) == 2:
                schema_name, table_name = parts
            elif len(parts) > 2:
                # Handle database.schema.table format (take last two parts)
                schema_name, table_name = parts[-2], parts[-1]

        declared_column_types = self._get_declared_column_types(table_name)

        try:
            table = Table(table_name, self.metadata, schema=schema_name, autoload_with=self._engine)

            column_defs = []
            for column in table.columns:
                column_type = self._map_column_type(column, declared_column_types.get(column.name))
                column_defs.append(
                    FlatColumn(
                        name=column.name,
                        type=column_type,
                        element_type=OrsoTypes.VARCHAR if column_type == OrsoTypes.ARRAY else None,
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
                )
            self.schema = RelationSchema(name=table.name, columns=column_defs)
            # DEBUG: print(f"Successfully loaded schema for {self.dataset} with {len(table.columns)} columns")
        except Exception as err:
            # Try again with quoted identifiers if the first attempt fails
            # This handles case sensitivity and reserved word issues
            try:
                from sqlalchemy import quoted_name

                quoted_table_name = quoted_name(table_name, quote=True)
                quoted_schema_name = quoted_name(schema_name, quote=True) if schema_name else None

                table = Table(
                    quoted_table_name,
                    self.metadata,
                    schema=quoted_schema_name,
                    autoload_with=self._engine,
                )

                column_defs = []
                for column in table.columns:
                    column_type = self._map_column_type(
                        column, declared_column_types.get(column.name)
                    )
                    column_defs.append(
                        FlatColumn(
                            name=column.name,
                            type=column_type,
                            element_type=OrsoTypes.VARCHAR
                            if column_type == OrsoTypes.ARRAY
                            else None,
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
                    )
                self.schema = RelationSchema(name=table.name, columns=column_defs)
                # DEBUG: print(f"Successfully loaded schema for {self.dataset} with {len(table.columns)} columns using quoted identifiers")
            except Exception as inner_err:
                # DEBUG: print(f"APPROXIMATING SCHEMA OF {self.dataset} BECAUSE OF {type(err).__name__}({err}) AND {type(inner_err).__name__}({inner_err})")
                # Fall back to getting the schema from the first few rows, this is the column names,
                # and where possible, column types.
                from sqlalchemy.sql import text

                try:
                    with self._engine.connect() as conn:
                        query = Query().SELECT("*").FROM(self.dataset).LIMIT("25")
                        rows = conn.execute(text(str(query))).mappings().fetchmany(25)

                        if not rows:
                            raise DatasetReadError(f"No rows found in dataset '{self.dataset}'.")

                        column_types = {}

                        # Walk rows until we find a non-null for each column
                        for row_dict in rows:
                            schema_name = None
                            table_name = self.dataset
                            if "." in self.dataset:
                                parts = self.dataset.split(".")
                                if len(parts) == 2:
                                    schema_name, table_name = parts
                                elif len(parts) > 2:
                                    # Handle database.schema.table format (take last two parts)
                                    schema_name, table_name = parts[-2], parts[-1]
                            declared_column_types = self._get_declared_column_types(table_name)
                            for column, value in row_dict.items():
                                if column not in column_types:
                                    column_types[column] = None
                                if column_types[column] is None and value is not None:
                                    column_types[column] = PYTHON_TO_ORSO_MAP.get(
                                        type(value), OrsoTypes.NULL
                                    )

                        column_defs = []
                        for col in column_types:
                            column_type = column_types[col] or OrsoTypes.NULL
                            if "ARRAY" in declared_column_types.get(col, ""):
                                column_type = OrsoTypes.ARRAY
                            column_defs.append(
                                FlatColumn(
                                    name=col,
                                    type=column_type,
                                    element_type=OrsoTypes.VARCHAR
                                    if column_type == OrsoTypes.ARRAY
                                    else None,
                                    precision=38,
                                    scale=14,
                                )
                            )
                        self.schema = RelationSchema(name=self.dataset, columns=column_defs)

                        # DEBUG: print("SCHEMA:", self.schema)
                except Exception as final_err:
                    raise DatasetReadError(
                        f"Unable to read dataset '{self.dataset}'."
                    ) from final_err

        self.schema.relation_statistics = self.collect_relation_stats()

        return self.schema
