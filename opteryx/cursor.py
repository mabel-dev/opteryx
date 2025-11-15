# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import datetime
import time
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from uuid import uuid4

import pyarrow
from orso import DataFrame
from orso import converters
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx import config
from opteryx import utils
from opteryx.constants import QueryStatus
from opteryx.constants import ResultType
from opteryx.exceptions import InconsistentSchemaError
from opteryx.exceptions import InvalidCursorStateError
from opteryx.exceptions import MissingSqlStatement
from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryStatistics
from opteryx.utils import sql

PROFILE_LOCATION = config.PROFILE_LOCATION


class Cursor(DataFrame):
    """
    This class inherits from the orso DataFrame library to provide features such as fetch.
    """

    def __init__(self, connection):
        """
        Initializes the Cursor object, setting the initial state and binding the connection.

        Parameters:
            connection: Connection object
                The database connection object.
        """
        self.arraysize = 1
        self._connection = connection
        self._query_planner = None
        self._collected_stats = None
        self._plan = None
        self._qid = str(uuid4())
        self._statistics = QueryStatistics(self._qid)
        self._query_status = QueryStatus._UNDEFINED
        self._result_type = ResultType._UNDEFINED
        self._rowcount = None
        self._description: Optional[Tuple[Tuple[Any, ...], ...]] = None
        self._owns_connection = False
        self._closed = False
        self._executed = False
        DataFrame.__init__(self, rows=[], schema=[])

    @property
    def id(self) -> str:
        """The unique internal reference for this query.

        Returns:
            The unique query identifier as a string.
        """
        return self._qid

    def _inner_execute(
        self,
        operation: str,
        params: Union[Iterable, Dict, None] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Executes a single SQL operation within the current cursor.

        Parameters:
            operation: str
                SQL operation to be executed.
            params: Iterable/Dictionary, optional
                Parameters for the SQL operation, defaults to None.
        Returns:
            Results of the query execution.
        """
        from opteryx import system_statistics
        from opteryx.managers.execution import execute
        from opteryx.planner import query_planner

        if not operation:  # pragma: no cover
            raise MissingSqlStatement("SQL provided was empty.")

        self._connection.context.history.append(
            (operation, True, datetime.datetime.now(datetime.UTC))
        )

        start = time.time_ns()
        try:
            plan = query_planner(
                operation=operation,
                parameters=params,
                visibility_filters=visibility_filters,
                connection=self._connection,
                qid=self.id,
                statistics=self._statistics,
            )
        except RuntimeError as err:  # pragma: no cover
            raise SqlError(f"Error Executing SQL Statement ({err})") from err
        finally:
            self._statistics.time_planning += time.time_ns() - start

        results = execute(plan, statistics=self._statistics)
        system_statistics.queries_executed += 1

        if results is not None:
            # we can't update tuples directly
            entry = list(self._connection.context.history[-1])
            entry[1] = True
            self._connection.context.history[-1] = tuple(entry)
            return results

    def _execute_statements(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ):
        """
        Executes one or more SQL statements, properly handling comments, cleaning, and splitting.

        Parameters:
            operation: str
                SQL operation(s) to be executed.
            params: Iterable, optional
                Parameters for the SQL operation(s), defaults to None.

        Returns:
            Results of the query execution, if any.
        """
        self._statistics.start_time = time.time_ns()

        if hasattr(operation, "decode"):
            operation = operation.decode()

        operation = sql.remove_comments(operation)
        operation = sql.clean_statement(operation)
        statements = sql.split_sql_statements(operation)

        if len(statements) == 0:
            raise MissingSqlStatement("No statement found")

        if len(statements) > 1 and params is not None and not isinstance(params, dict) and params:
            raise UnsupportedSyntaxError(
                "Batched queries cannot be parameterized with parameter lists, use named parameters."
            )

        results = None
        for index, statement in enumerate(statements):
            results = self._inner_execute(statement, params, visibility_filters)
            if index < len(statements) - 1:
                for _ in results:
                    pass

        # we only return the last result set
        return results

    def execute(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ):
        """
        Executes the provided SQL operation, converting results to internal DataFrame format.

        Parameters:
            operation: str
                SQL operation to be executed.
            params: Iterable, optional
                Parameters for the SQL operation, defaults to None.
        """
        self._ensure_open()
        results = self._execute_statements(operation, params, visibility_filters)
        if results is not None:
            result_data, self._result_type = results
            if self._result_type == ResultType.NON_TABULAR:
                import orso

                meta_dataframe = orso.DataFrame(
                    rows=[(result_data.record_count,)],  # type: ignore
                    schema=RelationSchema(
                        name="table",
                        columns=[FlatColumn(name="rows_affected", type=OrsoTypes.INTEGER)],
                    ),
                )  # type: ignore
                self._rows = meta_dataframe._rows
                self._schema = meta_dataframe._schema

                self._rowcount = result_data.record_count  # type: ignore
                self._query_status = result_data.status  # type: ignore
            elif self._result_type == ResultType.TABULAR:
                self._rows, self._schema = converters.from_arrow(result_data)
                self._cursor = iter(self._rows)
                self._query_status = QueryStatus.SQL_SUCCESS
            else:  # pragma: no cover
                self._query_status = QueryStatus.SQL_FAILURE
            self._description = self._schema_to_description(self._schema)
        else:
            self._description = None
        self._executed = True

    @property
    def result_type(self) -> ResultType:
        return self._result_type

    @property
    def query_status(self) -> QueryStatus:
        return self._query_status

    @property
    def rowcount(self) -> int:
        if self._result_type == ResultType.TABULAR:
            return super().rowcount
        if self._result_type == ResultType.NON_TABULAR:
            return self._rowcount
        raise InvalidCursorStateError("Cursor not in valid state to return a row count.")

    @property
    def description(self) -> Optional[Tuple[Tuple[Any, ...], ...]]:
        """DBAPI-compatible column description metadata."""
        return self._description

    def execute_to_arrow(
        self,
        operation: str,
        params: Optional[Iterable] = None,
        limit: Optional[int] = None,
        visibility_filters: Optional[Dict[str, Any]] = None,
    ) -> pyarrow.Table:
        """
        Executes the SQL operation, bypassing conversion to Orso and returning directly in Arrow format.

        Parameters:
            operation: str
                SQL operation to be executed.
            params: Iterable, optional
                Parameters for the SQL operation, defaults to None.
            limit: int, optional
                Limit on the number of records to return, defaults to all records.

        Returns:
            The query results in Arrow table format.
        """
        self._ensure_open()
        results = self._execute_statements(operation, params, visibility_filters)
        if results is not None:
            result_data, self._result_type = results

            if self._result_type == ResultType.NON_TABULAR:
                import orso

                meta_dataframe = orso.DataFrame(
                    rows=[(result_data.record_count,)],  # type: ignore
                    schema=RelationSchema(
                        name="table",
                        columns=[FlatColumn(name="rows_affected", type=OrsoTypes.INTEGER)],
                    ),
                )  # type: ignore
                self._executed = True
                return meta_dataframe.arrow()

            if limit is not None:
                result_data = utils.arrow.limit_records(result_data, limit)  # type: ignore
        if isinstance(result_data, pyarrow.Table):
            self._executed = True
            return result_data
        try:
            # arrow allows duplicate column names, but not when concatting
            from itertools import chain

            first_table = next(result_data, None)
            if first_table is not None:
                column_names = first_table.column_names
                if len(column_names) != len(set(column_names)):
                    temporary_names = [f"col_{i}" for i in range(len(column_names))]
                    first_table = first_table.rename_columns(temporary_names)
                    return_table = pyarrow.concat_tables(
                        chain(
                            [first_table], (t.rename_columns(temporary_names) for t in result_data)
                        ),
                        promote_options="permissive",
                    )
                    return return_table.rename_columns(column_names)
            table = pyarrow.concat_tables(
                chain([first_table], result_data), promote_options="permissive"
            )
            self._executed = True
            return table
        except (
            pyarrow.ArrowInvalid,
            pyarrow.ArrowTypeError,
        ) as err:  # pragma: no cover
            # DEBUG: print(err)
            if "struct" in str(err):
                raise InconsistentSchemaError(
                    "Unable to resolve different schemas, most likely related to a STRUCT column."
                ) from err

            from opteryx.exceptions import DataError

            raise DataError(f"Unable to build result dataset ({err})") from err

    @property
    def stats(self) -> Dict[str, Any]:
        """
        Gets the execution statistics.

        Returns:
            Dictionary containing query execution statistics.
        """
        if self._statistics.end_time == 0:  # pragma: no cover
            self._statistics.end_time = time.time_ns()
        return self._statistics.as_dict()

    @property
    def messages(self) -> List[str]:
        """
        Gets the list of run-time warnings.

        Returns:
            List of warnings generated during query execution.
        """
        return self._statistics.messages

    def close(self):
        """
        Closes the cursor, releasing any resources.
        """
        if self._closed:
            return
        self._cursor = iter(())
        self._description = None
        connection = self._connection
        self._connection = None
        if connection is not None:
            connection._unregister_cursor(self)
            if self._owns_connection:
                connection.close()
        self._closed = True

    def __enter__(self):
        """Support context manager usage for cursors."""
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    def _close_from_connection(self):
        """Called by the Connection when it is closing."""
        self._cursor = iter(())
        self._description = None
        self._connection = None
        self._closed = True

    def __repr__(self):  # pragma: no cover
        """
        Override the Orso repr

        In notebooks we should return a table
        """
        in_a_notebook = False
        try:
            from IPython import get_ipython

            in_a_notebook = get_ipython() is not None
            if not in_a_notebook:
                return f"<opteryx.Cursor {self._state}>"
        except Exception:  # nosec
            pass
        return str(self)

    def __bool__(self):
        """
        Truthy if executed, Falsy if not executed or error
        """
        return self._executed and not self._closed

    def _ensure_open(self):
        if self._closed or self._connection is None:
            raise InvalidCursorStateError("Cursor is closed.")

    @staticmethod
    def _schema_to_description(schema: Optional[RelationSchema]):
        if schema is None or not schema.columns:
            return None
        description: List[Tuple[Any, ...]] = []
        for column in schema.columns:
            description.append(
                (
                    column.name,
                    column.type,
                    None,
                    None,
                    None,
                    None,
                    getattr(column, "nullable", None),
                )
            )
        return tuple(description)
