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

import datetime
import time
from enum import Enum
from enum import auto
from functools import wraps
from itertools import chain
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
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
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryStatistics
from opteryx.shared.rolling_log import RollingLog
from opteryx.utils import sql

PROFILE_LOCATION = config.PROFILE_LOCATION
QUERY_LOG_LOCATION = config.QUERY_LOG_LOCATION
QUERY_LOG_SIZE = config.QUERY_LOG_SIZE


ROLLING_LOG = None
if QUERY_LOG_LOCATION:
    ROLLING_LOG = RollingLog(QUERY_LOG_LOCATION, max_entries=QUERY_LOG_SIZE)


class CursorState(Enum):
    INITIALIZED = auto()
    EXECUTED = auto()
    CLOSED = auto()


def require_state(required_state):
    """
    Decorator to enforce a required state before a Cursor method is called.

    The decorator takes a required_state parameter which is the state the Cursor
    must be in before the decorated method can be called. If the state condition
    is not met, an InvalidCursorStateError is raised.

    Parameters:
        required_state: The state that the cursor must be in to execute the method.

    Returns:
        A wrapper function that checks the state and calls the original function.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(obj, *args, **kwargs):
            if obj._state != required_state:
                raise InvalidCursorStateError(f"Cursor must be in {required_state} state.")
            return func(obj, *args, **kwargs)

        return wrapper

    return decorator


def transition_to(new_state):
    """
    Decorator to transition the Cursor to a new state after a method call.

    The decorator takes a new_state parameter which is the state the Cursor
    will transition to after the decorated method is called.

    Parameters:
        new_state: The new state to transition to after the method is called.

    Returns:
        A wrapper function that executes the original function and then updates the state.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(obj, *args, **kwargs):
            # Execute the original method.
            result = func(obj, *args, **kwargs)
            # Transition to the new state.
            obj._state = new_state
            return result

        return wrapper

    return decorator


class Cursor(DataFrame):
    """
    This class inherits from the orso DataFrame library to provide features such as fetch.

    This class includes custom decorators @require_state and @transition_to for state management.
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
        self._state = CursorState.INITIALIZED
        self._query_status = QueryStatus._UNDEFINED
        self._result_type = ResultType._UNDEFINED
        self._rowcount = None
        DataFrame.__init__(self, rows=[], schema=[])

    @property
    def id(self) -> str:
        """The unique internal reference for this query.

        Returns:
            The unique query identifier as a string.
        """
        return self._qid

    def _inner_execute(self, operation: str, params: Union[Iterable, Dict, None] = None) -> Any:
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

        from opteryx.planner import query_planner

        if not operation:  # pragma: no cover
            raise MissingSqlStatement("SQL provided was empty.")

        self._connection.context.history.append((operation, True, datetime.datetime.utcnow()))

        start = time.time_ns()
        plans = query_planner(
            operation=operation,
            parameters=params,
            connection=self._connection,
            qid=self.id,
            statistics=self._statistics,
        )

        try:
            start = time.time_ns()
            first_item = next(plans)
            self._statistics.time_planning += time.time_ns() - start
        except RuntimeError:  # pragma: no cover
            raise MissingSqlStatement(
                "SQL statement provided had no executable part, this may mean the statement was commented out."
            )

        plans = chain([first_item], plans)

        if ROLLING_LOG:
            ROLLING_LOG.append(operation)

        results = None
        start = time.time_ns()
        for plan in plans:
            self._statistics.time_planning += time.time_ns() - start
            results = plan.execute()
            start = time.time_ns()

        if results is not None:
            # we can't update tuples directly
            self._connection.context.history[-1] = tuple(
                True if i == 1 else value
                for i, value in enumerate(self._connection.context.history[-1])
            )
            return results

    def _execute_statements(self, operation, params: Optional[Iterable] = None):
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
            results = self._inner_execute(statement, params)
            if index < len(statements) - 1:
                for _ in results:
                    pass

        # we only return the last result set
        return results

    @require_state(CursorState.INITIALIZED)
    @transition_to(CursorState.EXECUTED)
    def execute(self, operation: str, params: Optional[Iterable] = None):
        """
        Executes the provided SQL operation, converting results to internal DataFrame format.

        Parameters:
            operation: str
                SQL operation to be executed.
            params: Iterable, optional
                Parameters for the SQL operation, defaults to None.
        """
        if hasattr(operation, "decode"):
            operation = operation.decode()
        results = self._execute_statements(operation, params)
        if results is not None:
            result_data, self._result_type = next(results, (ResultType._UNDEFINED, None))
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

    @require_state(CursorState.INITIALIZED)
    @transition_to(CursorState.EXECUTED)
    def execute_to_arrow(
        self, operation: str, params: Optional[Iterable] = None, limit: Optional[int] = None
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
        if hasattr(operation, "decode"):
            operation = operation.decode()
        results = self._execute_statements(operation, params)
        if results is not None:
            result_data, self._result_type = next(results, (ResultType._UNDEFINED, None))
            if limit is not None:
                result_data = utils.arrow.limit_records(result_data, limit)  # type: ignore
        try:
            return pyarrow.concat_tables(result_data, promote_options="permissive")
        except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError) as err:  # pragma: no cover
            # DEBUG: log (err)
            if "struct" in str(err):
                raise InconsistentSchemaError(
                    "Unable to resolve different schemas, most likely related to a STRUCT column."
                ) from err
            raise InconsistentSchemaError(
                "Unable to resolve different schemas, this may be due to uncoercible column types."
            ) from err

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

    @require_state(CursorState.EXECUTED)
    @transition_to(CursorState.CLOSED)
    def close(self):
        """
        Closes the cursor, releasing any resources and closing the associated connection.
        """
        self._connection.close()

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
        return self._state == CursorState.EXECUTED
