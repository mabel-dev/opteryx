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
This module provides a PEP-249 familiar interface for interacting with mabel data
stores, it is not compliant with the standard:
https://www.python.org/dev/peps/pep-0249/
"""
import datetime
import time
import typing
from dataclasses import dataclass
from dataclasses import field
from uuid import uuid4

import pyarrow
from orso import DataFrame
from orso import converters
from orso.tools import random_int

from opteryx import config
from opteryx import utils
from opteryx.exceptions import InvalidCursorStateError
from opteryx.exceptions import MissingSqlStatement
from opteryx.exceptions import PermissionsError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.shared import QueryStatistics
from opteryx.shared.rolling_log import RollingLog
from opteryx.shared.variables import SystemVariables
from opteryx.shared.variables import VariableOwner
from opteryx.utils import sql

CURSOR_NOT_RUN: str = "Cursor must be in an executed state"
PROFILE_LOCATION = config.PROFILE_LOCATION

HistoryItem = typing.Tuple[str, bool, datetime.datetime]

ROLLING_LOG = None
if PROFILE_LOCATION:
    ROLLING_LOG = RollingLog(PROFILE_LOCATION + ".log")


@dataclass
class ConnectionContext:
    connection_id: int = field(default_factory=random_int, init=False)
    connected_at: datetime.datetime = field(default_factory=datetime.datetime.utcnow, init=False)
    user: str = None
    schema: str = None
    variables: dict = field(init=False)
    history: typing.List[HistoryItem] = field(default_factory=list, init=False)

    def __post_init__(self):
        # the initializer is a function rather than an empty constructure so we init here
        object.__setattr__(self, "variables", SystemVariables.copy(VariableOwner.USER))


def validate_permissions(permissions):
    """
    This is checking the validity of the permissions provided, not that the user has the
    right permissions.
    """
    from opteryx.constants.permissions import PERMISSIONS

    if permissions is None:
        permissions = PERMISSIONS
    permissions = set(permissions)
    if permissions.intersection(PERMISSIONS) == set():
        raise PermissionsError("No valid permissions presented.")
    if not permissions.issubset(PERMISSIONS):
        raise PermissionsError(
            f"Invalid permissions presented - {PERMISSIONS.difference(permissions)}"
        )
    return permissions


class Connection:
    """
    A connection
    """

    def __init__(
        self,
        *,
        permissions: typing.Union[typing.Iterable, None] = None,
        **kwargs,
    ):
        """
        A virtual connection to the Opteryx query engine.
        """
        self._kwargs = kwargs

        self.context = ConnectionContext()

        # check the permissions we've been given are valid permissions
        self.permissions = validate_permissions(permissions)

    def cursor(self):
        """return a cursor object"""
        return Cursor(self)

    def close(self):
        """exists for interface compatibility only"""

    def commit(self):
        """exists for interface compatibility only"""

    def rollback(self):
        """exists for interface compatibility only"""
        # return AttributeError as per https://peps.python.org/pep-0249/#id48
        raise AttributeError("Opteryx does not support transactions.")


class Cursor(DataFrame):
    def __init__(self, connection):
        self.arraysize = 1
        self._connection = connection
        self._query = None
        self._query_planner = None
        self._collected_stats = None
        self._plan = None
        self._qid = str(uuid4())
        self._statistics = QueryStatistics(self._qid)
        DataFrame.__init__(self, rows=[], schema=[])

    @property
    def query(self):
        return self._query

    @property
    def id(self):
        """The unique internal reference for this query"""
        return self._qid

    def _inner_execute(self, operation, params=None):
        """
        Executes a single SQL operation within the current cursor.

        Parameters:
            operation: str
                SQL operation to be executed.
            params: Union[Dict, Tuple], optional
                Parameters for the SQL operation, defaults to None.
        Returns:
            Results of the query execution.
        """

        from opteryx.components import query_planner

        if not operation:
            raise MissingSqlStatement("SQL statement not found")

        self._connection.context.history.append((operation, True, datetime.datetime.utcnow()))
        plans = query_planner(
            operation=operation, parameters=params, connection=self._connection, qid=self.id
        )

        if ROLLING_LOG:
            ROLLING_LOG.append(operation)

        results = None
        for plan in plans:
            results = plan.execute()

        if results is not None:
            # we can't update tuples directly
            self._connection.context.history[-1] = tuple(
                True if i == 1 else value
                for i, value in enumerate(self._connection.context.history[-1])
            )
            return results

    def _execute_statements(self, operation, params=None):
        """
        Executes one or more SQL statements, properly handling comments, cleaning, and splitting.

        Parameters:
            operation: str
                SQL operation(s) to be executed.
            params: Union[Dict, Tuple], optional
                Parameters for the SQL operation(s), defaults to None.
        Returns:
            Results of the query execution.
        """
        statements = sql.remove_comments(operation)
        statements = sql.clean_statement(statements)
        statements = sql.split_sql_statements(statements)

        if self._query is not None:
            raise InvalidCursorStateError("Cursor can only be executed once")
        self._query = operation

        if len(statements) == 0:
            raise MissingSqlStatement("No statement found")

        if len(statements) > 1 and params is not None:
            raise UnsupportedSyntaxError("Batched queries cannot be parameterized.")

        results = None
        for index, statement in enumerate(statements):
            results = self._inner_execute(statement, params)
            if index < len(statements) - 1:
                for _ in results:
                    pass

        return results

    def execute(self, operation, params=None):
        if hasattr(operation, "decode"):
            operation = operation.decode()
        results = self._execute_statements(operation, params)
        if results is not None:
            self._rows, self._schema = converters.from_arrow(results)
            self._cursor = iter(self._rows)

    def execute_to_arrow(self, operation, params=None, limit=None):
        """
        Bypass conversion to Orso and return directly in Arrow format
        """
        results = self._execute_statements(operation, params)
        if results is not None:
            if limit is not None:
                results = utils.arrow.limit_records(results, limit)
        return pyarrow.concat_tables(results, promote=True)

    @property
    def stats(self):
        """execution statistics"""
        if self._statistics.end_time == 0:  # pragma: no cover
            self._statistics.end_time = time.time_ns()
        return self._statistics.as_dict()

    @property
    def messages(self) -> list:
        """list of run-time warnings"""
        return self._statistics.messages

    def close(self):
        """close the connection"""
        self._connection.close()
