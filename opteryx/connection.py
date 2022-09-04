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

from decimal import Decimal
from typing import Dict, List, Optional

from pyarrow import Table

from opteryx.exceptions import CursorInvalidStateError, ProgrammingError, SqlError
from opteryx.managers.cache import BaseBufferCache
from opteryx.models import QueryStatistics
from opteryx.utils import arrow

CURSOR_NOT_RUN = "Cursor must be in an executed state"


class Connection:
    """
    A connection
    """

    def __init__(
        self,
        *,
        cache: Optional[BaseBufferCache] = None,
        **kwargs,
    ):
        self._results = None
        self._cache = cache
        self._kwargs = kwargs

    def cursor(self):
        """return a cursor object"""
        return Cursor(self)

    def close(self):
        """exists for interface compatibility only"""
        pass


class Cursor:
    def __init__(self, connection):
        self._connection = connection
        self._query = None
        self.arraysize = 1
        self._stats = QueryStatistics()
        self._results = None

        self._query_plan = None

    def _format_prepared_param(self, param):
        """
        Formats parameters to be passed to a Query.
        """

        if param is None:
            return "NULL"

        if isinstance(param, bool):
            return "TRUE" if param else "FALSE"

        if isinstance(param, (float, int, Decimal)):
            return f"{param}"

        if isinstance(param, str):
            # if I have no apostrophes, use them as the delimiter
            if param.find("'") == -1:
                delimited = param.replace('"', '""')
                return f"'{delimited}'"
            # otherwise use quotes
            delimited = param.replace('"', '""')
            return f'"{delimited}"'

        if isinstance(param, datetime.datetime):
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
            return f"'{datetime_str}'"

        if isinstance(param, (list, tuple, set)):
            return f"({','.join(map(self._format_prepared_param, param))})"

        raise SqlError(f"Query parameter of type '{type(param)}' is not supported.")

    def execute(self, operation, params=None):
        if self._query is not None:
            raise CursorInvalidStateError("Cursor can only be executed once")

        self._stats.start_time = time.time_ns()

        if params:
            if not isinstance(params, (list, tuple)):
                raise ProgrammingError(
                    "params must be a list or tuple containing the query parameter values"
                )

            for param in params:
                if operation.find("%s") == -1:
                    # we have too few placeholders
                    raise ProgrammingError(
                        "Number of placeholders and number of parameters must match."
                    )
                operation = operation.replace(
                    "%s", self._format_prepared_param(param), 1
                )
            if operation.find("%s") != -1:
                # we have too many placeholders
                raise ProgrammingError(
                    "Number of placeholders and number of parameters must match."
                )

        # circular imports
        from opteryx.managers.query.planner import QueryPlanner

        self._query_plan = QueryPlanner(
            statistics=self._stats,
            cache=self._connection._cache,
        )
        self._query_plan.create_plan(sql=operation)

        # how long have we spent planning
        self._stats.time_planning = time.time_ns() - self._stats.start_time

        self._results = self._query_plan.execute()

    @property
    def rowcount(self):
        if self._results is None:
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        return self._results.count()

    @property
    def shape(self):
        if self._results is None:
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        return self._results.shape

    @property
    def stats(self):
        """execution statistics"""
        self._stats.end_time = time.time_ns()
        return self._stats.as_dict()

    @property
    def has_warnings(self):
        """do I have warnings"""
        return self._stats.has_warnings

    @property
    def warnings(self):
        """list of run-time warnings"""
        return self._stats.warnings

    def fetchone(self) -> Optional[Dict]:
        """fetch one record only"""
        if self._results is None:
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        return arrow.fetchone(self._results)

    def fetchmany(self, size=None) -> List[Dict]:
        """fetch a given number of records"""
        fetch_size = self.arraysize if size is None else size
        if self._results is None:
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        return arrow.fetchmany(self._results, fetch_size)

    def fetchall(self) -> List[Dict]:
        """fetch all matching records"""
        if self._results is None:
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        return arrow.fetchall(self._results)

    def to_arrow(self) -> Table:
        """fetch all matching records as a pyarrow table"""
        return arrow.as_arrow(self._results)

    def close(self):
        """close the connection"""
        self._connection.close()

    def head(self, size: int = 10):  # pragma: no cover

        from opteryx.utils.display import html_table, ascii_table

        try:
            from IPython import get_ipython

            i_am_in_a_notebook = get_ipython() is not None
        except Exception:
            i_am_in_a_notebook = False

        if i_am_in_a_notebook:
            from IPython.display import HTML, display

            html = html_table(iter(self.fetchmany(size)), size)
            display(HTML(html))
        else:
            return ascii_table(iter(self.fetchmany(size)), size)
