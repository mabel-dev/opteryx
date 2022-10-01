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

from opteryx.exceptions import CursorInvalidStateError, SqlError
from opteryx.managers.kvstores import BaseKeyValueStore
from opteryx.utils import arrow

CURSOR_NOT_RUN = "Cursor must be in an executed state"


class Connection:
    """
    A connection
    """

    def __init__(
        self,
        *,
        cache: Optional[BaseKeyValueStore] = None,
        **kwargs,
    ):
        self._results = None
        self.cache = cache
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
        self._results = None
        self._query_planner = None

    def _format_prepared_param(self, param):
        """
        Formats parameters to be passed to a Query.
        """

        if param is None:  # pragma: no cover
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

        raise SqlError(
            f"Query parameter of type '{type(param)}' is not supported."
        )  # pragma: no cover

    def execute(self, operation, params=None):
        if self._query is not None:  # pragma: no cover
            raise CursorInvalidStateError("Cursor can only be executed once")

        from opteryx.managers.planner import QueryPlanner

        self._query_planner = QueryPlanner(
            statement=operation, cache=self._connection.cache
        )
        self._query_planner.properties.statistics.start_time = time.time_ns()
        asts = self._query_planner.parse_and_lex()

        results = None
        for ast in asts:
            ast = self._query_planner.bind_ast(ast, parameters=params)
            plan = self._query_planner.create_logical_plan(ast)

            plan = self._query_planner.optimize_plan(plan)
            results = self._query_planner.execute(plan)

        self._results = results

    @property
    def rowcount(self):
        if self._results is None:  # pragma: no cover
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        if not isinstance(self._results, Table):
            self._results = arrow.as_arrow(self._results)
        return self._results.num_rows

    @property
    def shape(self):
        if self._results is None:  # pragma: no cover
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        if not isinstance(self._results, Table):
            self._results = arrow.as_arrow(self._results)
        return self._results.shape

    @property
    def stats(self):
        """execution statistics"""
        self._query_planner.properties.statistics.end_time = time.time_ns()
        return self._query_planner.properties.statistics.as_dict()

    @property
    def has_warnings(self):
        """do I have warnings"""
        return self._query_planner.properties.statistics.has_warnings

    @property
    def warnings(self):
        """list of run-time warnings"""
        return self._query_planner.properties.statistics.warnings

    def fetchone(self, as_dicts: bool = False) -> Optional[Dict]:
        """fetch one record only"""
        if self._results is None:  # pragma: no cover
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        return arrow.fetchone(self._results, as_dicts=as_dicts)

    def fetchmany(self, size=None, as_dicts: bool = False) -> List[Dict]:
        """fetch a given number of records"""
        fetch_size = self.arraysize if size is None else size
        if self._results is None:  # pragma: no cover
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        return arrow.fetchmany(self._results, limit=fetch_size, as_dicts=as_dicts)

    def fetchall(self, as_dicts: bool = False) -> List[Dict]:
        """fetch all matching records"""
        if self._results is None:  # pragma: no cover
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        return arrow.fetchall(self._results, as_dicts=as_dicts)

    def as_arrow(self, size: int = None) -> Table:
        """fetch all matching records as a pyarrow table"""
        # called 'size' to match the 'fetchmany' nomenclature
        if not isinstance(self._results, Table):
            self._results = arrow.as_arrow(self._results)
        if size:
            return self._results.slice(offset=0, length=size)
        return self._results

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

            html = html_table(iter(self.fetchmany(size, as_dicts=True)), size)
            display(HTML(html))
        else:
            return ascii_table(iter(self.fetchmany(size, as_dicts=True)), size)
