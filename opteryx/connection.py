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
from uuid import uuid4

from pyarrow import Table

from opteryx.exceptions import CursorInvalidStateError
from opteryx.exceptions import EmptyResultSetError
from opteryx.exceptions import SqlError
from opteryx.shared import QueryStatistics
from opteryx.managers.kvstores import BaseKeyValueStore
from opteryx import utils

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

    def commit(self):
        """exists for interface compatibility only"""
        pass

    def rollback(self):
        """exists for interface compatibility only"""
        # return AttributeError as per https://peps.python.org/pep-0249/#id48
        raise AttributeError("Opteryx does not support transactions.")


class Cursor:
    def __init__(self, connection):

        self.arraysize = 1

        self._connection = connection
        self._query = None
        self._results = None
        self._query_planner = None
        self._collected_stats = None
        self._plan = None
        self._qid = str(uuid4())
        self._statistics = QueryStatistics(self._qid)

    @property
    def id(self):
        """The unique internal reference for this query"""
        return self._qid

    def execute(self, operation, params=None):
        if self._query is not None:  # pragma: no cover
            raise CursorInvalidStateError("Cursor can only be executed once")

        from opteryx.managers.planner import QueryPlanner

        self._query_planner = QueryPlanner(
            statement=operation, cache=self._connection.cache, qid=self._qid
        )
        self._statistics.start_time = time.time_ns()
        asts = list(self._query_planner.parse_and_lex())

        results = None
        if params is None:
            params = []

        self._query_planner.test_paramcount(asts, params)

        for ast in asts:
            ast = self._query_planner.bind_ast(ast, parameters=params)
            plan = self._query_planner.create_logical_plan(ast)

            self._plan = self._query_planner.optimize_plan(plan)
            results = self._query_planner.execute(self._plan)

        self._results = results

    @property
    def rowcount(self):
        if self._results is None:  # pragma: no cover
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        if not isinstance(self._results, (Table, set)):
            self._results = utils.arrow.as_arrow(self._results)
        if self._statistics.end_time == 0:
            self._statistics.end_time = time.time_ns()
        if self._results == set():
            return 0
        return self._results.num_rows

    @property
    def shape(self):
        if self._results is None:  # pragma: no cover
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        if not isinstance(self._results, (Table, set)):
            self._results = utils.arrow.as_arrow(self._results)
        if self._statistics.end_time == 0:
            self._statistics.end_time = time.time_ns()
        if self._results == set():  # pragma: no cover
            return (0, 0)
        return self._results.shape

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

    def _do_fetch(self, limit: int, as_dicts):
        if self._results is None:  # pragma: no cover
            raise CursorInvalidStateError(CURSOR_NOT_RUN)
        if not isinstance(self._results, (Table, set)):
            self._results = utils.arrow.as_arrow(self._results)
        if self._results == set():
            raise EmptyResultSetError("Cannot fulfil request on an empty result set")
        if self._statistics.end_time == 0:
            self._statistics.end_time = time.time_ns()

        if limit == 1:
            return next(
                utils.arrow.fetchmany(
                    pages=self._results, limit=limit, as_dicts=as_dicts
                ),
                None,
            )

        return utils.arrow.fetchmany(self._results, limit=limit, as_dicts=as_dicts)

    def fetchone(self, as_dicts: bool = False) -> Optional[Dict]:
        """
        Fetch one record only.

        Parameters:
            as_dicts: boolean (optional):
                Return a dictionary, default is False, return a tuple
        """
        return self._do_fetch(1, as_dicts)

    def fetchmany(self, size=None, as_dicts: bool = False) -> List[Dict]:
        """fetch a given number of records"""
        fetch_size = self.arraysize if size is None else size
        return self._do_fetch(fetch_size, as_dicts)

    def fetchall(self, as_dicts: bool = False) -> List[Dict]:
        """fetch all matching records"""
        return self._do_fetch(-1, as_dicts)

    def arrow(self, size: int = None) -> Table:
        """
        Fetch the resultset as a pyarrow table, this is generally the fastest way to
        get the entire set of results.

        Parameters:
            size: int (optional)
                Return the head 'size' number of records.

        Returns:
            pyarrow.Table
        """
        # called 'size' to match the 'fetchmany' nomenclature
        if not isinstance(self._results, (Table, set)):
            self._results = utils.arrow.as_arrow(self._results)
        if self._results == set():  # pragma: no cover
            raise EmptyResultSetError("Cannot fulfil request on an empty result set")
        if self._statistics.end_time == 0:
            self._statistics.end_time = time.time_ns()
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
            return ascii_table(self.arrow(), size)
