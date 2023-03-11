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
import logging
import time
from typing import Optional
from uuid import uuid4

from orso import DataFrame
from orso import converters

from opteryx import utils
from opteryx.exceptions import CursorInvalidStateError
from opteryx.exceptions import MissingDependencyError
from opteryx.managers.kvstores import BaseKeyValueStore
from opteryx.shared import QueryStatistics

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
        """
        A virtual connection to the Opteryx query engine.
        """
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


class Cursor(DataFrame):
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
        DataFrame.__init__(self, rows=[], schema=[])

    @property
    def query(self):
        return self._query

    @property
    def id(self):
        """The unique internal reference for this query"""
        return self._qid

    def execute(self, operation, params=None):
        if not operation:
            raise ValueError("SQL statement not found")

        if self._query is not None:
            raise CursorInvalidStateError("Cursor can only be executed once")
        self._query = operation

        from opteryx.components.query_planner import QueryPlanner

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

        self._results = converters.from_arrow(utils.arrow.rename_columns(results))
        self._rows = self._results._rows
        self._schema = self._results._schema
        self._cursor = iter(self._rows)

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

    def pandas(self, size: int = None):
        """
        Fetch the resultset as Pandas DataFrame.

        Parameters:
            size: int (optional)
                Return the head 'size' number of records.

        Returns:
            pandas DataFrame
        """
        try:
            import pandas
        except ImportError as err:  # pragma: nocover
            raise MissingDependencyError(err.name) from err
        return self.arrow(size=size).to_pandas()

    def to_df(self, size: int = None):  # pragma: no cover
        logging.warning("connection.to_df will be removed, replace with connection.pandas")
        return self.pandas(size)

    def polars(self, size: int = None):
        """
        Fetch the resultset as a polars DataFrame
        """
        try:
            import polars
        except ImportError as err:  # pragma: nocover
            raise MissingDependencyError(err.name) from err
        return polars.DataFrame(self.arrow(size=size))

    def close(self):
        """close the connection"""
        self._connection.close()

    def head(
        self,
        size: int = 10,
        colorize: bool = True,
        max_column_width: int = 30,
        table_width=True,
    ):  # pragma: no cover
        from opteryx.utils.display import ascii_table
        from opteryx.utils.display import html_table

        try:
            from IPython import get_ipython

            i_am_in_a_notebook = get_ipython() is not None
        except Exception:
            i_am_in_a_notebook = False

        if i_am_in_a_notebook:
            from IPython.display import HTML
            from IPython.display import display

            html = html_table(iter(self.fetchmany(size, as_dicts=True)), size)
            display(HTML(html))
        else:
            displayed_footer = f"({size} displayed) " if size < self.rowcount else ""
            footer = f"\n[ {self.rowcount} row{'s' if self.rowcount != 1 else ''} {displayed_footer}x {self.shape[1]} column{'s' if self.shape[1] != 1 else ''} ]"
            return (
                ascii_table(
                    self.arrow(),
                    size,
                    colorize=colorize,
                    max_column_width=max_column_width,
                    display_width=table_width,
                )
                + footer
            )
