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
import time
from typing import Dict, Optional, List, Union, Tuple
from decimal import Decimal
from opteryx.storage import BaseStorageAdapter, BaseBufferCache, BasePartitionScheme
from opteryx.storage.adapters import DiskStorage
from opteryx.engine import QueryStatistics, QueryPlanner
from opteryx.storage.schemes import DefaultPartitionScheme, MabelPartitionScheme
from opteryx.utils.pyarrow import fetchmany, fetchall, fetchone


class Connection:
    """
    A connection
    """

    def __init__(
        self,
        *,
        reader: Optional[BaseStorageAdapter] = None,
        partition_scheme: Union[str, Tuple, BasePartitionScheme] = "mabel",
        cache: Optional[BaseBufferCache] = None,
        **kwargs,
    ):
        self._reader = reader
        if reader is None:
            self._reader = DiskStorage()

        self._partition_scheme = partition_scheme
        if isinstance(partition_scheme, (str, tuple, list, set)):
            if str(partition_scheme).lower() == "mabel":
                self._partition_scheme = MabelPartitionScheme()
            else:
                self._partition_scheme = DefaultPartitionScheme(partition_scheme)
        if partition_scheme is None:
            self._partition_scheme = DefaultPartitionScheme("")

        self._cache = cache

        self._kwargs = kwargs

    def cursor(self):
        return Cursor(self)

    def close(self):
        pass


class Cursor:
    def __init__(self, connection):
        self._connection = connection
        self._query = None
        self.arraysize = 1
        self._stats = QueryStatistics()

    def _format_prepared_param(self, param):
        """
        Formats parameters to be passed to a Query.
        """
        import uuid
        import datetime

        if param is None:
            return "NULL"

        if isinstance(param, bool):
            return "TRUE" if param else "FALSE"

        if isinstance(param, (float, int, Decimal)):
            return f'NUMERIC("{param}")'

        if isinstance(param, str):
            # if I have no apostrophes, use them as the delimiter
            if param.find("'") == -1:
                delimited = param.replace('"', '""')
                return f"'{delimited}'"
            # otherwise use quotes
            delimited = param.replace('"', '""')
            return f'"{delimited}"'

        if isinstance(param, bytes):
            return "X'%s'" % param.hex()

        if isinstance(param, datetime.datetime):
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
            return f'TIMESTAMP("{datetime_str}")'

        if isinstance(param, (list, tuple, set)):
            return "(%s)" % ",".join(map(self._format_prepared_param, param))

        if isinstance(param, dict):
            keys = list(param.keys())
            if any(type(k) != str for k in keys):
                raise Exception("STRUCT keys must be strings")
            return "{%s}" % ",".join(
                f'"{k}":{self._format_prepared_param(v)}' for k, v in param.items()
            )

        raise Exception("Query parameter of type '%s' is not supported." % type(param))

    def execute(self, operation, params=None):
        if self._query is not None:
            raise Exception("Cursor can only be executed once")

        self._stats.start_time = time.time_ns()

        if params:
            if not isinstance(params, (list, tuple)):
                raise Exception(
                    "params must be a list or tuple containing the query parameter values"
                )

            for param in params:
                if operation.find("%s") == -1:
                    # we have too few placeholders
                    raise Exception(
                        "Number of placeholders and number of parameters must match."
                    )
                operation = operation.replace(
                    "%s", self._format_prepared_param(param), 1
                )
            if operation.find("%s") != -1:
                # we have too many placeholders
                raise Exception(
                    "Number of placeholders and number of parameters must match."
                )

        self._query_plan = QueryPlanner(
            self._stats,
            self._connection._reader,
            self._connection._partition_scheme,
        )
        self._query_plan.create_plan(sql=operation)
        # optimize the plan

        # how long have we spent planning
        self._stats.time_planning = time.time_ns() - self._stats.start_time
        # self._execute = QueryExecutor(QueryPlan)
        self._results = self._query_plan.execute()

    @property
    def rowcount(self):
        if self._results is None:
            raise Exception("Cursor must be executed first")
        return self._results.count()

    @property
    def shape(self):
        if self._results is None:
            raise Exception("Cursor must be executed first")
        return self._results.shape

    @property
    def stats(self):
        self._stats.end_time = time.time_ns()
        return self._stats.as_dict()

    def fetchone(self) -> Optional[Dict]:
        if self._results is None:
            raise Exception("Cursor must be executed first")
        return fetchone(self._results)

    def fetchmany(self, size=None) -> List[Dict]:
        fetch_size = self.arraysize if size is None else size
        if self._results is None:
            raise Exception("Cursor must be executed first")
        return fetchmany(self._results, fetch_size)

    def fetchall(self) -> List[Dict]:
        if self._query is None:
            raise Exception("Cursor must be executed first")
        return fetchall(self._results)

    def close(self):
        self._connection.close()

    def __repr__(self):  # pragma: no cover

        try:
            from IPython import get_ipython

            i_am_in_a_notebook = get_ipython() is not None
        except Exception:
            i_am_in_a_notebook = False

        if i_am_in_a_notebook():
            from IPython.display import HTML, display
            from opteryx.utils import display

            html = display.html_table(iter(self._iterator), 10)
            display(HTML(html))
            return ""  # __repr__ must return something
        else:
            from opteryx.utils import display

            return display.ascii_table(iter(self._iterator), 10)
