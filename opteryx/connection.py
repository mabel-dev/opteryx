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

from typing import Dict, Optional, List, Any
from opteryx import constants
from opteryx.query import OpteryxQuery


class Connection:
    """
    A connection
    """

    def __init__(
        self,
        host,
        port=constants.DEFAULT_PORT,
        user=None,
        source=constants.DEFAULT_SOURCE,
        catalog=constants.DEFAULT_CATALOG,
        schema=constants.DEFAULT_SCHEMA,
        auth=constants.DEFAULT_AUTH,
        max_attempts=constants.DEFAULT_MAX_ATTEMPTS,
        request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.source = source
        self.catalog = catalog
        self.schema = schema
        self.auth = auth
        self.max_attempts = max_attempts
        self.request_timeout = request_timeout

    def cursor(self):
        return Cursor(self)

    def close(self):
        pass


class Cursor:
    def __init__(self, connection):
        self._connection = connection
        self._query = None
        self.arraysize = 1

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

        if isinstance(param, int):
            return f"{param}"

        if isinstance(param, float):
            return f'DOUBLE("{param}")'

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

        if isinstance(param, (list, tuple)):
            return "(%s)" % ",".join(map(self._format_prepared_param, param))

        if isinstance(param, dict):
            keys = list(param.keys())
            if any(type(k) != str for k in keys):
                raise Exception("STRUCT keys must be strings")
            return "{%s}" % ",".join(
                f'"{k}":{self._format_prepared_param(v)}' for k, v in param.items()
            )

        if isinstance(param, uuid.UUID):
            return f"UUID('{param}')"

        raise Exception("Query parameter of type '%s' is not supported." % type(param))

    def execute(self, operation, params=None):
        if self._query is not None:
            raise Exception("Cursor can only be executed once")

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

            print(operation)

        self._query = OpteryxQuery(self._connection, operation)

    @property
    def rowcount(self):
        if self._query is None:
            raise Exception("Cursor must be executed first")
        return self._query.count()

    @property
    def stats(self):
        pass

    def fetchone(self) -> Optional[Dict]:
        if self._query is None:
            raise Exception("Cursor must be executed first")
        return self._query.fetchone()

    def fetchmany(self, size=None) -> List[Dict]:
        fetch_size = self.arraysize if size is None else size
        if self._query is None:
            raise Exception("Cursor must be executed first")
        return self._query.fetchmany(fetch_size)

    def fetchall(self) -> List[Dict]:
        if self._query is None:
            raise Exception("Cursor must be executed first")
        return self._query.fetchall()

    def close(self):
        self._connection.close()
