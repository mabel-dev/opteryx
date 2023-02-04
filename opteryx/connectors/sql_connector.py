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
Inner Reader for SQL stores

This currently isn't a base class because we're assuming a standard
functionality of SQL engines.

This relies on SQLAlchemy
"""
from typing import List, Union

import pyarrow

from opteryx.exceptions import MissingDependencyError


class SqlConnector:
    __mode__ = "SQL"

    def __init__(self, server) -> None:
        # defer this step until we know we need it
        try:
            from sqlalchemy import create_engine
            from sqlalchemy import text
        except ImportError as err:  # pragma: nocover
            raise MissingDependencyError(
                "`sqlalchemy` is missing, please install or include in requirements.txt"
            ) from err

        self._predicates: List = []
        self._server = server

    def read_records(
        self, dataset, selection: Union[list, None] = None, page_size: int = 500
    ):  # pragma: no cover
        """
        Return a page of documents
        """
        raise NotImplementedError("read_document not implemented")

    @property
    def can_push_selection(self):
        return True


if __name__ == "__main__":
    from sqlalchemy import create_engine
    engine = create_engine("postgresql://postgres:postgrespw@localhost:49153")
