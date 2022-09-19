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
This is the Query Planner, it is responsible for creating the physical execution plan
from the SQL statement provided by the user. It does this is a multistage process
as per the below.

 ┌────────────┐         ┌─────────────────┐
 │            │  SQL    │ Parser + Lexer  │
 │            ├────────►│                 │
 │            │         └─────────────────┘
 │            │         ┌─────────────────┐
 │  Query     │  AST    │ Binder          │
 │   Planner  ├────────►│                 │
 │            │         └─────────────────┘
 │            │         ┌─────────────────┐
 │            │  AST    │ Logical Planner │
 │            ├────────►│                 │
 │            │         └─────────────────┘
 │            │         ┌─────────────────┐
 │            │  Plan   │ Optimizer       │
 │            ├────────►│                 │
 └────────────┘         └─────────────────┘
       │
       ▼
    Executor
"""
import sqloxide

from opteryx.config import config
from opteryx.exceptions import SqlError
from opteryx.managers.planner.temporal import extract_temporal_filters
from opteryx.models import QueryProperties, QueryStatistics


class QueryPlanner:
    def __init__(self, *, statement: str = "", cache=None):

        self._cache = cache

        # if it's a byte string, convert to an ascii string
        if isinstance(statement, bytes):
            statement = statement.decode()

        self.statistics = QueryStatistics()
        self.properties = QueryProperties(config)

        # extract temporal filters, this isn't supported by sqloxide
        self.start_date, self.end_date, self.statement = extract_temporal_filters(
            statement
        )

        # Parse the SQL into a AST
        try:
            self.ast = sqloxide.parse_sql(self.statement, dialect="mysql")
            # MySQL Dialect allows identifiers to be delimited with ` (backticks) and
            # identifiers to start with _ (underscore) and $ (dollar sign)
            # https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/dialect/mysql.rs
        except ValueError as exception:  # pragma: no cover
            raise SqlError(exception) from exception
