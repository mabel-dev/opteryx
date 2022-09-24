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

 ┌────────────┐  SQL    ┌─────────────────┐
 │            ├────────►│ Parser + Lexer  │
 │            |         └─────────────────┘
 │  Query     │  AST    ┌─────────────────┐
 │   Planner  ├────────►| Binder          │
 │            |         └─────────────────┘
 │            │  AST    ┌─────────────────┐
 │            ├────────►│ Logical Planner │
 │            |         └─────────────────┘
 │            │  Plan   ┌─────────────────┐
 │            ├────────►│ Optimizer       │
 └────────────┘         └─────────────────┘
       │
       ▼
    Executor
"""
import datetime
import decimal

import sqloxide

from opteryx.config import config
from opteryx.exceptions import SqlError, ProgrammingError
from opteryx.managers.planner.temporal import extract_temporal_filters
from opteryx.managers.planner.logical import logical_planner
from opteryx.models import QueryProperties, QueryStatistics


class QueryPlanner:
    def __init__(self, *, statement: str = "", cache=None):

        self._cache = cache

        # if it's a byte string, convert to an ascii string
        if isinstance(statement, bytes):
            statement = statement.decode()
        self.statement = statement

        self.statistics = QueryStatistics()
        self.properties = QueryProperties(config)

        # set to null, we're going to populate these later
        self.start_date = None
        self.end_date = None
        self.ast = None
        self.logical_plan = None
        self.physical_plan = None

    def parse_and_lex(self):
        """
        Parse & Lex the SQL into an Abstract Syntax Tree (AST)

        We use sqlparser-rs to create an AST from the SQL. More information is
        available in the documentation for that library, but very broadly it
        will tokenize (split into words) the SQL, parse, and then assign meaning
        to each of the tokens, lex, to build the AST.

        The AST is just another representation of the SQL provided by the user,
        when we have the AST we know we probably have valid SQL, but not if we
        can actually execute it.
        """

        # Extract and remove temporal filters, this isn't supported by sqloxide.
        self.start_date, self.end_date, statement = extract_temporal_filters(
            self.statement
        )
        try:
            self.ast = sqloxide.parse_sql(statement, dialect="mysql")
            # MySQL Dialect allows identifiers to be delimited with ` (backticks) and
            # identifiers to start with _ (underscore) and $ (dollar sign)
            # https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/dialect/mysql.rs
        except ValueError as exception:  # pragma: no cover
            raise SqlError(exception) from exception

    def bind_ast(self, parameters):
        """
        Bind physical information to the AST

        This includes the following activities
        - Replacing placeholders with the parameters
        """

        # Replace placeholders with parameters.
        # We do this after the AST has been parsed to remove any chance of the
        # parameter affecting the meaning of any of the other tokens - i.e. to
        # eliminate this feature being used for SQL injection.

        def _exchange_placeholders(node, parameter_set):
            """Walk the AST replacing 'Placeholder' nodes, this is recursive"""
            if isinstance(node, list):
                return [_exchange_placeholders(i, parameter_set) for i in node]
            if isinstance(node, dict):
                if "Value" in node:
                    if "Placeholder" in node["Value"]:
                        # fmt:off
                        if len(parameter_set) == 0:
                            raise ProgrammingError("Incorrect number of bindings supplied."
                            " More placeholders are provided than parameters.")
                        placeholder_value = parameter_set.pop(0)
                        if placeholder_value is None:
                            return {"Value": "Null"}
                        if isinstance(placeholder_value, (datetime.date, datetime.datetime)):
                            return {"Value": {"SingleQuotedString": placeholder_value.isoformat()}}
                        if isinstance(placeholder_value, (str)):
                            return {"Value": {"SingleQuotedString": placeholder_value}}
                        if isinstance(placeholder_value, (int, float, decimal.Decimal)):
                            return {"Value": {"Number": [placeholder_value, False]}}
                        if isinstance(placeholder_value, bool):
                            return {"Value": {"Boolean": placeholder_value}}
                        # fmt:on
                return {
                    k: _exchange_placeholders(v, parameter_set) for k, v in node.items()
                }
            # we're a leaf
            return node

        # create a copy of the parameters so we can consume them
        working_parameter_set = list(parameters or [])

        self.ast = _exchange_placeholders(self.ast, working_parameter_set)
        if len(working_parameter_set) > 0:
            raise ProgrammingError(
                "Incorrect number of bindings supplied. Fewer placeholders are provided than parameters."
            )

    # TODO: restore variable exchanges - do it in the binder
    #        token_name = function["value"]
    #        if token_name[0] == "@":
    #            if token_name not in self.properties.variables:  # pragma: no cover
    #                raise SqlError(f"Undefined variable found in query `{token_name}`.")
    #            return self.properties.variables.get(token_name)
    #        else:

    def create_logical_plan(self):
        self.logical_plan = logical_planner.create_plan(self.ast, self.properties)

    def optimize_plan(self):
        self.physical_plan = self.logical_plan

    def execute(self):
        return self.physical_plan.execute()
