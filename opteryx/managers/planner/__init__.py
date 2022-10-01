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
from typing import Iterable

import sqloxide

from opteryx.config import config
from opteryx.exceptions import SqlError, ProgrammingError
from opteryx.managers.planner.logical import logical_planner
from opteryx.managers.planner.optimizer import run_optimizer
from opteryx.managers.planner.temporal import extract_temporal_filters
from opteryx.models import QueryProperties


class QueryPlanner:
    def __init__(self, *, statement: str = "", cache=None, ast=None, properties=None):

        # if it's a byte string, convert to an ascii string
        if isinstance(statement, bytes):
            statement = statement.decode()
        self.raw_statement = statement

        if properties is None:
            self.properties = QueryProperties(config)
            self.properties.cache = cache

            # we need to deal with the temporal filters before we use sqloxide
            if statement is not None:
                (
                    self.properties.start_date,
                    self.properties.end_date,
                    self.statement,
                ) = extract_temporal_filters(statement)
            else:
                self.statement = statement
        else:
            self.properties = properties

        self.ast = ast
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
        try:
            yield from sqloxide.parse_sql(self.statement, dialect="mysql")
            # MySQL Dialect allows identifiers to be delimited with ` (backticks) and
            # identifiers to start with _ (underscore) and $ (dollar sign)
            # https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/dialect/mysql.rs
        except ValueError as exception:  # pragma: no cover
            raise SqlError(exception) from exception

    def bind_ast(self, ast, parameters: Iterable = None):
        """
        Bind physical information to the AST

        This includes the following activities
        - Replacing placeholders with the parameters
        """

        # Replace placeholders with parameters.
        # We do this after the AST has been parsed to remove any chance of the
        # parameter affecting the meaning of any of the other tokens - i.e. to
        # eliminate this feature being used for SQL injection.

        def _build_literal_node(value):
            if value is None:
                return {"Value": "Null"}
            if isinstance(value, (datetime.date, datetime.datetime)):
                return {"Value": {"SingleQuotedString": value.isoformat()}}
            if isinstance(value, (str)):
                return {"Value": {"SingleQuotedString": value}}
            if isinstance(value, (int, float, decimal.Decimal)):
                return {"Value": {"Number": [value, False]}}
            if isinstance(value, bool):
                return {"Value": {"Boolean": value}}

        def _exchange_placeholders(node, parameter_set, query_type):
            """Walk the AST replacing 'Placeholder' nodes, this is recursive"""
            if isinstance(node, list):
                return [
                    _exchange_placeholders(i, parameter_set, query_type) for i in node
                ]
            if isinstance(node, dict):
                if "Value" in node:
                    if "Placeholder" in node["Value"]:
                        # fmt:off
                        if len(parameter_set) == 0:
                            raise ProgrammingError("Incorrect number of bindings supplied."
                            " More placeholders are provided than parameters.")
                        placeholder_value = parameter_set.pop(0)
                        return _build_literal_node(placeholder_value)
                        # fmt:on
                # replace @variables
                if query_type != "SetVariable" and "Identifier" in node:
                    token_name = node["Identifier"]["value"]
                    if token_name[0] == "@":
                        if (
                            token_name not in self.properties.variables
                        ):  # pragma: no cover
                            raise SqlError(
                                f"Undefined variable found in query `{token_name}`."
                            )
                        variable_value = self.properties.variables.get(token_name)
                        return _build_literal_node(variable_value.value)
                return {
                    k: _exchange_placeholders(v, parameter_set, query_type)
                    for k, v in node.items()
                }
            # we're a leaf
            return node

        # create a copy of the parameters so we can consume them
        if parameters is None:
            working_parameter_set = []
        else:
            working_parameter_set = list(parameters)

        query_type = next(iter(ast))

        bound_ast = _exchange_placeholders(ast, working_parameter_set, query_type)
        if len(working_parameter_set) > 0:
            raise ProgrammingError(
                "Incorrect number of bindings supplied. Fewer placeholders are provided than parameters."
            )
        return bound_ast

    def create_logical_plan(self, ast):
        return logical_planner.create_plan(ast, self.properties)

    def optimize_plan(self, plan):
        if self.properties.enable_optimizer:
            return run_optimizer(plan)
        return plan

    def execute(self, plan):
        return plan.execute()
