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
from opteryx.config import config
from opteryx.exceptions import SqlError, ProgrammingError
from opteryx.managers.planner import binder
from opteryx.managers.planner.logical import logical_planner
from opteryx.managers.planner.optimizer import run_optimizer
from opteryx.managers.planner.temporal import extract_temporal_filters
from opteryx.models import QueryProperties
from opteryx.third_party import sqloxide


class QueryPlanner:
    def __init__(
        self, *, statement: str = "", cache=None, ast=None, properties=None, qid=None
    ):

        # if it's a byte string, convert to an ascii string
        if isinstance(statement, bytes):
            statement = statement.decode()
        self.raw_statement = statement

        if properties is None:
            self.properties = QueryProperties(qid, config)
            self.properties.cache = cache

            # we need to deal with the temporal filters before we use sqloxide
            if statement is not None:
                (
                    self.statement,
                    self.properties.temporal_filters,
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

    def bind_ast(self, ast, parameters):
        return binder.bind_ast(ast, parameters, self.properties)

    def create_logical_plan(self, ast):
        return logical_planner.create_plan(ast, self.properties)

    def optimize_plan(self, plan):
        if self.properties.enable_optimizer:
            return run_optimizer(plan, self.properties)
        return plan

    def execute(self, plan):
        return plan.execute()

    def test_paramcount(self, asts, params):
        """count the number of Placeholders and compare to the number of params"""

        def _inner(node):
            # walk the tree counting Placeholders
            if isinstance(node, list):
                return sum(_inner(n) for n in node)
            if isinstance(node, dict):
                if "Value" in node:
                    if "Placeholder" in node["Value"]:
                        return 1
                return sum(_inner(v) for v in node.values())
            return 0

        found_params = _inner(asts)
        if found_params > len(params):
            raise ProgrammingError(
                "Incorrect number of bindings supplied. Fewer parameters were found than placeholders."
            )
        if found_params < len(params):
            raise ProgrammingError(
                "Incorrect number of bindings supplied. Fewer placeholders are provided than parameters."
            )
