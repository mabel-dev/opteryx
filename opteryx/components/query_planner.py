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

                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                         ┌─────┴─────┐
   │ SQL       │                         │           │
   │  Rewriter │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │SQL                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │           │      │           │      │           │
   │ Parser    │      │ Catalogue ├──────► Optimizer │
   └─────┬─────┘      └─────┬─────┘      └─────▲─────┘
         │AST               │                  │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │ Tree      │
   │   Planner ├──────► Binder    ├──────►  Rewriter │
   └───────────┘      └───────────┘      └───────────┘
"""
from opteryx import config
from opteryx.components.binder.binder import bind_ast
from opteryx.components.logical_planner.logical_planner import create_logical_plan
from opteryx.components.sql_rewriter.sql_rewriter import clean_statement
from opteryx.components.sql_rewriter.sql_rewriter import remove_comments
from opteryx.components.sql_rewriter.temporal_extraction import extract_temporal_filters
from opteryx.components.tree_rewriter import tree_rewriter
from opteryx.exceptions import ProgrammingError
from opteryx.exceptions import SqlError
from opteryx.models import QueryProperties
from opteryx.shared import CircularLog
from opteryx.third_party import sqloxide

PROFILE_LOCATION = config.PROFILE_LOCATION
QUERY_LOG_LOCATION = config.QUERY_LOG_LOCATION
QUERY_LOG_SIZE = config.QUERY_LOG_SIZE


class QueryPlanner:
    def __init__(self, *, statement: str = "", cache=None, ast=None, properties=None, qid=None):
        # if it's a byte string, convert to an ascii string
        if isinstance(statement, bytes):
            statement = statement.decode()
        self.raw_statement = statement
        self.statement = statement

        if properties is None:
            self.properties = QueryProperties(qid, config._config_values)
            self.properties.cache = cache

            # we need to deal with the temporal filters before we use sqloxide
            if statement is not None:
                # prep the statement, by normalizing it
                clean_sql = remove_comments(statement)
                clean_sql = clean_statement(clean_sql)

                (
                    self.statement,
                    self.properties.temporal_filters,
                ) = extract_temporal_filters(clean_sql)
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
            # MySQL Dialect allows identifiers to be delimited with ` (backticks) and
            # identifiers to start with _ (underscore) and $ (dollar sign)
            # https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/dialect/mysql.rs

            parsed_statements = sqloxide.parse_sql(self.statement, dialect="mysql")

            from opteryx.components.v2.logical_planner.planner import get_planners

            if QUERY_LOG_LOCATION:
                log = CircularLog(QUERY_LOG_LOCATION, QUERY_LOG_SIZE, 1024 * 1024)
                log.append(self.statement)

            if PROFILE_LOCATION:
                try:
                    plans = ""
                    for planner, ast in get_planners(parsed_statements):
                        plans = self.statement + "\n\n"
                        plans += planner(ast).draw()
                    with open(PROFILE_LOCATION, mode="w") as f:
                        f.write(plans)
                except Exception as err:
                    # print("Unable to plan query {self.statement}")
                    # print(f"{type(err).__name__} - {err}")
                    pass

            yield from parsed_statements
        except ValueError as exception:  # pragma: no cover
            raise SqlError(exception) from exception

    def bind_ast(self, ast, parameters):
        return bind_ast(ast, parameters, self.properties)

    def create_logical_plan(self, ast):
        return create_logical_plan(ast, self.properties)

    def optimize_plan(self, plan):
        if self.properties.enable_optimizer:
            plan = tree_rewriter(plan, self.properties)
        #            return run_optimizer(plan, self.properties)
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
