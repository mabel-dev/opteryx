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
Evaluation Node

This is a SQL Query Execution Plan Node.

This performs aliases and resolves function calls.
"""
import pyarrow
from typing import Iterable
from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.engine.functions import FUNCTIONS
from opteryx.exceptions import SqlError


class EvaluationNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        projection = config.get("projection", [])
        self.functions = [c for c in projection if "function" in c]
        self.aliases: list = []

        # work out what the columns are called
        for function in self.functions:

            if function["function"] not in FUNCTIONS:
                raise SqlError(
                    f"Function not known or not supported - {function['function']}"
                )

            args = [
                ((f"({','.join(a[0])})",) if isinstance(a[0], list) else a)
                for a in function["args"]
            ]
            column_name = f"{function['function']}({','.join(str(a[0]) for a in args)})"
            function["column_name"] = column_name

    def execute(self, data_pages: Iterable) -> Iterable:

        for page in data_pages:

            # for function, calculate and add the column
            for function in self.functions:
                arg_list = []
                # go through the arguments and build arrays of the values
                for arg in function["args"]:
                    # TODO: do we need to account for functions calling functions?
                    if arg[1] == TOKEN_TYPES.IDENTIFIER:
                        # get the column from the dataset
                        arg_list.append(page[arg[0]].to_numpy())
                    else:
                        # it's a literal, just add it
                        arg_list.append(arg[0])

                if len(arg_list) == 0:
                    arg_list = [page.num_rows]

                calculated_values = FUNCTIONS[function["function"]](*arg_list)
                if isinstance(calculated_values, (pyarrow.lib.StringScalar)):
                    calculated_values = [[calculated_values.as_py()]]
                page = pyarrow.Table.append_column(
                    page, function["column_name"], calculated_values
                )

            # for alias, add aliased column, do this after the functions because they
            # could have aliases

            yield page
