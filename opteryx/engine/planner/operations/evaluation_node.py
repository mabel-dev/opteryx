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
        self.aliases = []

        # work out what the columns are called
        for function in  self.functions:

            if function["function"] not in FUNCTIONS:
                raise SqlError(f"Function not known or not supported - {function['function']}")

            if function.get('alias'):
                column_name = function["alias"]
            else:
                column_name = f"{function['function']}({','.join(str(a[0]) for a in function['args'])})"
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
                        
                calculated_values = FUNCTIONS[function["function"]](*arg_list)
                page = pyarrow.Table.append_column(page, function["column_name"], calculated_values)


            # for alias, add aliased column, do this after the functions because they
            # could have aliases

            yield page
