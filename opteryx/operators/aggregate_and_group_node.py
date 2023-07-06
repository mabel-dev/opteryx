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
Grouping Node

This is a SQL Query Execution Plan Node.

This is the grouping node, it is always followed by the aggregation node, but
the aggregation node doesn't need the grouping node.


"""
import random
import time
from typing import Iterable

import numpy
import pyarrow

from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate_and_append
from opteryx.managers.expression import format_expression
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode


from opteryx.operators.aggregate_node import project


def _extract_functions(aggregates):
    # extract any inner evaluations, like the IIF in SUM(IIF(x, 1, 0))

    all_evaluatable_nodes = get_all_nodes_of_type(
        aggregates,
        select_nodes=(
            NodeType.FUNCTION,
            NodeType.BINARY_OPERATOR,
            NodeType.COMPARISON_OPERATOR,
        ),
    )

    evaluatable_nodes = []
    for node in all_evaluatable_nodes:
        aggregators = get_all_nodes_of_type(node, select_nodes=(NodeType.AGGREGATOR,))
        if len(aggregators) == 0:
            evaluatable_nodes.append(node)

    return evaluatable_nodes


class AggregateAndGroupNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.groups = config["groups"]
        self.aggregates = config["aggregates"]

    @property
    def config(self):  # pragma: no cover
        return str(self._aggregates)

    @property
    def greedy(self):  # pragma: no cover
        return True

    @property
    def name(self):  # pragma: no cover
        return "Group"

    def execute(self) -> Iterable:

        # for each morsel
        #   perform any evaluations                                    ] these two steps are the projection node
        #   project down to the columns for the group by + aggregation ]
        #   do the group by

        if len(self._producers) != 1:  # pragma: no cover
            raise SqlError(f"{self.name} on expects a single producer")

        morsels = self._producers[0]  # type:ignore

        # get all the columns anywhere in the groups or aggregates
        all_identifiers = [
            node.schema_column.identity
            for node in get_all_nodes_of_type(
                self.groups + self.aggregates, select_nodes=(NodeType.IDENTIFIER,)
            )
        ]
        all_identifiers = list(dict.fromkeys(all_identifiers))

        # merge all the morsels together into one table, selecting only the columns
        # we're pretty sure we're going to use - this will fail for datasets
        # larger than memory
        table = pyarrow.concat_tables(project(morsels.execute(), all_identifiers), promote=True)

        # Get any functions we need to execute before aggregating
        evaluatable_nodes = _extract_functions(self.aggregates)

        # Allow grouping by functions by evaluating them first
        start_time = time.time_ns()
        table = evaluate_and_append(evaluatable_nodes, table)
        table = evaluate_and_append(self.groups, table)

        # Add a "*" column, this is an int because when a bool it miscounts
        if "*" not in table.column_names:
            table = table.append_column(
                "*", [numpy.full(shape=table.num_rows, fill_value=1, dtype=numpy.int8)]
            )
        self.statistics.time_evaluating += time.time_ns() - start_time

        # Extract any literal columns, we need to add these so we can group and/or
        # aggregate by them (e.g. SELECT SUM(4) FROM table;)
        all_literals = [
            node.value
            for node in get_all_nodes_of_type(
                self.groups + self.aggregates,
                select_nodes=(
                    NodeType.LITERAL,
                ),
            )
        ]
        all_literals = list(dict.fromkeys(all_literals))
        all_literals = [a for a in all_literals if str(a) not in table.column_names]
        for literal in all_literals:
            table = table.append_column(
                str(literal), [numpy.full(shape=table.num_rows, fill_value=literal)]
            )
            columns.add_column(str(literal))

        start_time = time.time_ns()
        # GROUP BY columns are deduplicated #870
        group_by_columns = list(
            dict.fromkeys(
                columns.get_column_from_alias(group.value, only_one=True) for group in self.groups
            )
        )


        groups = table.group_by(group_by_columns)

        yield groups
