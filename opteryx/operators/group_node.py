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


def _project(tables, fields):
    fields = list(dict.fromkeys(fields))
    for table in tables:
        row_count = table.num_rows
        columns = Columns(table)
        column_names = [columns.get_column_from_alias(field, only_one=True) for field in fields]
        if len(column_names) > 0:
            yield table.select(dict.fromkeys(column_names))
        else:
            # if we can't find the column, add a placeholder column
            yield pyarrow.Table.from_pydict({"*": numpy.full(row_count, 1, dtype=numpy.int8)})


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


class GroupNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.groups = config["groups"]

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
        if len(self._producers) != 1:  # pragma: no cover
            raise SqlError(f"{self.name} on expects a single producer")

        morsels = self._producers[0]  # type:ignore

        # get all the columns anywhere in the groups or aggregates
        all_identifiers = [
            node.value
            for node in get_all_nodes_of_type(
                self._groups + self._aggregates, select_nodes=(NodeType.IDENTIFIER,)
            )
        ]
        all_identifiers = list(dict.fromkeys(all_identifiers))

        # merge all the morsels together into one table, selecting only the columns
        # we're pretty sure we're going to use - this will fail for datasets
        # larger than memory
        table = pyarrow.concat_tables(_project(morsels.execute(), all_identifiers), promote=True)

        # Get any functions we need to execute before aggregating
        evaluatable_nodes = _extract_functions(self._aggregates)

        # Allow grouping by functions by evaluating them first
        start_time = time.time_ns()
        columns, _, table = evaluate_and_append(evaluatable_nodes, table)
        columns, self._groups, table = evaluate_and_append(self._groups, table)

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
                self._groups + self._aggregates,
                select_nodes=(
                    NodeType.LITERAL_BOOLEAN,
                    NodeType.LITERAL_INTEGER,
                    NodeType.LITERAL_FLOAT,
                    NodeType.LITERAL_VARCHAR,
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
                columns.get_column_from_alias(group.value, only_one=True) for group in self._groups
            )
        )

        column_map, aggs = _build_aggs(self._aggregates, columns)

        # we're not a group_by - we're aggregating without grouping
        if len(group_by_columns) == 0:
            groups = _non_group_aggregates(self._aggregates, table, columns)
            del table
        else:
            table = table.combine_chunks()
            groups = table.group_by(group_by_columns)
            groups = groups.aggregate(aggs)

        # do the secondary activities for ARRAY_AGG
        for node in get_all_nodes_of_type(self._aggregates, select_nodes=(NodeType.AGGREGATOR,)):
            if node.value == "ARRAY_AGG":
                _, _, order, limit = node.parameters
                if order or limit:
                    # rip the column out of the table
                    column_name = column_map[format_expression(node)]
                    column_def = groups.field(column_name)  # this is used
                    column = groups.column(column_name).to_pylist()
                    groups = groups.drop([column_name])
                    # order
                    if order:
                        pass
                    if limit:
                        column = [c[:limit] for c in column]
                    # put the new column into the table
                    groups = groups.append_column(column_def, [column])

        # name the aggregate fields and add them to the Columns data
        for friendly_name, agg_name in column_map.items():
            # randomize the column name to prevent collisions, particularly on joined
            # or chained aggregations
            new_column_name = hex(random.getrandbits(64))
            groups = groups.rename_columns(
                [col if col != agg_name else new_column_name for col in groups.column_names]
            )

            columns.add_column(new_column_name)
            columns.set_preferred_name(new_column_name, friendly_name)
            # if we have an alias for this column, add it to the metadata
            aliases = [
                agg.alias for agg in self._aggregates if friendly_name == format_expression(agg)
            ]
            aliases.append(agg_name)
            for alias in aliases:
                if alias:
                    columns.add_alias(new_column_name, alias)

        # apply the metadata to the resultant dataset
        groups = columns.apply(groups)

        self.statistics.time_aggregating += time.time_ns() - start_time

        yield groups
