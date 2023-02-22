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
The Execution Tree is the Graph which defines a Query Plan.

The execution tree contains functionality to:

- build and define the plan
- execute the plan
- manipulate the plan

"""

import pyarrow

from opteryx.exceptions import DatabaseError
from opteryx.third_party.travers import Graph


class ExecutionTree(Graph):
    """
    The execution tree is defined separately to the planner to simplify the
    complex code which is the planner from the tree that describes the plan.
    """

    def get_nodes_of_type(self, operator):
        """
        Traverse the plan looking for nodes of a given type or set of types.

        Parameters:
            operator: BaseOperator or Iterable of BaseOperator
                The operator type(s) to match.
        Yields:
            string
                nids of matching nodes.
        """

        def _inner(operator):
            if not isinstance(operator, (list, set)):
                operator = tuple([operator])
            if not isinstance(operator, tuple):
                operator = tuple(operator)
            for nid, item in list(self._nodes.items()):
                if isinstance(item, operator):
                    yield nid

        return list(_inner(operator))

    def execute(self):
        """
        This implements a 'pull' model execution engine. It finds the last stage in
        the plan and pulls records from it - this stage then pulls records from earlier
        stages in the plan as needed, and so on, until we get to a node that creates
        records to feed into the engine (usually reading a data file).
        """

        def map_operators(nodes):
            """
            We're walking the query plan telling each node where to get the data it
            needs from.
            """
            for node in nodes:
                producers = self.ingoing_edges(node)
                operator = self[node]
                if producers:
                    operator.set_producers([self[i[0]] for i in producers])
                    map_operators(i[0] for i in producers)

        # do some basic validation before we try to execute
        if not self.is_acyclic():  # pragma: no cover
            raise DatabaseError("Problem executing the query plan - it is cyclic.")

        # we get the tail of the query - the first steps
        head = list(dict.fromkeys(self.get_exit_points()))
        if len(head) != 1:  # pragma: no cover
            raise DatabaseError(f"Problem executing the query plan - it has {len(head)} heads.")

        map_operators(head)

        operator = self[head[0]]
        yield from operator.execute()

    def explain(self):
        from opteryx import operators
        from opteryx.models import Columns

        def _inner_explain(node, depth):
            if depth == 1:
                operator = self[node]
                yield {
                    "operator": operator.name,
                    "config": operator.config,
                    "depth": depth - 1,
                }
            incoming_operators = self.ingoing_edges(node)
            for operator_name in incoming_operators:
                operator = self[operator_name[0]]
                if isinstance(operator, operators.BasePlanNode):
                    yield {
                        "operator": operator.name,
                        "config": operator.config,
                        "depth": depth,
                    }
                yield from _inner_explain(operator_name[0], depth + 1)

        head = list(dict.fromkeys(self.get_exit_points()))
        if len(head) != 1:  # pragma: no cover
            raise DatabaseError(f"Problem with the plan - it has {len(head)} heads.")
        plan = list(_inner_explain(head[0], 1))

        table = pyarrow.Table.from_pylist(plan)
        table = Columns.create_table_metadata(
            table, table.num_rows, "plan", None, "calculated", "explain"
        )
        yield table
