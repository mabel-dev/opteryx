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
The Execution Tree is the graph which defines and supports the execution of
query plans.
"""

import pyarrow


from opteryx.exceptions import DatabaseError


class ExecutionTree:
    """
    The execution tree is defined separately to the planner to simplify the
    complex code which is the planner from the tree that describes the plan.
    """

    def __init__(self):
        """create empty plan"""
        self._nodes: dict = {}
        self._edges: list = []

    def get_nodes_of_type(self, operator):
        if not isinstance(operator, (list, tuple, set)):
            operator = [operator]
        for nid, item in list(self._nodes.items()):
            if isinstance(item, *operator):
                yield nid

    def insert_operator_before(self, nid, operator, before_nid):
        """rewrite the plan putting the new node before a given node"""
        # add the new node to the plan
        self.add_operator(nid, operator)
        # change all the edges that were going into the old nid to the new one
        self._edges = [
            (source, target if target != before_nid else nid, direction)
            for source, target, direction in self._edges
        ]
        # add an edge from the new nid to the old one
        self.link_operators(nid, before_nid)

    def remove_operator(self, nid):
        """rewrite a plan, removing a node"""

        # remove the node
        self._nodes.pop(nid, None)

        # link the nodes each side of the node being removed
        out_going = self.get_outgoing_links(nid)
        in_coming = self.get_incoming_links(nid)
        for out_nid in out_going:
            for in_nid in in_coming:
                self.link_operators(in_nid[0], out_nid, in_nid[1])

        self._edges = [
            (source, target, direction)
            for source, target, direction in self._edges
            if source != nid and target != nid
        ]

    def add_operator(self, nid, operator):
        """
        Add a step to the DAG

        Parameters:
            id: string
                The id of the step, must be unique
            Operator: BaseOperator
                The Operator
        """
        self._nodes[nid] = operator

    def link_operators(self, source_operator, target_operator, direction=None):
        """
        Link steps in a flow.

        Parameters:
            source_operator: string
                The id of the source step
            target_operator: string
                The id of the target step
            direction: string (optional)
                The name of the connection, for joins this will be Left/Right
        """
        edge = (
            source_operator,
            target_operator,
            direction,
        )
        if edge not in self._edges:
            self._edges.append(edge)

    def get_outgoing_links(self, nid):
        """
        Get the ids of outgoing nodes from a given step.

        Paramters:
            name: string
                The name of the step to search from
        """
        retval = {target for source, target, direction in self._edges if source == nid}
        return sorted(retval)

    def get_incoming_links(self, nid):
        """
        Get the ids of incoming nodes for a given step.

        Paramters:
            nid: string
                The name of the step to search from
        """
        retval = {
            (source, direction)
            for source, target, direction in self._edges
            if target == nid
        }
        return sorted(retval)

    def get_exit_points(self):
        """
        Get steps in the flow with no outgoing steps.
        """
        if len(self._nodes) == 1:
            return list(self._nodes.keys())
        sources = {source for source, target, direction in self._edges}
        retval = (
            target for source, target, direction in self._edges if target not in sources
        )
        return sorted(retval)

    def get_entry_points(self):
        """
        Get steps in the flow with no incoming steps.
        """
        if len(self._nodes) == 1:
            return list(self._nodes.keys())
        targets = {target for source, target, direction in self._edges}
        retval = (
            source for source, target, direction in self._edges if source not in targets
        )
        return sorted(retval)

    def get_operator(self, nid):
        """
        Get the Operator class by id.

        Parameters:
            nid: string
                The id of the step
        """
        return self._nodes.get(nid)

    def is_acyclic(self):
        """
        Test if the graph is acyclic
        """
        # cycle over the graph removing a layer of exits each cycle
        # if we have nodes but no exists, we're cyclic
        my_edges = self._edges.copy()

        while len(my_edges) > 0:
            # find all of the exits
            sources = {source for source, target, direction in my_edges}
            exits = {
                target
                for source, target, direction in my_edges
                if target not in sources
            }

            if len(exits) == 0:
                return False

            # remove the exits
            new_edges = [
                (source, target, direction)
                for source, target, direction in my_edges
                if target not in exits
            ]
            my_edges = new_edges
        return True

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
                producers = self.get_incoming_links(node)
                operator = self.get_operator(node)
                if producers:
                    operator.set_producers([self.get_operator(i[0]) for i in producers])
                    map_operators(i[0] for i in producers)

        # do some basic validation
        if not self.is_acyclic():
            raise DatabaseError("Problem executing the query plan - it is cyclic.")

        # we get the tail of the query - the first steps
        head = list(set(self.get_exit_points()))
        if len(head) != 1:
            raise DatabaseError(
                f"Problem executing the query plan - it has {len(head)} heads."
            )

        map_operators(head)

        operator = self.get_operator(head[0])
        yield from operator.execute()

    def explain(self):

        from opteryx import operators
        from opteryx.models import Columns

        def _inner_explain(node, depth):
            if depth == 1:
                operator = self.get_operator(node)
                yield {
                    "operator": operator.name,
                    "config": operator.config,
                    "depth": depth - 1,
                }
            incoming_operators = self.get_incoming_links(node)
            for operator_name in incoming_operators:
                operator = self.get_operator(operator_name[0])
                if isinstance(operator, operators.BasePlanNode):
                    yield {
                        "operator": operator.name,
                        "config": operator.config,
                        "depth": depth,
                    }
                yield from _inner_explain(operator_name[0], depth + 1)

        head = list(set(self.get_exit_points()))
        # print(head, _edges)
        if len(head) != 1:
            raise DatabaseError(f"Problem with the plan - it has {len(head)} heads.")
        plan = list(_inner_explain(head[0], 1))

        table = pyarrow.Table.from_pylist(plan)
        table = Columns.create_table_metadata(table, table.num_rows, "plan", None)
        yield table
