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


class ExecutionTree:
    """
    The execution tree is defined separately to the planner to simplify the
    complex code which is the planner from the tree that describes the plan.
    """

    def __init__(self):
        """create empty plan"""
        self._nodes: dict = {}
        self._edges: list = []

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

    def link_operators(self, source_operator, target_operator, connection_name=None):
        """
        Link steps in a flow.

        Parameters:
            source_operator: string
                The id of the source step
            target_operator: string
                The id of the target step
            connection_name: string (optional)
                The name of the connection, for joins this will be Left/Right
        """
        edge = (
            source_operator,
            target_operator,
            connection_name,
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
        retval = {
            target for source, target, connection_name in self._edges if source == nid
        }
        return sorted(retval)

    def get_incoming_links(self, nid):
        """
        Get the ids of incoming nodes for a given step.

        Paramters:
            nid: string
                The name of the step to search from
        """
        retval = {
            (
                source,
                connection_name,
            )
            for source, target, connection_name in self._edges
            if target == nid
        }
        return sorted(retval)

    def get_exit_points(self):
        """
        Get steps in the flow with no outgoing steps.
        """
        if len(self._nodes) == 1:
            return list(self._nodes.keys())
        sources = {source for source, target, connection_name in self._edges}
        retval = (
            target
            for source, target, connection_name in self._edges
            if target not in sources
        )
        return sorted(retval)

    def get_entry_points(self):
        """
        Get steps in the flow with no incoming steps.
        """
        if len(self._nodes) == 1:
            return list(self._nodes.keys())
        targets = {target for source, target, connection_name in self._edges}
        retval = (
            source
            for source, target, connection_name in self._edges
            if source not in targets
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
            sources = {source for source, target, connection_name in my_edges}
            exits = {
                target
                for source, target, connection_name in my_edges
                if target not in sources
            }

            if len(exits) == 0:
                return False

            # remove the exits
            new_edges = [
                (source, target, connection_name)
                for source, target, connection_name in my_edges
                if target not in exits
            ]
            my_edges = new_edges
        return True
