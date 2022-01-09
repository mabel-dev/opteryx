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
Query Planner
-------------

This builds a DAG which describes a query.

This doesn't attempt to do optimization, this just decomposes the query.
"""
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.engine.planner.operations import *
from opteryx.exceptions import SqlError
from typing import List


class QueryPlan:
    def __init__(self, sql):
        """
        PLan represents Directed Acyclic Graphs which are used to describe data
        pipelines.
        """
        import sqloxide

        self.nodes = {}
        self.edges = []

        # Parse the SQL into a AST
        try:
            self._ast = sqloxide.parse(sql, dialect="ansi")
        except ValueError as e:
            raise SqlError(e)

        # build a plan for the query
        self._naive_planner(self._ast)

    def _naive_planner(self, ast):
        """
        The naive planner only works on single tables and puts operations in this
        order.

            FROM clause
            WHERE clause
            GROUP BY clause
            HAVING clause
            SELECT clause
            ORDER BY clause
            LIMIT clause

        This is primary used to test core functionality and isn't intended to be
        called by users.
        """
        query = ast[0]["Query"]["body"]

        self.add_operator("from", PartitionReaderNode(query["select"]["from"]))
        self.add_operator("union", UnionNode())
        self.add_operator("where", SelectionNode(ast["select"]["selection"]))
        self.add_operator("group", GroupByNode(ast["select"]["group_by"]))
        self.add_operator("having", SelectionNode(ast["select"]["having"]))
        self.add_operator("select", ProjectionNode(ast["select"]["projection"]))
        self.add_operator("order", OrderNode(ast["order_by"]))
        self.add_operator("limit", LimitNode(ast["limit"]))

        self.link_operators("from", "union")
        self.link_operators("union", "where")
        self.link_operators("where", "group")
        self.link_operators("group", "having")
        self.link_operators("having", "select")
        self.link_operators("select", "order")
        self.link_operators("order", "limit")

    def add_operator(self, name, operator):
        """
        Add a step to the DAG

        Parameters:
            name: string
                The name of the step, must be unique
            Operator: BaseOperator
                The Operator
        """
        self.nodes[name] = operator

    def link_operators(self, source_operator, target_operator):
        """
        Link steps in a flow.

        Parameters:
            source_operator: string
                The name of the source step
            target_operator: string
                The name of the target step
        """
        edge = (source_operator, target_operator)
        if edge not in self.edges:
            self.edges.append((source_operator, target_operator))

    def get_outgoing_links(self, name):
        """
        Get the names of outgoing links from a given step.

        Paramters:
            name: string
                The name of the step to search from
        """
        retval = {target for source, target in self.edges if source == name}
        return sorted(retval)

    def get_exit_points(self):
        """
        Get steps in the flow with no outgoing steps.
        """
        sources = {source for source, target in self.edges}
        retval = {target for source, target in self.edges if target not in sources}
        return sorted(retval)

    def get_entry_points(self):
        """
        Get steps in the flow with no incoming steps.
        """
        targets = {target for source, target in self.edges}
        retval = {source for source, target in self.edges if source not in targets}
        return sorted(retval)

    def get_operator(self, name):
        """
        Get the Operator class by name.

        Parameters:
            name: string
                The name of the step
        """
        return self.nodes.get(name)

    def merge(self, assimilatee):
        """
        Merge a flow into the current flow.

        Parameters:
            assimilatee: Flow
                The flow to assimilate into the current flows
        """
        self.nodes = {**self.nodes, **assimilatee.nodes}
        self.edges += assimilatee.edges
        self.edges = list(set(self.edges))

    def __repr__(self):
        if not self.is_acyclic():
            return "Flow: cannot represent cyclic flows"
        return "\n".join(list(self._draw()))

    def __str__(self) -> str:
        return self.get_entry_points().pop()

    def _draw(self):
        for entry in self.get_entry_points():
            yield (f"{str(entry)}")
            t = self._tree(entry, "")
            yield ("\n".join(t))

    def _tree(self, node, prefix=""):

        space = "    "
        branch = " │  "
        tee = " ├─ "
        last = " └─ "

        contents = self.get_outgoing_links(node)
        # contents each get pointers that are ├── with a final └── :
        pointers = [tee] * (len(contents) - 1) + [last]
        for pointer, child_node in zip(pointers, contents):
            yield prefix + pointer + str(child_node)
            if len(self.get_outgoing_links(node)) > 0:
                # extend the prefix and recurse:
                extension = branch if pointer == tee else space
                # i.e. space because last, └── , above so no more |
                yield from self._tree(str(child_node), prefix=prefix + extension)


"""
[
    {'Query': {
        'with': None, 
        'body': {
            'Select': {
                'distinct': False, 
                'top': None, 
                'projection': ['Wildcard'], 
                'from': [
                    {
                        'relation': {
                            'Table': {
                                'name': [
                                    {
                                        'value': 't', 
                                        'quote_style': None
                                    }
                                ], 
                                'alias': None, 
                                'args': [], 
                                    'with_hints': []
                                }
                            }, 'joins': []
                        }
                    ], 
                    'lateral_views': [], 
                    'selection': {
                        'BinaryOp': {
                            'left': {
                                'Identifier': {
                                    'value': 'a', 
                                    'quote_style': None
                                }
                            }, 
                            'op': 'Eq', 
                            'right': {
                                'Identifier': {
                                    'value': 'b', 
                                    'quote_style': None
                                }
                            }
                        }
                    }, 
                    'group_by': [], 
                    'cluster_by': [], 
                    'distribute_by': [], 
                    'sort_by': [], 
                    'having': None
                }
            }, 
            'order_by': [],
            'limit': None, 
            'offset': None, 
            'fetch': None
        }
    }
]
"""
