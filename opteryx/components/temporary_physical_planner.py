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
This is a temporary step, which takes logical plans from the V2 planner
and converts them to modified-V1 physical plans.

This should look different when the operators are rewritten for the 
Gen 2 execution engine (a later piece of work)
"""

from opteryx import operators
from opteryx.components.logical_planner import LogicalPlanStepType
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import ExecutionTree


def create_physical_plan(logical_plan, query_properties):
    plan = ExecutionTree()

    for nid, logical_node in logical_plan.nodes(data=True):
        node_type = logical_node.node_type
        node_config = logical_node.properties
        node: operators.BasePlanNode = None

        # fmt: off
        if node_type == LogicalPlanStepType.Aggregate:
            node = operators.AggregateNode(query_properties, aggregates=node_config["aggregates"])
        elif node_type == LogicalPlanStepType.AggregateAndGroup:
            node = operators.AggregateAndGroupNode(query_properties, groups=node_config["groups"], aggregates=node_config["aggregates"], projection=node_config["projection"])
        elif node_type == LogicalPlanStepType.Distinct:
            node = operators.DistinctNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Exit:
            node = operators.ExitNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Explain:
            node = operators.ExplainNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Filter:
            node = operators.SelectionNode(query_properties, filter=node_config["condition"])
        elif node_type == LogicalPlanStepType.FunctionDataset:
            node = operators.FunctionDatasetNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Join:
            if node_config.get("type") == "cross join":
                # CROSS JOINs are quite different
                node = operators.CrossJoinNode(query_properties, **node_config)
            else:
                node = operators.JoinNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Limit:
            node = operators.LimitNode(query_properties, limit=node_config.get("limit"), offset=node_config.get("offset", 0))
        elif node_type == LogicalPlanStepType.Order:
            node = operators.SortNode(query_properties, order=node_config["order_by"])
        elif node_type == LogicalPlanStepType.Project:
            node = operators.ProjectionNode(query_properties, projection=logical_node.columns)
        elif node_type == LogicalPlanStepType.Scan:
            node = operators.ScannerNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Set:
            node = operators.SetVariableNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Show:
            if node_config["items"][0] == "PARAMETER":
                node = operators.ShowValueNode(query_properties, kind="PARAMETER", value=node_config["items"][1])
            else:
                raise UnsupportedSyntaxError("Can only SHOW variables")
        elif node_type == LogicalPlanStepType.ShowColumns:
            node = operators.ShowColumnsNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Subquery:
            node = operators.NoOpNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Union:
            node = operators.NoOpNode(query_properties, **node_config)

        else:
            raise Exception(f"something unexpected happed - {node_type.name}")
        # fmt: on
        plan.add_node(nid, node)

    for source, destination, relation in logical_plan.edges():
        plan.add_edge(source, destination)

    return plan
