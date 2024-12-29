# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from orso.schema import OrsoTypes

from opteryx import operators as operators
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import PhysicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType


def create_physical_plan(logical_plan, query_properties) -> PhysicalPlan:
    plan = PhysicalPlan()

    for nid, logical_node in logical_plan.nodes(data=True):
        node_type = logical_node.node_type
        node_config = logical_node.properties
        node: operators.BasePlanNode = None

        # fmt: off
        if node_type == LogicalPlanStepType.Aggregate:
            node = operators.AggregateNode(query_properties, **{k:v for k,v in node_config.items() if k in ("aggregates", "all_relations")})
        elif node_type == LogicalPlanStepType.AggregateAndGroup:
            node = operators.AggregateAndGroupNode(query_properties, **{k:v for k,v in node_config.items() if k in ("aggregates", "groups", "projection", "all_relations")})
        #        elif node_type == LogicalPlanStepType.Defragment:
        #            node = operators.MorselDefragmentNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Distinct:
            node = operators.DistinctNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Exit:
            node = operators.ExitNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Explain:
            node = operators.ExplainNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Filter:
            node = operators.FilterNode(query_properties, filter=node_config["condition"], **{k:v for k,v in node_config.items() if k in ("all_relations",)})
        elif node_type == LogicalPlanStepType.FunctionDataset:
            node = operators.FunctionDatasetNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.HeapSort:
            node = operators.HeapSortNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Join:
            if node_config.get("type") == "inner":
                # We use our own implementation of INNER JOIN
                # We have optimized VARCHAR version
                if len(node_config["left_columns"]) == 1 and node_config["columns"][0].schema_column.type == OrsoTypes.VARCHAR:
                    node = operators.InnerJoinSingleNode(query_properties, **node_config)
                else:
                    node = operators.InnerJoinNode(query_properties, **node_config)
            elif node_config.get("type") in ("left outer", "full outer", "right outer"):
                # We use out own implementation of OUTER JOINS
                node = operators.OuterJoinNode(query_properties, **node_config)
            elif node_config.get("type") == "cross join":
                # Pyarrow doesn't have a CROSS JOIN
                node = operators.CrossJoinNode(query_properties, **node_config)
            elif node_config.get("type") in ("left anti", "left semi"):
                # We use our own implementation of LEFT SEMI and LEFT ANTI JOIN
                node = operators.FilterJoinNode(query_properties, **node_config)
            else:
                raise InvalidInternalStateError(f"Unsupported JOIN type '{node_config['type']}'")
        elif node_type == LogicalPlanStepType.Limit:
            node = operators.LimitNode(query_properties, **{k:v for k,v in node_config.items() if k in ("limit", "offset", "all_relations")})
        elif node_type == LogicalPlanStepType.Order:
            node = operators.SortNode(query_properties, **{k:v for k,v in node_config.items() if k in ("order_by", "all_relations")})
        elif node_type == LogicalPlanStepType.Project:
            node = operators.ProjectionNode(query_properties, projection=logical_node.columns, **{k:v for k,v in node_config.items() if k in ("projection", "all_relations")})
        elif node_type == LogicalPlanStepType.Scan:
            connector = node_config.get("connector")
            if connector and hasattr(connector, "async_read_blob"):
                node = operators.AsyncReaderNode(query_properties, **node_config)
            else:
                node = operators.ReaderNode(properties=query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Set:
            node = operators.SetVariableNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Show:
            if node_config["object_type"] == "VARIABLE":
                node = operators.ShowValueNode(query_properties, kind=node_config["items"][1], value=node_config["items"][1], **node_config)
            elif node_config["object_type"] == "VIEW":
                node = operators.ShowCreateNode(query_properties, **node_config)
            else:
                raise UnsupportedSyntaxError(f"Unsupported SHOW type '{node_config['object_type']}'")
        elif node_type == LogicalPlanStepType.ShowColumns:
            node = operators.ShowColumnsNode(query_properties, **node_config)
        elif node_type == LogicalPlanStepType.Union:
            node = operators.UnionNode(query_properties, **node_config)
        else:  # pragma: no cover
            raise Exception(f"something unexpected happed - {node_type.name}")
        # fmt: on

        plan.add_node(nid, node)

    for source, destination, relation in logical_plan.edges():
        plan.add_edge(source, destination, relation)

    return plan
