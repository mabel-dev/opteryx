# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is Binder, it sits between the Logical Planner and the Optimizers.

~~~
                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                               │
   │ SQL       │                               │
   │ Rewriter  │                               │
   └─────┬─────┘                               │
         │SQL                                  │Results
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ AST       │      │           │      │ Physical  │
   │ Rewriter  │      │ Catalogue │      │ Planner   │
   └─────┬─────┘      └───────────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │           │
   │   Planner ├──────► Binder    ├──────► Optimizer │
   └───────────┘      └───────────┘      └───────────┘

~~~

The Binder is responsible for adding information about the database and engine into the
Logical Plan.

The binder takes the the logical plan, and adds information from various catalogues
into that planand then performs some validation checks.

These catalogues include:
- The Data Catalogue (e.g. data schemas)
- The Function Catalogue (e.g. function inputs and data types)
- The Variable Catalogue (i.e. the @ variables)

The Binder performs these activities:
- schema lookup and propagation (add columns and types, add aliases)

"""

from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import Node
from opteryx.planner.binder.binder_visitor import BinderVisitor
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import apply_visibility_filters


def rename_relations(plan: LogicalPlan):
    """
    When we include VIEWs and CTEs in a plan, we randomize the name of the
    relations to avoid conflicts.
    """
    import uuid

    from orso.tools import random_string

    from opteryx.models import LogicalColumn

    relations = {}

    # first we collection the relations
    for nid, node in [
        (nid, node)
        for (nid, node) in plan.nodes(True)
        if node.node_type == LogicalPlanStepType.Scan
    ]:
        alias = f"$view-{random_string(4)}"
        unique_id = str(uuid.uuid4())
        relations[node.alias] = (node.relation, alias, unique_id)
        node.alias = alias
        node.uuid = unique_id
        plan[nid] = node

    def _prop(property):
        if isinstance(property, LogicalColumn) and property.source in relations:
            property.source = relations[property.source][1]
        if isinstance(property, list):
            return [_prop(p) for p in property]
        if isinstance(property, dict):
            return {k: _prop(v) for k, v in property.items()}
        if isinstance(property, Node):
            for p in property.properties:
                property.properties[p] = _prop(property.properties[p])
        return property

    for nid, node in plan.nodes(True):
        for property in node.properties:
            node.properties[property] = _prop(node.properties[property])

    return plan


def join_leg_preprocess(plan: LogicalPlan):
    for nid, node in (
        (nid, node)
        for (nid, node) in plan.nodes(True)
        if node.node_type == LogicalPlanStepType.Scan
    ):
        uuid = node.uuid

        location_nid = nid
        location_node = plan[location_nid]
        leg = None
        while location_nid:
            if location_node.node_type == LogicalPlanStepType.Join:
                if leg == "left":
                    location_node.left_readers.append(uuid)
                    location_node.left_relation_names.append(node.alias)
                elif leg == "right":
                    location_node.right_readers.append(uuid)
                    location_node.right_relation_names.append(node.alias)
                plan[location_nid] = location_node
            incoming = plan.outgoing_edges(location_nid)
            if incoming:
                location_nid = incoming[0][1]
                location_node = plan[location_nid]
                leg = incoming[0][2]
            else:
                location_nid = None

    return plan


def bind_logical_relations(plan: LogicalPlan, ctes: dict) -> LogicalPlan:
    """
    Bind the logical relations in the logical plan.

    Parameters:
        plan: LogicalPlan
            The logical plan.
        context: BindingContext
            The context needed for the binding phase.

    Returns:
        LogicalPlan: The logical plan with the logical relations bound.
    """
    from opteryx.managers.expression import NodeType
    from opteryx.models import Node
    from opteryx.planner.logical_planner import LogicalPlanStepType
    from opteryx.planner.views import is_view
    from opteryx.planner.views import view_as_plan

    if ctes is None:
        ctes = {}

    for nid, node in [
        (nid, node)
        for (nid, node) in plan.nodes(True)
        if node.node_type == LogicalPlanStepType.Scan
    ]:
        relation = node.relation
        sub_plan = None
        if is_view(relation):
            sub_plan = view_as_plan(relation)
        elif relation in ctes:
            sub_plan = ctes[relation]
        if sub_plan:
            sub_plan = rename_relations(sub_plan)
            sub_plan_head = sub_plan.get_exit_points()[0]
            consumer = plan.outgoing_edges(nid)[0]
            node.node_type = LogicalPlanStepType.Subquery
            node.columns = sub_plan[sub_plan_head].columns or [Node(NodeType.WILDCARD)]
            plan += sub_plan
            plan.add_edge(sub_plan_head, nid, consumer[2])

            plan = join_leg_preprocess(plan)

        # DEBUG: print(plan.draw())

    return plan


def do_bind_phase(
    plan: LogicalPlan,
    connection=None,
    qid: str = None,
    common_table_expressions: dict = None,
    visibility_filters: dict = None,
    statistics=None,
) -> LogicalPlan:
    """
    Execute the bind phase of the query engine.

    Parameters:
        plan: Any
            The logical plan.
        context: BindingContext
            The context needed for the binding phase.

    Returns:
        Modified logical plan after the binding phase.

    Raises:
        InvalidInternalStateError: Raised when the logical plan has more than one root node.
    """
    if common_table_expressions is None:
        common_table_expressions = {}

    plan = bind_logical_relations(plan, common_table_expressions)

    if visibility_filters:
        plan = apply_visibility_filters(plan, visibility_filters, statistics)

    binder_visitor = BinderVisitor()
    root_node = plan.get_exit_points()
    context = BindingContext.initialize(qid=qid, connection=connection)

    if len(root_node) > 1:
        raise InvalidInternalStateError(
            f"{context.qid} - logical plan has {len(root_node)} heads - this is an error"
        )

    plan, _ = binder_visitor.traverse(plan, root_node[0], context=context)

    return plan
