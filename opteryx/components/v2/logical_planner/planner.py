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
Represents a logical plan

A small set of functions are available in the logical plan (a set similar to, but
different from Cobb's relational algebra)

Steps are given random IDs to prevent collisions
"""

"""1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"""
import os
import sys
from enum import Enum
from enum import auto

from opteryx.components.logical_planner import builders
from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models.node import Node
from opteryx.third_party.travers import Graph
from opteryx.utils import random_string

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))


"""2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"""


class LogicalPlanStepType(int, Enum):
    PROJECT = auto()  # field selection
    SELECT = auto()  # tuple filtering
    UNION = auto()  #  appending relations
    DIFFERENCE = auto()  # relation interection
    JOIN = auto()  # all joina
    GROUP = auto()  # group by, without the aggregation
    AGGREGATE = auto()
    READ = auto()  # read a dataset
    SET = auto()  # set a variable
    LIMIT = auto()  # limit and offset
    ORDER = auto()  # order by
    DISTINCT = auto()

    CTE = auto()
    SUBQUERY = auto()


class LogicalPlan(Graph):
    pass


"""
CLAUSE PLANNERS
"""


def extract_ctes(branch, planner):
    ctes = {}
    if branch["with"]:
        for _ast in branch["with"]["cte_tables"]:
            alias = _ast.get("alias")["name"]["value"]
            plan = {"Query": _ast["query"]}
            ctes[alias] = planner(plan)
    return ctes


def extract_value(clause):
    if len(clause) == 1:
        return builders.build(clause[0])
    return [builders.build(token) for token in clause]


def extract_variable(clause):
    if len(clause) == 1:
        return clause[0]["value"]
    return [token["value"] for token in clause]


"""
STATEMENT PLANNERS
"""


def plan_query(statement):
    """ """

    def _inner_query_planner(sub_plan):
        inner_plan = LogicalPlan()
        step_id = None

        # from
        _relations = sub_plan["Select"]["from"]
        for relation in _relations:
            # TODO: if it's a subquery, expand it out
            from_step = Node(node_type=LogicalPlanStepType.READ, relation=relation)
            previous_step_id, step_id = step_id, random_string()
            previous_from_step_id, from_step_id = previous_step_id, step_id
            inner_plan.add_node(from_step_id, from_step)

            # joins
            _joins = relation["joins"]
            for join in _joins:
                # add the join node
                join_step = Node(node_type=LogicalPlanStepType.JOIN, join=join)
                previous_step_id, step_id = step_id, random_string()
                join_step_id = step_id
                inner_plan.add_node(join_step_id, join_step)
                # add the from table as the left side of the join
                inner_plan.add_edge(from_step_id, join_step_id, "left")
                # add the other side of the join
                # TODO: if it's a subquery, expand it out
                right_node = random_string()
                joined_read_step = Node(
                    node_type=LogicalPlanStepType.READ, relation=join["relation"]
                )
                inner_plan.add_node(right_node, joined_read_step)
                inner_plan.add_edge(right_node, join_step_id, "right")

            if len(_joins) == 0:
                if previous_from_step_id is not None:
                    inner_plan.add_edge(previous_from_step_id, step_id)

        # TODO: if there's any orphaned relations, they are implicit cross joins

        # selection
        _selection = builders.build(sub_plan["Select"]["selection"])
        if _selection:
            selection_step = Node(node_type=LogicalPlanStepType.SELECT)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, selection_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # groups
        _groups = builders.build(sub_plan["Select"]["group_by"])
        if _groups != []:
            group_step = Node(node_type=LogicalPlanStepType.GROUP, group=_groups)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, group_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # aggregates
        _projection = builders.build(sub_plan["Select"]["projection"])
        _aggregates = get_all_nodes_of_type(
            _projection, select_nodes=(NodeType.AGGREGATOR, NodeType.COMPLEX_AGGREGATOR)
        )
        if len(_aggregates) > 0:
            aggregate_step = Node(node_type=LogicalPlanStepType.AGGREGATE, aggregates=_aggregates)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, aggregate_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # projection
        _projection = [clause for clause in _projection if clause not in _aggregates]
        if not (len(_projection) == 1 and _projection[0].token_type == NodeType.WILDCARD):
            project_step = Node(node_type=LogicalPlanStepType.PROJECT, projection=_projection)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, project_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # having
        _having = builders.build(sub_plan["Select"]["having"])
        if _having:
            having_step = Node(node_type=LogicalPlanStepType.SELECT)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, having_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # distinct
        if sub_plan["Select"]["distinct"]:
            distinct_step = Node(node_type=LogicalPlanStepType.DISTINCT)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, distinct_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # order
        _order_by = sub_plan["order_by"]
        if _order_by:
            order_step = Node(node_type=LogicalPlanStepType.ORDER, order=_order_by)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, order_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # limit/offset
        _limit = sub_plan["limit"]
        _offset = sub_plan["offset"]
        if _limit or _offset:
            limit_step = Node(node_type=LogicalPlanStepType.LIMIT, limit=_limit, offset=_offset)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, limit_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        return inner_plan

    # CTEs need to be extracted so we can deal with them later
    raw_ctes = extract_ctes(statement["Query"], _inner_query_planner)

    # union?
    if "SetOperator" in statement["Query"]["body"]:
        plan = LogicalPlan()
        root_node = statement["Query"]["body"]["SetOperator"]
        _left = _inner_query_planner(root_node["left"])
        _right = _inner_query_planner(root_node["right"])
        _operator = root_node["op"]
        # join the plans together
        raise NotImplementedError("Set Operators (UNION) not implemented")

    root_node = statement["Query"]["body"]
    # we do some minor AST rewriting
    root_node["limit"] = statement["Query"].get("limit", None)
    root_node["offset"] = statement["Query"].get("offset", None)
    root_node["order_by"] = statement["Query"].get("order_by", None)
    return _inner_query_planner(root_node)


def plan_set_variable(statement):
    root_node = "SetVariable"
    plan = LogicalPlan()
    set_step = Node(
        node_type=LogicalPlanStepType.SET,
        variable=extract_variable(statement[root_node]["variable"]),
        value=extract_value(statement[root_node]["value"]),
    )
    plan.add_node(random_string(), set_step)
    return plan


def plan_show_variables(statement):
    root_node = "ShowVariables"
    plan = LogicalPlan()

    read_step = Node(node_type=LogicalPlanStepType.READ, source="$variables")
    step_id = random_string()
    plan.add_node(step_id, read_step)

    predicate = statement[root_node]["filter"]
    if predicate is not None:
        operator = next(iter(predicate))
        select_step = Node(
            node_type=LogicalPlanStepType.SELECT,
            predicate=ExpressionTreeNode(
                token_type=NodeType.COMPARISON_OPERATOR,
                value=operator,
                left=ExpressionTreeNode(token_type=NodeType.IDENTIFIER, value="name"),
                right=predicate[operator],
            ),
        )
        previous_step_id, step_id = step_id, random_string()
        plan.add_node(step_id, select_step)
        plan.add_edge(previous_step_id, step_id)

    return plan


QUERY_BUILDERS = {
    #    "Analyze": analyze_query,
    #    "Explain": explain_query,
    "Query": plan_query,
    "SetVariable": plan_set_variable,
    #    "ShowColumns": show_columns_query,
    #    "ShowCreate": show_create_query,
    #    "ShowFunctions": show_functions_query,
    #    "ShowVariable": show_variable_query,  # generic SHOW handler
    "ShowVariables": plan_show_variables,
}


def get_planners(parsed_statements):
    # The sqlparser ast is an array of asts
    for parsed_statement in parsed_statements:
        statement_type = next(iter(parsed_statement))
        yield QUERY_BUILDERS[statement_type], parsed_statement


if __name__ == "__main__":  # pragma: no cover
    import json

    import opteryx.third_party.sqloxide

    SQL = "SET enable_optimizer = 7"
    SQL = "SELECT * FROM $planets"
    SQL = "SELECT DISTINCT MAX(planetId), name FROM $satellites INNER JOIN $planets ON $planets.id = $satellites.id WHERE id = 1 GROUP BY planetId HAVING id > 2 ORDER BY name LIMIT 1 OFFSET 1"
    TSQL = "SELECT * FROM T1, T2"
    SQL = "SET @planet = 'Saturn'; SELECT name AS nom FROM (SELECT DISTINCT id as planetId, name FROM $planets WHERE name = @planet) as planets -- LEFT JOIN (SELECT planetId, COUNT(*) FROM $satellites FOR DATES BETWEEN '2022-01-01' AND TODAY WHERE gm > 10) AS bigsats ON bigsats.planetId = planets.planetId -- LEFT JOIN (SELECT planetId, COUNT(*) FROM $satellites FOR DATES IN LAST_MONTH WHERE gm < 10) as smallsats ON smallsats.planetId = planets.planetId ; "

    parsed_statements = opteryx.third_party.sqloxide.parse_sql(SQL, dialect="mysql")
    print(json.dumps(parsed_statements, indent=2))
    for planner, ast in get_planners(parsed_statements):
        print("---")
        print(planner(ast).draw())
