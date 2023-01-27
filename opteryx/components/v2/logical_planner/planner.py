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

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.utils import unique_id
from opteryx.components.logical_planner import builders

"""2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"""


from enum import auto, Enum

from opteryx.components.logical_planner import builders
from opteryx.managers.expression import ExpressionTreeNode, NodeType
from opteryx.third_party.travers import Graph
from opteryx.utils import unique_id


class LogicalPlanStepType(int, Enum):
    PROJECT = auto()  # field selection
    SELECT = auto()  # tuple filtering
    UNION = auto()  #  appending relations
    DIFFERENCE = auto()  # relation interection
    RENAME = auto()  # field renaming, including evaluation
    JOIN = auto()  # all joina
    GROUP = auto()  # group by, without the aggregation
    READ = auto()  # read a dataset
    SET = auto()  # set a variable
    LIMIT = auto()  # limit and offset
    ORDER = auto()  # order by
    DISTINCT = auto()

    CTE = auto()
    SUBQUERY = auto()


class LogicalPlan(Graph):
    def get_relations(self):
        relations = []
        for nid, node in self._nodes.items():
            if node["node_type"] == LogicalPlanStepType.READ:
                relations.append(nid)
        return relations


"""
CLAUSE PLANNERS
"""


def extract_ctes(branch, planner):
    ctes = {}
    if branch["with"]:
        for _ast in branch["with"]["cte_tables"]:
            alias = _ast.pop("alias")["name"]["value"]
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
    """
    01. FROM
    02. JOIN
    03. WHERE
    04. GROUP BY
    05. HAVING
    06. SELECT
    07. DISTINCT
    08. ORDER BY
    09. OFFSET
    10. LIMIT
    """

    def _inner_query_planner(sub_plan):
        inner_plan = LogicalPlan()
        step_id = None

        # from
        _relations = sub_plan["Select"]["from"]
        for relation in _relations:
            read_step = {"node_type": LogicalPlanStepType.READ, "relation": relation}
            previous_step_id, step_id = step_id, unique_id()
            inner_plan.add_node(step_id, read_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # joins
        _joins = sub_plan["Select"]["from"][0]["joins"]
        for join in _joins:
            join_step = {"node_type": LogicalPlanStepType.JOIN, "join": join}
            previous_step_id, step_id = step_id, unique_id()
            inner_plan.add_node(step_id, join_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id, "left")
            # add the other side of the join
            previous_step_id, step_id = step_id, unique_id()
            read_step = {"node_type": LogicalPlanStepType.READ, "relation": join["relation"]}
            inner_plan.add_node(step_id, read_step)
            inner_plan.add_edge(previous_step_id, step_id, "right")

        # there's any orphaned relations, they are implicit cross joins
        if len(_joins) < len(_relations):
            pass

        # groups
        _groups = builders.build(sub_plan["Select"]["group_by"])
        if _groups is not None:
            group_step = {"node_type": LogicalPlanStepType.GROUP, "group": _groups}
            previous_step_id, step_id = step_id, unique_id()
            inner_plan.add_node(step_id, group_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # aggregates

        # projection

        # selection

        # if groups or aggregates:
        #   having

        # order

        # distinct
        if sub_plan["Select"]["distinct"]:
            distinct_step = {"node_type": LogicalPlanStepType.DISTINCT}
            previous_step_id, step_id = step_id, unique_id()
            inner_plan.add_node(step_id, distinct_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # limit/offset
        _limit = sub_plan["limit"]
        _offset = sub_plan["offset"]
        if _limit or _offset:
            limit_step = {"node_type": LogicalPlanStepType.LIMIT, "limit": _limit, "offset": _offset }
            previous_step_id, step_id = step_id, unique_id()
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
    root_node["limit"] = statement["Query"].pop("limit", None)
    root_node["offset"] = statement["Query"].pop("offset", None)
    return _inner_query_planner(root_node)


def plan_set_variable(statement):
    root_node = "SetVariable"
    plan = LogicalPlan()
    set_step = {
        "node_type": LogicalPlanStepType.SET,
        "variable": extract_variable(statement[root_node]["variable"]),
        "value": extract_value(statement[root_node]["value"]),
    }
    plan.add_node(unique_id(), set_step)
    return plan


def plan_show_variables(statement):
    root_node = "ShowVariables"
    plan = LogicalPlan()

    read_step = {
        "node_type": LogicalPlanStepType.READ,
        "source": "$variables",
    }
    step_id = unique_id()
    plan.add_node(step_id, read_step)

    predicate = statement[root_node]["filter"]
    if predicate is not None:
        operator = next(iter(predicate))
        select_step = {
            "node_type": LogicalPlanStepType.SELECT,
            "predicate": ExpressionTreeNode(
                token_type=NodeType.COMPARISON_OPERATOR,
                value=operator,
                left=ExpressionTreeNode(token_type=NodeType.IDENTIFIER, value="name"),
                right=predicate[operator],
            ),
        }
        print(select_step)
        previous_step_id, step_id = step_id, unique_id()
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


if __name__ == "__main__":
    import json
    import opteryx.third_party.sqloxide

    SQL = "SET enable_optimizer = 7"
    SQL = "SELECT * FROM $planets"
    SQL = "SELECT DISTINCT MAX(planetId), name FROM $satellites INNER JOIN $planets ON $planets.id = $satellites.id GROUP BY planetId ORDER BY name LIMIT 1 OFFSET 1"

    parsed_statements = opteryx.third_party.sqloxide.parse_sql(SQL, dialect="mysql")
    print(json.dumps(parsed_statements, indent=2))
    for planner, ast in get_planners(parsed_statements):
        print(planner(ast).draw())
