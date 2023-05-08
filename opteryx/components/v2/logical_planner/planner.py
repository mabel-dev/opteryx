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

import os
import sys
from enum import Enum
from enum import auto

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))  # isort:skip

from opteryx.components.logical_planner import builders
from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import format_expression
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models.node import Node
from opteryx.third_party.travers import Graph
from opteryx.utils import random_string


class LogicalPlanStepType(int, Enum):
    Project = auto()  # field selection
    Filter = auto()  # tuple filtering
    Union = auto()  #  appending relations
    Difference = auto()  # relation interection
    Join = auto()  # all joina
    Group = auto()  # group by, without the aggregation
    Aggregate = auto()
    Scan = auto()  # read a dataset
    Set = auto()  # set a variable
    Limit = auto()  # limit and offset
    Order = auto()  # order by
    Distinct = auto()

    CTE = auto()
    Subquery = auto()
    Values = auto()


class LogicalPlan(Graph):
    pass


class LogicalPlanNode(Node):
    def __str__(self):
        try:
            if self.node_type == LogicalPlanStepType.Scan:
                return f"Scan ({self.relation}{' AS ' + self.alias if self.alias else ''}{' WITH(' + ','.join(self.hints) + ')' if self.hints else ''})"
            if self.node_type == LogicalPlanStepType.Join:
                if self.on:
                    return f"{self.type} ({format_expression(self.filter)})"
                if self.using:
                    return f"{self.type} (USING {','.join(format_expression(self.using))})"
                return self.type
            if self.node_type == LogicalPlanStepType.Filter:
                return "Filter (condition)"
            if self.node_type == LogicalPlanStepType.Subquery:
                return f"Subquery{' AS ' + self.alias if self.alias else ''}"
            if self.node_type == LogicalPlanStepType.Values:
                return f"Values (({', '.join(self.columns)}) x {len(self.values)} AS {self.alias})"
        except:
            pass
        return f"{str(self.node_type)[20:]}"


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


def create_node_relation(relation):
    sub_plan = LogicalPlan()
    root_node = None

    if "Derived" in relation["relation"]:
        if relation["relation"]["Derived"]["subquery"]:
            subquery = relation["relation"]["Derived"]
            if "Values" not in subquery["subquery"]["body"]:
                subquery_step = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
                subquery_step.alias = (
                    None if subquery["alias"] is None else subquery["alias"]["name"]["value"]
                )
                step_id = random_string()
                sub_plan.add_node(step_id, subquery_step)

                subquery_plan = plan_query(subquery["subquery"])

                sub_plan += subquery_plan
                subquery_entry_id = subquery_plan.get_exit_points()[0]
                sub_plan.add_edge(subquery_entry_id, step_id)

                root_node = step_id
                relation["step_id"] = step_id
            else:
                values_step = LogicalPlanNode(node_type=LogicalPlanStepType.Values)
                values_step.alias = subquery["alias"]["name"]["value"]
                values_step.columns = tuple(col["value"] for col in subquery["alias"]["columns"])
                values_step.values = [
                    tuple(builders.build(value["Value"]).value for value in row)
                    for row in subquery["subquery"]["body"]["Values"]["rows"]
                ]
                step_id = random_string()
                sub_plan.add_node(step_id, values_step)
                root_node = step_id
        else:
            raise NotImplementedError(relation["relation"]["Derived"])
    else:
        from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
        table = relation["relation"]["Table"]
        from_step.relation = ".".join(part["value"] for part in table["name"])
        from_step.alias = None if table["alias"] is None else table["alias"]["name"]["value"]
        from_step.hints = [hint["Identifier"]["value"] for hint in table["with_hints"]]
        step_id = random_string()
        sub_plan.add_node(step_id, from_step)

        root_node = step_id
        relation["step_id"] = step_id

    # joins
    _joins = relation.get("joins", [])
    for join in _joins:
        # add the join node
        # TODO: joins need: type, filter
        join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join, join=join["join_operator"])
        join_operator = next(iter(join["join_operator"]))
        join_condition = next(iter(join["join_operator"][join_operator]))
        join_step.type = {
            "CrossJoin": "Cross Join",
            "LeftOuter": "Left Join",
            "Inner": "Inner Join",
            "RightOuter": "Right Join",
        }.get(join_operator)
        if join_condition == "On":
            join_step.on = builders.build(join["join_operator"][join_operator][join_condition])
        else:
            join_step.using = [
                builders.build({"Identifier": identifier})
                for identifier in join["join_operator"][join_operator][join_condition]
            ]
        join_step_id = random_string()
        sub_plan.add_node(join_step_id, join_step)
        # add the other side of the join

        right_node_id, right_plan = create_node_relation(join)
        sub_plan += right_plan

        # add the from table as the left side of the join
        sub_plan.add_edge(root_node, join_step_id, "left")
        sub_plan.add_edge(right_node_id, join_step_id, "right")

        root_node = join_step_id

    return root_node, sub_plan


def plan_query(statement):
    """ """

    def _inner_query_planner(ast_branch):
        inner_plan = LogicalPlan()
        step_id = None

        # from
        _relations = ast_branch["Select"]["from"]
        for relation in _relations:
            step_id, sub_plan = create_node_relation(relation)
            inner_plan += sub_plan

        # If there's any peer relations, they are implicit cross joins
        if len(_relations) > 1:
            join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join, join={"CrossJoin": []})
            step_id = random_string()
            inner_plan.add_node(step_id, join_step)
            for relation in _relations:
                inner_plan.add_edge(relation["step_id"], step_id)

        # selection
        _selection = builders.build(ast_branch["Select"]["selection"])
        if _selection:
            # TODO: filters need: condition
            selection_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, selection_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # groups
        _groups = builders.build(ast_branch["Select"]["group_by"])
        if _groups != []:
            # TODO: groups need: grouped columns
            group_step = LogicalPlanNode(node_type=LogicalPlanStepType.Group, group=_groups)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, group_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # aggregates
        _projection = builders.build(ast_branch["Select"]["projection"])
        _aggregates = get_all_nodes_of_type(
            _projection, select_nodes=(NodeType.AGGREGATOR, NodeType.COMPLEX_AGGREGATOR)
        )
        if len(_aggregates) > 0:
            # TODO: aggregates need: functions
            aggregate_step = LogicalPlanNode(
                node_type=LogicalPlanStepType.Aggregate, aggregates=_aggregates
            )
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, aggregate_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # projection
        _projection = [clause for clause in _projection if clause not in _aggregates]
        if not (len(_projection) == 1 and _projection[0].token_type == NodeType.WILDCARD):
            # TODO: projection needs: functions, columns, aliases
            project_step = LogicalPlanNode(
                node_type=LogicalPlanStepType.Project, projection=_projection
            )
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, project_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # having
        _having = builders.build(ast_branch["Select"]["having"])
        if _having:
            # TODO: filters need: condition
            having_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, having_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # distinct
        if ast_branch["Select"]["distinct"]:
            # TODO: distinct needs: columns to distinct on, keep 1st/last
            distinct_step = LogicalPlanNode(node_type=LogicalPlanStepType.Distinct)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, distinct_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # order
        _order_by = ast_branch["order_by"]
        if _order_by:
            # TODO: order by needs: columns and directions
            order_step = LogicalPlanNode(node_type=LogicalPlanStepType.Order, order=_order_by)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, order_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # limit/offset
        _limit = ast_branch["limit"]
        _offset = ast_branch["offset"]
        if _limit or _offset:
            # TODO: limit needs: the limit
            limit_step = LogicalPlanNode(
                node_type=LogicalPlanStepType.Limit, limit=_limit, offset=_offset
            )
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, limit_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        return inner_plan

    root_node = statement
    if "Query" in root_node:
        root_node = root_node["Query"]

    # CTEs need to be extracted so we can deal with them later
    raw_ctes = extract_ctes(root_node, _inner_query_planner)

    # union?
    if "SetOperator" in root_node["body"]:
        plan = LogicalPlan()
        root_node = root_node["body"]["SetOperator"]
        _left = _inner_query_planner(root_node["left"])
        _right = _inner_query_planner(root_node["right"])
        _operator = root_node["op"]
        # join the plans together
        raise NotImplementedError("Set Operators (UNION) not implemented")

    # we do some minor AST rewriting
    root_node["body"]["limit"] = root_node.get("limit", None)
    root_node["body"]["offset"] = root_node.get("offset", None)
    root_node["body"]["order_by"] = root_node.get("order_by", None)
    return _inner_query_planner(root_node["body"])


def plan_set_variable(statement):
    root_node = "SetVariable"
    plan = LogicalPlan()
    set_step = LogicalPlanNode(
        node_type=LogicalPlanStepType.Set,
        variable=extract_variable(statement[root_node]["variable"]),
        value=extract_value(statement[root_node]["value"]),
    )
    plan.add_node(random_string(), set_step)
    return plan


def plan_show_variables(statement):
    root_node = "ShowVariables"
    plan = LogicalPlan()

    read_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan, source="$variables")
    step_id = random_string()
    plan.add_node(step_id, read_step)

    predicate = statement[root_node]["filter"]
    if predicate is not None:
        operator = next(iter(predicate))
        select_step = LogicalPlanNode(
            node_type=LogicalPlanStepType.Filter,
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

    TSQL = "SET enable_optimizer = 7"
    SQL = "SELECT id FROM $planets"
    SQL = "SELECT DISTINCT MAX(planetId), name FROM $satellites INNER JOIN $planets ON $planets.id = $satellites.id WHERE id = 1 GROUP BY planetId HAVING id > 2 ORDER BY name LIMIT 1 OFFSET 1"
    SQL = "SELECT a FROM T1, T2"
    SQL = "SET @planet = 'Saturn'; SELECT name AS nom FROM (SELECT DISTINCT id as planetId, name FROM $planets WHERE name = @planet) as planets -- LEFT JOIN (SELECT planetId, COUNT(*) FROM $satellites FOR DATES BETWEEN '2022-01-01' AND TODAY WHERE gm > 10) AS bigsats ON bigsats.planetId = planets.planetId -- LEFT JOIN (SELECT planetId, COUNT(*) FROM $satellites FOR DATES IN LAST_MONTH WHERE gm < 10) as smallsats ON smallsats.planetId = planets.planetId ; "
    SQL = """SELECT name AS nom , bigsats.occurances , smallsats.occurances 
  FROM ( SELECT DISTINCT id as planetId , name FROM $planets WHERE name = 'Earth' ) as planets 
  Inner JOIN ( SELECT planetId , COUNT ( * ) AS occurances FROM $satellites WHERE gm > 10 GROUP BY planetId ) AS bigsats 
       ON bigsats.planetId = planets.planetId
  left JOIN ( SELECT planetId , COUNT ( * ) AS occurances FROM $satellites WHERE gm < 10 GROUP BY planetId ) as smallsats 
       ON smallsats.planetId = planets.planetId ;"""
    TSQL = "SELECT COUNT(*) FROM $astronauts WHERE $astronauts.a = $astronauts.b GROUP BY a"
    TSQL = "SELECT * FROM (SELECT * FROM $planets)"
    TSQL = "SELECT * FROM (VALUES ('High', 3),('Medium', 2),('Low', 1)) AS ratings(name, rating) "
    SQL = "SELECT * FROM tablio INNER JOIN table2 USING(b)"

    parsed_statements = opteryx.third_party.sqloxide.parse_sql(SQL, dialect="mysql")
    # print(json.dumps(parsed_statements, indent=2))
    for planner, ast in get_planners(parsed_statements):
        print("---")
        tree = planner(ast)

        #        print(tree.breadth_first_search(tree.get_entry_points()[0]))
        print(json.dumps(tree.depth_first_search(), indent=2))
        #        print(opteryx.query("EXPLAIN " + SQL))
        print(tree.draw())
#        print(tree.draw())
