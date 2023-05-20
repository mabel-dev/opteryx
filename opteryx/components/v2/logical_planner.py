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
~~~
                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                               │
   │ SQL       │                               │
   │  Rewriter │                               │
   └─────┬─────┘                               │
         │SQL                                  │Plan
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ AST       │      │           │Stats │           │
   │ Rewriter  │      │ Catalogue ├──────► Optimizer │
   └─────┬─────┘      └─────┬─────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │ Tree      │
   │   Planner ├──────► Binder    ├──────►  Rewriter │
   └───────────┘      └───────────┘      └───────────┘
~~~
Converts the AST to a logical query plan.

The plan does not try to be efficient or clever, at this point it is only trying to be correct.
"""

import os
import sys
from enum import Enum
from enum import auto

from opteryx.components.logical_planner import builders
from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import format_expression
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models.node import Node
from opteryx.third_party.travers import Graph
from opteryx.utils import random_string

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))  # isort:skip


class LogicalPlanStepType(int, Enum):
    Project = auto()  # field selection
    Filter = auto()  # tuple filtering
    Union = auto()  #  appending relations
    Explain = auto()  # EXPLAIN
    Difference = auto()  # relation interection
    Join = auto()  # all joins
    Group = auto()  # group by, without the aggregation
    Aggregate = auto()
    Scan = auto()  # read a dataset
    Show = auto()  # show a variable
    ShowColumns = auto()  # SHOW COLUMNS
    Set = auto()  # set a variable
    Limit = auto()  # limit and offset
    Order = auto()  # order by
    Distinct = auto()

    CTE = auto()
    Subquery = auto()
    Values = auto()
    Unnest = auto()
    GenerateSeries = auto()
    Fake = auto()


class LogicalPlan(Graph):
    pass


class LogicalPlanNode(Node):
    def __str__(self):
        try:
            # fmt:off
            if self.node_type == LogicalPlanStepType.Explain:
                return f"EXPLAIN{' ANALYZE' if self.analyze else ''}{(' (' + self.format + ')') if self.format else ''}"
            if self.node_type == LogicalPlanStepType.Fake:
                return f"FAKE ({', '.join(format_expression(arg) for arg in self.args)}{' AS ' + self.alias if self.alias else ''})"
            if self.node_type == LogicalPlanStepType.Filter:
                return f"FILTER ({format_expression(self.condition)})"
            if self.node_type == LogicalPlanStepType.GenerateSeries:
                return f"GENERATE SERIES ({', '.join(format_expression(arg) for arg in self.args)}{' AS ' + self.alias if self.alias else ''})"
            if self.node_type == LogicalPlanStepType.Group:
                return f"GROUP ({', '.join(format_expression(col) for col in self.columns)})"
            if self.node_type == LogicalPlanStepType.Join:
                if self.on:
                    return f"{self.type.upper()} ({format_expression(self.on)})"
                if self.using:
                    return f"{self.type.upper()} (USING {','.join(format_expression(self.using))})"
                return self.type.upper()
            if self.node_type == LogicalPlanStepType.Order:
                return f"ORDER BY ({', '.join(item[0] + (' DESC' if not item[1] else '') for item in self.order_by)})"
            if self.node_type == LogicalPlanStepType.Project:
                return f"PROJECT ({', '.join(format_expression(col) for col in self.columns)})"
            if self.node_type == LogicalPlanStepType.Scan:
                date_range = ""
                if self.start_date == self.end_date:
                    if self.start_date is not None:
                        date_range = f" FOR '{self.start_date}'"
                else:
                    date_range = f" FOR '{self.start_date}' TO '{self.end_date}'"
                return f"SCAN ({self.relation}{' AS ' + self.alias if self.alias else ''}{date_range}{' WITH(' + ','.join(self.hints) + ')' if self.hints else ''})"
            if self.node_type == LogicalPlanStepType.Show:
                return f"SHOW ({', '.join(self.items)})"
            if self.node_type == LogicalPlanStepType.ShowColumns:
                return f"SHOW{' FULL' if self.full else ''}{' EXTENDED' if self.extended else ''} COLUMNS ({self.relation})"
            if self.node_type == LogicalPlanStepType.Subquery:
                return f"SUBQUERY{' AS ' + self.alias if self.alias else ''}"
            if self.node_type == LogicalPlanStepType.Unnest:
                return f"UNNEST ({', '.join(format_expression(arg) for arg in self.args)}{' AS ' + self.alias if self.alias else ''})"
            if self.node_type == LogicalPlanStepType.Union:
                return f"UNION {'' if self.modifier is None else self.modifier.upper()}"
            if self.node_type == LogicalPlanStepType.Values:
                return f"VALUES (({', '.join(self.columns)}) x {len(self.values)} AS {self.alias})"

            # fmt:on
        except Exception as err:
            print(err)
        return f"{str(self.node_type)[20:].upper()}"


"""
CLAUSE PLANNERS
"""


def extract_ctes(branch, planner):
    ctes = {}
    if branch.get("with"):
        for _ast in branch["with"]["cte_tables"]:
            alias = _ast.get("alias")["name"]["value"]
            ctes[alias] = planner(_ast["query"]["body"])
    return ctes


def extract_value(clause):
    if len(clause) == 1:
        return builders.build(clause[0])
    return [builders.build(token) for token in clause]


def extract_variable(clause):
    if len(clause) == 1:
        return clause[0]["value"]
    return [token["value"] for token in clause]


def extract_simple_filter(filters, identifier: str = "Name"):
    if "Like" in filters:
        left = ExpressionTreeNode(NodeType.IDENTIFIER, value=identifier)
        right = ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=filters["Like"])
        root = ExpressionTreeNode(
            NodeType.COMPARISON_OPERATOR,
            value="ILike",  # we're case insensitive for SHOW filters
            left=left,
            right=right,
        )
        return root
    if "Where" in filters:
        root = builders.build(filters["Where"])
        return root


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
                # SUBQUERY nodes wrap other queries and the result is available as a relation in
                # the parent query.
                #
                # We have the name of the relation (alias), the query is added as a query plan to
                # the parent plan.
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
                # VALUES nodes are where the relation is defined within the SQL statement.
                # e.g. SELECT * FROM (VALUES(1),(2)) AS numbers (number)
                #
                # We have the name of the relation (alias), the column names (columns) and the
                # values in each row (values)
                values_step = LogicalPlanNode(node_type=LogicalPlanStepType.Values)
                values_step.alias = subquery["alias"]["name"]["value"]
                values_step.columns = tuple(col["value"] for col in subquery["alias"]["columns"])
                values_step.values = [
                    tuple(builders.build(value["Value"]) for value in row)
                    for row in subquery["subquery"]["body"]["Values"]["rows"]
                ]
                step_id = random_string()
                sub_plan.add_node(step_id, values_step)
                root_node = step_id
        else:
            raise NotImplementedError(relation["relation"]["Derived"])
    elif relation["relation"]["Table"]["args"]:
        function = relation["relation"]["Table"]
        function_name = function["name"][0]["value"].upper()
        if function_name == "UNNEST":
            function_step = LogicalPlanNode(node_type=LogicalPlanStepType.Unnest)
        elif function_name == "GENERATE_SERIES":
            function_step = LogicalPlanNode(node_type=LogicalPlanStepType.GenerateSeries)
        elif function_name == "FAKE":
            function_step = LogicalPlanNode(node_type=LogicalPlanStepType.Fake)
        else:
            raise NotImplementedError(f"function {function_name}")
        function_step.alias = (
            None if function["alias"] is None else function["alias"]["name"]["value"]
        )
        function_step.args = [builders.build(arg) for arg in function["args"]]

        step_id = random_string()
        sub_plan.add_node(step_id, function_step)
        root_node = step_id
    else:
        # SCAN nodes are where we read relations; these can be from memory, disk or a remote
        # system. This has many physical implementations but at this point all we have is the
        # name/location of the relation (relation), what the relation is called inside the
        # query (alias) and if there are any hints (hints)
        from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
        table = relation["relation"]["Table"]
        from_step.relation = ".".join(part["value"] for part in table["name"])
        from_step.alias = (
            from_step.relation if table["alias"] is None else table["alias"]["name"]["value"]
        )
        from_step.hints = [hint["Identifier"]["value"] for hint in table["with_hints"]]
        from_step.start_date = table.get("start_date")
        from_step.end_date = table.get("end_date")
        step_id = random_string()
        sub_plan.add_node(step_id, from_step)

        root_node = step_id
        relation["step_id"] = step_id

    # joins
    _joins = relation.get("joins", [])
    for join in _joins:
        # add the join node
        join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join, join=join["join_operator"])
        if join["join_operator"] == {"Inner": "Natural"}:
            join_step.type = "Natural Join"
        elif join["join_operator"] == "CrossJoin":
            join_step.type = "Cross Join"
        else:
            join_operator = next(iter(join["join_operator"]))
            join_condition = next(iter(join["join_operator"][join_operator]))
            join_step.type = {
                "FullOuter": "Full Outer Join",
                "Inner": "Inner Join",
                "LeftAnti": "Left Anti Join",
                "LeftOuter": "Left Outer Join",
                "LeftSemi": "Left Semi Join",
                "RightAnti": "Right Anti Join",
                "RightOuter": "Right Outer Join",
                "RightSemi": "Right Semi Join",
            }.get(join_operator, join_operator)
            if join_condition == "On":
                join_step.on = builders.build(join["join_operator"][join_operator][join_condition])
            elif join_condition == "Using":
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


def plan_explain(statement):
    plan = LogicalPlan()
    explain_node = LogicalPlanNode(node_type=LogicalPlanStepType.Explain)
    explain_node.analyze = statement["Explain"]["analyze"]
    explain_node.format = statement["Explain"]["format"]

    explain_id = random_string()
    plan.add_node(explain_id, explain_node)

    sub_plan = plan_query(statement=statement["Explain"]["statement"])
    sub_plan_id = sub_plan.get_exit_points()[0]
    plan += sub_plan
    plan.add_edge(sub_plan_id, explain_id)

    return plan


def plan_query(statement):
    """ """

    def _inner_query_planner(ast_branch):
        inner_plan = LogicalPlan()
        step_id = None

        # from
        _relations = ast_branch["Select"].get("from", [])
        for relation in _relations:
            step_id, sub_plan = create_node_relation(relation)
            inner_plan += sub_plan

        # If there's any peer relations, they are implicit cross joins
        if len(_relations) > 1:
            join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
            join_step.type = "Cross Join"
            step_id = random_string()
            inner_plan.add_node(step_id, join_step)
            for relation in _relations:
                inner_plan.add_edge(relation["step_id"], step_id)

        # selection
        _selection = builders.build(ast_branch["Select"].get("selection"))
        if _selection:
            selection_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
            selection_step.condition = _selection
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, selection_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # groups
        _groups = builders.build(ast_branch["Select"].get("group_by"))
        if _groups is not None and _groups != []:
            # TODO: groups need: grouped columns
            group_step = LogicalPlanNode(node_type=LogicalPlanStepType.Group)
            group_step.columns = _groups
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, group_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # aggregates
        _projection = builders.build(ast_branch["Select"].get("projection")) or []
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
        if not _projection == [] and not (
            len(_projection) == 1 and _projection[0].token_type == NodeType.WILDCARD
        ):
            project_step = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
            project_step.columns = _projection
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, project_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # having
        _having = builders.build(ast_branch["Select"].get("having"))
        if _having:
            having_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
            having_step.condition = _having
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, having_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # distinct
        if ast_branch["Select"].get("distinct"):
            # TODO: distinct needs: columns to distinct on, keep 1st/last
            distinct_step = LogicalPlanNode(node_type=LogicalPlanStepType.Distinct)
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, distinct_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # order
        _order_by = ast_branch.get("order_by")
        if _order_by:
            order_step = LogicalPlanNode(node_type=LogicalPlanStepType.Order)
            order_step.order_by = [
                (builders.build(item["expr"]).value, not bool(item["asc"])) for item in _order_by
            ]
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, order_step)
            if previous_step_id is not None:
                inner_plan.add_edge(previous_step_id, step_id)

        # limit/offset
        _limit = ast_branch.get("limit")
        _offset = ast_branch.get("offset")
        if _limit or _offset:
            limit_step = LogicalPlanNode(node_type=LogicalPlanStepType.Limit)
            limit_step.limit = _limit
            limit_step.offset = _offset
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
    if "SetOperation" in root_node["body"]:
        set_operation = root_node["body"]["SetOperation"]

        if set_operation["op"] == "Union":
            set_op_node = LogicalPlanNode(node_type=LogicalPlanStepType.Union)
        else:
            raise NotImplementedError(f"Unsupported SET operator {set_operation['op']}")
        set_op_node.modifier = (
            None if set_operation["set_quantifier"] == "None" else set_operation["set_quantifier"]
        )
        step_id = random_string()
        plan = LogicalPlan()
        plan.add_node(step_id, set_op_node)

        left_plan = _inner_query_planner(set_operation["left"])
        plan += left_plan
        subquery_entry_id = left_plan.get_exit_points()[0]
        plan.add_edge(subquery_entry_id, step_id)

        right_plan = _inner_query_planner(set_operation["left"])
        plan += right_plan
        subquery_entry_id = right_plan.get_exit_points()[0]
        plan.add_edge(subquery_entry_id, step_id)

        root_node["Select"] = {}
        parent_plan = _inner_query_planner(root_node)
        if len(parent_plan) > 0:
            plan += parent_plan
            parent_plan_exit_id = parent_plan.get_entry_points()[0]
            plan.add_edge(step_id, parent_plan_exit_id)

        return plan

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


def plan_show_columns(statement):
    root_node = "ShowColumns"
    plan = LogicalPlan()
    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.ShowColumns)
    show_step.extended = statement[root_node]["extended"]
    show_step.full = statement[root_node]["full"]
    show_step.relation = ".".join([part["value"] for part in statement[root_node]["table_name"]])
    show_step_id = random_string()
    plan.add_node(show_step_id, show_step)

    _filter = statement[root_node]["filter"]
    if _filter:
        filter_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        filter_node.filter = extract_simple_filter(_filter, "Column")
        filter_node_id = random_string()
        plan.add_node(filter_node_id, filter_node)
        plan.add_edge(filter_node_id, show_step_id)

    return plan


def plan_show_variable(statement):
    root_node = "ShowVariable"
    plan = LogicalPlan()
    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.Show)
    show_step.items = extract_variable(statement[root_node]["variable"])
    plan.add_node(random_string(), show_step)
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
    #    "Execute": plan_execute_query,
    "Explain": plan_explain,
    "Query": plan_query,
    "SetVariable": plan_set_variable,
    "ShowColumns": plan_show_columns,
    #    "ShowCreate": show_create_query,
    #    "ShowFunctions": show_functions_query,
    "ShowVariable": plan_show_variable,  # generic SHOW handler
    "ShowVariables": plan_show_variables,
}


def do_logical_planning_phase(parsed_statements):
    # The sqlparser ast is an array of asts
    for parsed_statement in parsed_statements:
        statement_type = next(iter(parsed_statement))
        yield QUERY_BUILDERS[statement_type](parsed_statement), parsed_statement
