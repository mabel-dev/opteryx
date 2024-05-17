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
         │SQL                                  │Results
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐                         ╔═══════════╗
   │ AST       │                         ║Cost-Based ║
   │ Rewriter  │                         ║ Optimizer ║
   └─────┬─────┘                         ╚═════▲═════╝
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ Logical   │ Plan │ Heuristic │ Plan │           │
   │   Planner ├──────► Optimizer ├──────► Binder    │
   └───────────┘      └───────────┘      └─────▲─────┘
                                               │Schemas
                                         ┌─────┴─────┐
                                         │           │
                                         │ Catalogue │
                                         └───────────┘
~~~
Converts the AST to a logical query plan.

The plan does not try to be efficient or clever, at this point it is only trying to be correct.
"""

from enum import Enum
from enum import auto
from typing import Generator
from typing import List
from typing import Optional
from typing import Tuple

from orso.tools import random_string
from orso.types import OrsoTypes

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import format_expression
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.logical_planner import logical_planner_builders
from opteryx.third_party.travers import Graph


class LogicalPlanStepType(int, Enum):
    Project = auto()  # field selection
    Filter = auto()  # tuple filtering
    Union = auto()  #  appending relations
    Explain = auto()  # EXPLAIN
    Difference = auto()  # relation interection
    Join = auto()  # all joins
    #    Containment = auto() # IN (maybe also EXISTS?)
    AggregateAndGroup = auto()  # group by
    Aggregate = auto()
    Scan = auto()  # read a dataset
    Show = auto()  # show a variable
    ShowColumns = auto()  # SHOW COLUMNS
    Set = auto()  # set a variable
    Limit = auto()  # limit and offset
    Order = auto()  # order by
    Distinct = auto()
    Exit = auto()
    Defragment = auto()  # TODO: not currently used
    HeapSort = auto()

    CTE = auto()
    Subquery = auto()
    FunctionDataset = auto()  # Unnest, GenerateSeries, values + Fake
    MetadataWriter = auto()


class LogicalPlan(Graph):
    pass


class LogicalPlanNode(Node):
    def copy(self) -> "Node":
        return LogicalPlanNode(**super().copy().properties)

    def __str__(self):
        try:
            # fmt:off
            node_type = self.node_type
            if node_type == LogicalPlanStepType.AggregateAndGroup:
                return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)}) GROUP BY ({', '.join(format_expression(col) for col in self.groups)})"
            if node_type == LogicalPlanStepType.Aggregate:
                return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)})"
            if node_type == LogicalPlanStepType.Distinct:
                distinct_on = ""
                if self.on is not None:
                    distinct_on = f" ON ({','.join(format_expression(col) for col in self.on)})"
                return f"DISTINCT{distinct_on}"
            if node_type == LogicalPlanStepType.Explain:
                return f"EXPLAIN{' ANALYZE' if self.analyze else ''}{(' (' + self.format + ')') if self.format else ''}"
            if node_type == LogicalPlanStepType.FunctionDataset:
                if self.function == "FAKE":
                    return f"FAKE ({', '.join(format_expression(arg) for arg in self.args)}{' AS ' + self.alias if self.alias else ''})"
                if self.function == "GENERATE_SERIES":
                    return f"GENERATE SERIES ({', '.join(format_expression(arg) for arg in self.args)}){' AS ' + self.alias if self.alias else ''}"
                if self.function == "VALUES":
                    return f"VALUES (({', '.join(self.columns)}) x {len(self.values)} AS {self.alias})"
                if self.function == "UNNEST":
                    return f"UNNEST ({', '.join(format_expression(arg) for arg in self.args)}{' AS ' + self.unnest_target if self.unnest_target else ''})"
            if node_type == LogicalPlanStepType.Filter:
                return f"FILTER ({format_expression(self.condition)})"
            if node_type == LogicalPlanStepType.Join:
                filters = ""
                if self.filters:
                    filters = f"({self.unnest_alias} IN ({', '.join(self.filters)}))"
                if self.on:
                    return f"{self.type.upper()} JOIN ({format_expression(self.on, True)}){filters}"
                if self.using:
                    return f"{self.type.upper()} JOIN (USING {','.join(map(format_expression, self.using))}){filters}"
                return f"{self.type.upper()} {filters}"
            if node_type == LogicalPlanStepType.HeapSort:
                return f"HEAP SORT (LIMIT {self.limit}, ORDER BY {', '.join(format_expression(item[0]) + (' DESC' if item[1] =='descending' else '') for item in self.order_by)})"
            if node_type == LogicalPlanStepType.Limit:
                limit_str = f"LIMIT ({self.limit})" if self.limit is not None else ""
                offset_str = f" OFFSET ({self.offset})" if self.offset is not None else ""
                return (limit_str + offset_str).strip()
            if node_type == LogicalPlanStepType.Order:
                return f"ORDER BY ({', '.join(format_expression(item[0]) + (' DESC' if item[1] =='descending' else '') for item in self.order_by)})"
            if node_type == LogicalPlanStepType.Project:
                order_by_indicator = " +" if self.order_by_columns else ""
                return f"PROJECT ({', '.join(format_expression(col) for col in self.columns)}){order_by_indicator}"
            if node_type == LogicalPlanStepType.Scan:
                date_range = ""
                if self.start_date == self.end_date and self.start_date is not None:
                    date_range = f" FOR '{self.start_date}'"
                elif self.start_date is not None:
                    date_range = f" FOR '{self.start_date}' TO '{self.end_date}'"
                alias = ""
                if self.relation != self.alias:
                    alias = f" AS {self.alias}"
                columns = ""
                if self.columns:
                    columns = " [" + ", ".join(c.name for c in self.columns) + "]"
                predicates = ""
                if self.predicates:
                    predicates = " (" + " AND ".join(map(format_expression, self.predicates)) + ")"
                return f"SCAN ({self.relation}{alias}{date_range}{' WITH(' + ','.join(self.hints) + ')' if self.hints else ''}){columns}{predicates}"
            if node_type == LogicalPlanStepType.Set:
                return f"SET ({self.variable} TO {self.value.value})"
            if node_type == LogicalPlanStepType.Show:
                return f"SHOW ({', '.join(self.items)})"
            if node_type == LogicalPlanStepType.ShowColumns:
                return f"SHOW{' FULL' if self.full else ''}{' EXTENDED' if self.extended else ''} COLUMNS ({self.relation})"
            if node_type == LogicalPlanStepType.Subquery:
                return f"SUBQUERY{' AS ' + self.alias if self.alias else ''}"
            if node_type == LogicalPlanStepType.Union:
                columns = ""
                if self.columns:
                    columns = " [" + ", ".join(c.qualified_name for c in self.columns) + "]"
                return f"UNION {'' if self.modifier is None else self.modifier.upper()}{columns}"

            # fmt:on
        except Exception as err:
            import warnings

            warnings.warn(f"Problem drawing logical plan - {err}")
        return f"{str(self.node_type)[20:].upper()}"


def get_subplan_schemas(sub_plan: Graph) -> List[str]:
    """
    Retrieves schemas related to exit and entry points within a given sub-plan.

    This function iterates through functions named 'get_exit_points' and 'get_entry_points'
    in the `sub_plan` object to collect the schemas of the exit and entry points.

    Parameters:
        sub_plan: Graph
            The sub-plan object containing the necessary information for processing.

    Returns:
        List[str]:
            A list of schemas corresponding to the exit and entry points in the sub-plan.
    """
    aliases = set()

    def traverse(nid: dict):
        # get the actual node
        node = sub_plan[nid["name"]]
        # If this node is a subquery, capture its alias and stop traversing deeper
        if node.node_type == LogicalPlanStepType.Subquery:
            aliases.add(node.alias)
            return  # Stop traversing this branch as it's a separate scope
        if node.alias:
            aliases.add(node.alias)

        # Otherwise, continue traversing the children
        for child in nid.get("children", []):
            traverse(child)

    # Start the traversal from the provided node
    traverse(sub_plan.depth_first_search())

    return list(aliases)


"""
CLAUSE PLANNERS
"""


def extract_ctes(branch, planner):
    ctes = {}
    if branch.get("Query", branch).get("with"):
        for _ast in branch.get("Query", branch)["with"]["cte_tables"]:
            alias = _ast.get("alias")["name"]["value"]
            ctes[alias] = planner(_ast["query"]["body"])
    return ctes


def extract_value(clause):
    if len(clause) == 1:
        return logical_planner_builders.build(clause[0])
    return [logical_planner_builders.build(token) for token in clause]


def extract_variable(clause):
    if len(clause) == 1:
        return clause[0]["value"]
    return [token["value"] for token in clause]


def extract_simple_filter(filters, identifier: str = "Name"):
    if "Like" in filters:
        left = Node(NodeType.IDENTIFIER, value=identifier)
        right = Node(NodeType.LITERAL, type=OrsoTypes.VARCHAR, value=filters["Like"])
        root = Node(
            NodeType.COMPARISON_OPERATOR,
            value="ILike",  # we're case insensitive for SHOW filters
            left=left,
            right=right,
        )
        return root
    if "Where" in filters:
        root = logical_planner_builders.build(filters["Where"])
        return root


def _table_name(branch):
    keys = ("Table", "Derived")
    for key in keys:
        if key in branch["relation"]:
            break
    if branch["relation"][key]["alias"]:
        return branch["relation"][key]["alias"]["name"]["value"]
    return ".".join(part["value"] for part in branch["relation"][key]["name"])


def inner_query_planner(ast_branch):
    if "Query" in ast_branch:
        # Sometimes we get a full query plan here (e.g. when queries in set
        # functions are in parenthesis)
        return plan_query(ast_branch)

    inner_plan = LogicalPlan()
    step_id = None

    # from
    _relations = ast_branch["Select"].get("from", [])
    for relation in _relations:
        step_id, sub_plan = create_node_relation(relation)
        inner_plan += sub_plan

    # If there's any peer relations, they are implicit cross joins
    if len(_relations) > 2:
        raise UnsupportedSyntaxError("Cannot CROSS JOIN more than two relations.")
    if len(_relations) > 1:
        join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
        join_step.type = "cross join"

        join_step.right_relation_names = [_table_name(_relations[0])]
        join_step.left_relation_names = [_table_name(_relations[1])]

        step_id = random_string()
        inner_plan.add_node(step_id, join_step)
        for relation in _relations:
            inner_plan.add_edge(relation["step_id"], step_id)

    # If there's no relations, use $no_table
    if len(_relations) == 0:
        step_id, sub_plan = create_node_relation(
            {
                "relation": {
                    "Table": {
                        "name": [{"value": "$no_table"}],
                        "args": None,
                        "alias": None,
                        "with_hints": [],
                    }
                }
            }
        )
        inner_plan += sub_plan

    # selection
    _selection = logical_planner_builders.build(ast_branch["Select"].get("selection"))
    if _selection:
        if len(_relations) == 0:
            raise UnsupportedSyntaxError("Statement has a WHERE clause but no FROM clause.")
        all_ops = get_all_nodes_of_type(_selection, (NodeType.COMPARISON_OPERATOR,))
        if any(op.value in ("Arrow", "LongArrow") for op in all_ops):
            raise UnsupportedSyntaxError("JSON Accessors (->, ->>) cannot be used in filters.")
        selection_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        selection_step.condition = _selection
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, selection_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # groups
    _projection = logical_planner_builders.build(ast_branch["Select"].get("projection")) or []
    if len(_projection) > 1 and any(p.node_type == NodeType.WILDCARD for p in _projection):
        from opteryx.exceptions import SqlError

        raise SqlError("SELECT * cannot coexist with additional columns.")
    _aggregates = get_all_nodes_of_type(_projection, select_nodes=(NodeType.AGGREGATOR,))
    _groups = logical_planner_builders.build(ast_branch["Select"].get("group_by"))
    if _groups is not None and _groups != []:
        if any(p.node_type == NodeType.WILDCARD for p in _projection):
            raise UnsupportedSyntaxError(
                "SELECT * cannot be used with GROUP BY, fields in the SELECT must be aggregates or in the GROUP BY clause."
            )

        group_step = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
        group_step.groups = _groups
        group_step.aggregates = _aggregates
        group_step.projection = _projection
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, group_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)
    # aggregates
    elif len(_aggregates) > 0:
        aggregate_step = LogicalPlanNode(node_type=LogicalPlanStepType.Aggregate)
        aggregate_step.groups = _groups
        aggregate_step.aggregates = _aggregates

        known_columns = set(get_all_nodes_of_type(_groups + _aggregates, (NodeType.IDENTIFIER,)))
        project_columns = set(get_all_nodes_of_type(_projection, (NodeType.IDENTIFIER,)))

        if project_columns - known_columns:
            from opteryx.exceptions import SqlError

            column = (project_columns - known_columns).pop().source_column
            error = f"Column '{column}' must appear in the `GROUP BY` clause or must be part of an aggregate function. Either add it to the `GROUP BY` list, or add an aggregation such as `MIN({column})`."
            raise SqlError(error)

        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, aggregate_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # pre-process part of the order by before the projection
    _order_by = ast_branch.get("order_by")
    _order_by_columns_not_in_projection = []
    _order_by_columns = []
    if _order_by:
        _order_by = [
            (
                logical_planner_builders.build(item["expr"]),
                True if item["asc"] is None else item["asc"],
            )
            for item in _order_by
        ]
        if any(c[0].node_type == NodeType.LITERAL for c in _order_by):
            raise UnsupportedSyntaxError("Cannot ORDER BY constant values")
        _order_by_columns = [exp[0] for exp in _order_by]

    # projection
    if not (
        len(_projection) == 1
        and _projection[0].node_type == NodeType.WILDCARD
        and _projection[0].value is None
    ):
        for column in _projection:
            if column.node_type == NodeType.LITERAL and column.type == OrsoTypes.ARRAY:
                if ast_branch["Select"].get("distinct"):
                    raise UnsupportedSyntaxError(
                        "Values cannot be parenthesised in the SELECT clause. Did you mean DISTINCT ON(cols) cols FROM ?"
                    )
                raise UnsupportedSyntaxError("Values cannot be parenthesised in the SELECT clause.")

        # ORDER BY needing to be able to order by columns not in the projection
        # whilst being able to order by aliases created by the projection means
        # we need to do specific checks
        if _order_by_columns:
            # Collect qualified names and aliases from projection columns
            projection_qualified_names = {
                proj_col.qualified_name for proj_col in _projection if proj_col.qualified_name
            }.union({f".{proj_col.alias}" for proj_col in _projection if proj_col.alias})

            # Collect expression columns from projection
            projection_expressions = {
                format_expression(proj_col)
                for proj_col in _projection
                if proj_col.node_type != NodeType.IDENTIFIER
            }

            # Remove columns from ORDER BY that are directly in the projection, aliased, or have the same expression
            _order_by_columns_not_in_projection = [
                ord_col
                for ord_col in _order_by_columns
                if ord_col.qualified_name not in projection_qualified_names
                and f".{ord_col.source_column}" not in projection_qualified_names
                and ord_col.qualified_name
                not in [f".{proj_col.source_column}" for proj_col in _projection]
                and format_expression(ord_col) not in projection_expressions
            ]

            # Remove columns from ORDER BY that match the source of a wildcard in the projection
            for proj_col in [pc for pc in _projection if pc.node_type == NodeType.WILDCARD]:
                _order_by_columns_not_in_projection = [
                    ord_col
                    for ord_col in _order_by_columns_not_in_projection
                    if ord_col.source != proj_col.value[0]
                ]

        project_step = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
        project_step.columns = _projection
        project_step.order_by_columns = _order_by_columns_not_in_projection
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, project_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)
    else:
        _order_by_columns_not_in_projection = []

    # having
    _having = logical_planner_builders.build(ast_branch["Select"].get("having"))
    if _having:
        having_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        having_step.condition = _having
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, having_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # distinct
    if ast_branch["Select"].get("distinct"):
        distinct_step = LogicalPlanNode(node_type=LogicalPlanStepType.Distinct)
        if isinstance(ast_branch["Select"]["distinct"], dict):
            distinct_step.on = logical_planner_builders.build(
                ast_branch["Select"]["distinct"]["On"]
            )
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, distinct_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

        if distinct_step.on and _projection[0].source_column == "FROM":
            cols = ", ".join([format_expression(c) for c in distinct_step.on])
            raise UnsupportedSyntaxError(
                f"Did you mean 'SELECT DISTINCT ON ({cols}) {cols} FROM {_projection[0].alias};'?"
            )

    # order
    if _order_by:
        order_step = LogicalPlanNode(node_type=LogicalPlanStepType.Order)
        order_step.order_by = _order_by
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, order_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # limit/offset
    _limit = ast_branch.get("limit")
    _offset = ast_branch.get("offset")
    if _limit or _offset:
        limit_step = LogicalPlanNode(node_type=LogicalPlanStepType.Limit)
        limit_step.limit = None if _limit is None else logical_planner_builders.build(_limit).value
        limit_step.offset = (
            None if _offset is None else logical_planner_builders.build(_offset).value
        )
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, limit_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # add the exit node
    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    exit_node.columns = _projection
    previous_step_id, step_id = step_id, random_string()
    inner_plan.add_node(step_id, exit_node)
    if previous_step_id is not None:
        inner_plan.add_edge(previous_step_id, step_id)

    return inner_plan


"""
STATEMENT PLANNERS
"""


def process_join_tree(join: dict) -> LogicalPlanNode:
    """
    Processes a join tree from the AST and returns a LogicalPlanNode representing the join.
    """

    def extract_join_type(join: dict) -> str:
        """
        Extracts the type of the join from the AST node representing the join.
        """
        join_operator = join["join_operator"]

        if join_operator == {"Inner": "Natural"}:
            return "natural join"
        elif join_operator == "CrossJoin":
            return "cross join"

        join_operator = next(iter(join["join_operator"]))

        return {
            "FullOuter": "full outer",
            "Inner": "inner",
            "LeftAnti": "left anti",
            "LeftOuter": "left outer",
            "LeftSemi": "left semi",
            "RightAnti": "right anti",
            "RightOuter": "right outer",
            "RightSemi": "right semi",
            "CrossJoin": "cross join",  # should never match, here for completeness
            "Natural": "natural join",  # should never match, here for completeness
        }.get(join_operator)

    def extract_join_condition(join: dict) -> Tuple[Optional[str], Optional[List[str]]]:
        """
        Extracts the join's limiting condition from the AST node representing the join.
        """
        join_operator = join["join_operator"]
        if not isinstance(join_operator, dict):
            return None, None

        join_on = None
        join_using = None

        join_operator = next(iter(join_operator))
        join_condition = next(iter(join["join_operator"][join_operator]))
        if join_condition == "On":
            join_on = logical_planner_builders.build(
                join["join_operator"][join_operator][join_condition]
            )
        if join_condition == "Using":
            join_using = [
                logical_planner_builders.build({"Identifier": identifier})
                for identifier in join["join_operator"][join_operator][join_condition]
            ]

        return join_on, join_using

    def extract_unnest_dataset(join: dict, join_type: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extracts information for an UNNEST dataset from the AST node representing the join.
        """
        unnest_column = None
        unnest_alias = None
        if (
            join["relation"].get("Table", {}).get("name", [{}])[0].get("value", "").upper()
            == "UNNEST"
        ):
            if not join_type in ("cross join", "inner"):
                raise UnsupportedSyntaxError(
                    "JOIN on UNNEST only supported for CROSS and INNER joins."
                )
            unnest_column = logical_planner_builders.build(join["relation"]["Table"]["args"][0])
            unnest_alias = join["relation"]["Table"]["alias"]["name"]["value"]

        return unnest_column, unnest_alias

    join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join)

    join_step.type = extract_join_type(join)
    join_step.on, join_step.using = extract_join_condition(join)
    join_step.unnest_column, join_step.unnest_alias = extract_unnest_dataset(join, join_step.type)

    return join_step


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
                if subquery["alias"] is None:
                    from opteryx.exceptions import UnnamedSubqueryError

                    raise UnnamedSubqueryError(
                        "Subqueries in FROM or JOIN clauses must be named (AS)."
                    )

                subquery_step = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
                subquery_step.alias = (
                    None if subquery["alias"] is None else subquery["alias"]["name"]["value"]
                )
                step_id = random_string()
                sub_plan.add_node(step_id, subquery_step)

                subquery_plan = plan_query(subquery["subquery"])
                exit_node = subquery_plan.get_exit_points()[0]
                subquery_step.columns = subquery_plan[exit_node].columns
                subquery_plan.remove_node(exit_node, heal=True)

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
                values_step = LogicalPlanNode(
                    node_type=LogicalPlanStepType.FunctionDataset, function="VALUES"
                )
                values_step.alias = subquery["alias"]["name"]["value"]
                values_step.columns = tuple(col["value"] for col in subquery["alias"]["columns"])
                values_step.values = [
                    tuple(logical_planner_builders.build(value) for value in row)
                    for row in subquery["subquery"]["body"]["Values"]["rows"]
                ]
                step_id = random_string()
                sub_plan.add_node(step_id, values_step)
                root_node = step_id
        else:  # pragma: no cover
            raise NotImplementedError(relation["relation"]["Derived"])
    elif relation["relation"]["Table"]["args"]:
        function = relation["relation"]["Table"]
        function_name = function["name"][0]["value"].upper()

        if function["alias"] is None:
            from opteryx.exceptions import UnnamedColumnError

            raise UnnamedColumnError(
                f"Column created by {function_name} has no name, use AS to name the column."
            )

        function_step = LogicalPlanNode(
            node_type=LogicalPlanStepType.FunctionDataset, function=function_name
        )
        if function_name == "UNNEST":
            function_step.alias = f"$unnest-{random_string(4)}"
            function_step.unnest_target = function["alias"]["name"]["value"]
        else:
            function_step.alias = function["alias"]["name"]["value"]

        function_step.args = [logical_planner_builders.build(arg) for arg in function["args"]]
        function_step.columns = tuple(col["value"] for col in function["alias"]["columns"])

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
        join_step = process_join_tree(join)

        left_node_id, left_plan = create_node_relation(join)

        # add the left and right relation names - we sometimes need these later
        join_step.right_relation_names = get_subplan_schemas(sub_plan)
        join_step.left_relation_names = get_subplan_schemas(left_plan)

        # add the other side of the join
        sub_plan += left_plan

        join_step_id = random_string()
        sub_plan.add_node(join_step_id, join_step)

        # add the from table as the left side of the join
        sub_plan.add_edge(root_node, join_step_id, "left")
        sub_plan.add_edge(left_node_id, join_step_id, "right")

        root_node = join_step_id

    return root_node, sub_plan


def analyze_query(statement) -> LogicalPlan:

    root_node = "Analyze"
    plan = LogicalPlan()

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    table = statement[root_node]["table_name"]
    from_step.relation = ".".join(part["value"] for part in table)
    from_step.alias = from_step.relation
    from_step.start_date = table[0].get("start_date")
    from_step.end_date = table[0].get("end_date")
    step_id = random_string()
    plan.add_node(step_id, from_step)

    metadata_step = LogicalPlanNode(node_type=LogicalPlanStepType.MetadataWriter)
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, metadata_step)
    plan.add_edge(previous_step_id, step_id)

    return plan
    # write manifest


def plan_execute_query(statement) -> LogicalPlan:

    import orjson

    from opteryx.exceptions import SqlError
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide
    from opteryx.utils import sql

    def build_parm(node):
        if "BinaryOp" in node:
            op = node["BinaryOp"]
            if "op" in op and op["op"] == "Eq":
                return (
                    logical_planner_builders.build(op["left"]).value,
                    logical_planner_builders.build(op["right"]).value,
                )
        from opteryx.exceptions import ParameterError

        raise ParameterError("EXECUTE paramters must be named, e.g. 'paramter=value'")

    # the parser allows USING, but we want EXECUTE function (parmeters)
    if statement["Execute"].get("using"):
        raise UnsupportedSyntaxError(
            "EXECUTE does not support USING syntax, please provide parameters in parenthesis."
        )

    statement_name = statement["Execute"]["name"]["value"]
    parameters = dict(build_parm(p) for p in statement["Execute"]["parameters"])
    try:
        with open("prepared_statements.json", "r") as ps:
            prepared_statatements = orjson.loads(ps.read())
    except OSError:
        prepared_statatements = {}

    # we have an inbuilt statements as a fallback
    prepared_statatements["PLANETS_BY_ID"] = {
        "statement": "SELECT * FROM $planets WHERE id = :id",
        "parameters": [{"id": "name", "type": "INTEGER"}],
    }
    prepared_statatements["VERSION"] = {"statement": "SELECT version()", "parameters": []}
    if statement_name not in prepared_statatements:
        raise SqlError(f"Unable to EXECUTE prepared statement, '{statement_name}' not defined.")
    operation = prepared_statatements[statement_name]["statement"]

    operation = sql.remove_comments(operation)
    operation = sql.clean_statement(operation)
    statements = sql.split_sql_statements(operation)
    if len(statements) > 1:
        raise UnsupportedSyntaxError("EXECUTE cannot run multi-part and batch prepared statements.")

    clean_sql, temporal_filters = do_sql_rewrite(operation)
    try:
        parsed_statements = sqloxide.parse_sql(clean_sql, dialect="mysql")
    except ValueError as parser_error:
        raise SqlError(parser_error) from parser_error

    parsed_statements = do_ast_rewriter(
        parsed_statements,
        temporal_filters=temporal_filters,
        parameters=parameters,
        connection=None,
    )
    return list(do_logical_planning_phase(parsed_statements))[0][0]


def plan_explain(statement) -> LogicalPlan:
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

    root_node = statement
    if "Query" in root_node:
        root_node = root_node["Query"]

    # union?
    if "SetOperation" in root_node["body"]:
        set_operation = root_node["body"]["SetOperation"]

        if set_operation["op"] == "Union":
            set_op_node = LogicalPlanNode(node_type=LogicalPlanStepType.Union)
        else:
            raise UnsupportedSyntaxError(f"Unsupported SET operator '{set_operation['op']}'")

        set_op_node.modifier = (
            None if set_operation["set_quantifier"] == "None" else set_operation["set_quantifier"]
        )
        step_id = random_string()
        plan = LogicalPlan()
        plan.add_node(step_id, set_op_node)
        head_nid = step_id

        left_plan = inner_query_planner(set_operation["left"])
        plan += left_plan
        subquery_entry_id = left_plan.get_exit_points()[0]
        plan.add_edge(subquery_entry_id, step_id)
        # remove the exit node
        plan.remove_node(subquery_entry_id, heal=True)

        right_plan = inner_query_planner(set_operation["right"])
        plan += right_plan
        subquery_entry_id = right_plan.get_exit_points()[0]
        plan.add_edge(subquery_entry_id, step_id)
        # remove the exit node
        plan.remove_node(subquery_entry_id, heal=True)

        # UNION ALL
        if set_op_node.modifier != "All":
            distinct = LogicalPlanNode(node_type=LogicalPlanStepType.Distinct)
            head_nid, step_id = step_id, random_string()
            plan.add_node(step_id, distinct)
            plan.add_edge(head_nid, step_id)

        # limit/offset
        _limit = root_node.get("limit")
        _offset = root_node.get("offset")
        if _offset:
            _offset = _offset.get("value")
        if _limit or _offset:
            limit_step = LogicalPlanNode(node_type=LogicalPlanStepType.Limit)
            limit_step.limit = (
                None if _limit is None else logical_planner_builders.build(_limit).value
            )
            limit_step.offset = (
                None if _offset is None else logical_planner_builders.build(_offset).value
            )
            head_nid, step_id = step_id, random_string()
            plan.add_node(step_id, limit_step)
            if head_nid is not None:
                plan.add_edge(head_nid, step_id)

        # add the exit node
        exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
        _projection_nodes = [
            left_plan[nid]
            for nid in left_plan.nodes()
            if left_plan[nid].node_type in (LogicalPlanStepType.Project,)
        ]
        columns = [LogicalPlanNode(NodeType.WILDCARD, value=(None,))]
        if _projection_nodes:
            columns = _projection_nodes[0].columns
        exit_node.columns = columns
        head_nid, step_id = step_id, random_string()
        plan.add_node(step_id, exit_node)
        if head_nid is not None:
            plan.add_edge(head_nid, step_id)

        set_op_node.columns = columns
        set_op_node.left_relation_names = get_subplan_schemas(left_plan)
        set_op_node.right_relation_names = get_subplan_schemas(right_plan)

        return plan

    # we do some minor AST rewriting
    root_node["body"]["limit"] = root_node.get("limit", None)
    root_node["body"]["offset"] = (root_node.get("offset") or {}).get("value")
    root_node["body"]["order_by"] = root_node.get("order_by", None)

    planned_query = inner_query_planner(root_node["body"])

    # DEBUG: log ("LOGICAL PLAN")
    # DEBUG: log (planned_query.draw())

    return planned_query


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

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    table = statement[root_node]["table_name"]
    from_step.relation = ".".join(part["value"] for part in table)
    from_step.alias = from_step.relation
    from_step.start_date = table[0].get("start_date")
    from_step.end_date = table[0].get("end_date")
    step_id = random_string()
    plan.add_node(step_id, from_step)

    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.ShowColumns)
    show_step.extended = statement[root_node]["extended"]
    show_step.full = statement[root_node]["full"]
    show_step.relation = from_step.relation
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, show_step)
    plan.add_edge(previous_step_id, step_id)

    _filter = statement[root_node]["filter"]
    if _filter:
        filter_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        filter_node.condition = extract_simple_filter(_filter, "name")
        previous_step_id, step_id = step_id, random_string()
        plan.add_node(step_id, filter_node)
        plan.add_edge(previous_step_id, step_id)
        raise UnsupportedSyntaxError("Unable to filter colmns in SHOW COLUMNS")

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

    read_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    read_step.relation = "$variables"
    step_id = random_string()
    plan.add_node(step_id, read_step)

    predicate = statement[root_node]["filter"]
    if predicate is not None:
        operator = next(iter(predicate))
        select_step = LogicalPlanNode(
            node_type=LogicalPlanStepType.Filter,
            condition=Node(
                node_type=NodeType.COMPARISON_OPERATOR,
                value=operator,
                left=Node(node_type=NodeType.IDENTIFIER, value="name"),
                right=Node(
                    node_type=NodeType.LITERAL, type=OrsoTypes.VARCHAR, value=predicate[operator]
                ),
            ),
        )
        previous_step_id, step_id = step_id, random_string()
        plan.add_node(step_id, select_step)
        plan.add_edge(previous_step_id, step_id)
        raise UnsupportedSyntaxError("Cannot filter by Variable Names")

    exit_step = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    exit_step.columns = [Node(node_type=NodeType.WILDCARD)]  # We are always SELECT *
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, exit_step)
    plan.add_edge(previous_step_id, step_id)

    return plan


QUERY_BUILDERS = {
    "Analyze": analyze_query,
    "Execute": plan_execute_query,
    "Explain": plan_explain,
    "Query": plan_query,
    "SetVariable": plan_set_variable,
    "ShowColumns": plan_show_columns,
    #    "ShowCreate": show_create_query,
    #    "ShowFunctions": show_functions_query,
    "ShowVariable": plan_show_variable,  # generic SHOW handler
    "ShowVariables": plan_show_variables,
    # "Use": plan_use
}


def do_logical_planning_phase(parsed_statements) -> Generator:
    # The sqlparser ast is an array of asts
    for parsed_statement in parsed_statements:
        statement_type = next(iter(parsed_statement))
        if not statement_type in QUERY_BUILDERS:
            from opteryx.exceptions import UnsupportedSyntaxError

            raise UnsupportedSyntaxError(
                f"Version 2 Planner does not support '{statement_type}' type queries yet."
            )
        # CTEs are Common Table Expressions, they're variations of subqueries
        ctes = extract_ctes(parsed_statement, inner_query_planner)
        yield QUERY_BUILDERS[statement_type](parsed_statement), parsed_statement, ctes
