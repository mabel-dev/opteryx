# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Converts the AST to a logical query plan.

The plan does not try to be efficient or clever, at this point it is only trying to be correct.
"""

from enum import Enum
from enum import auto
from typing import List
from typing import Optional
from typing import Tuple

from orso.tools import random_string
from orso.types import OrsoTypes

from opteryx.exceptions import UnnamedColumnError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import format_expression
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import logical_planner_builders
from opteryx.planner.logical_planner.logical_planner_rewriter import decompose_aggregates
from opteryx.third_party.travers import Graph


class LogicalPlanStepType(int, Enum):
    Project = auto()  # field selection
    Filter = auto()  # tuple filtering
    Union = auto()  #  appending relations
    Explain = auto()  # EXPLAIN
    Difference = auto()  # relation interection
    Join = auto()  # all joins
    Unnest = auto()  # UNNEST
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

    def __str__(self):  # pragma: no cover
        try:
            from opteryx.planner.logical_planner.logical_planner_renderers import _render_registry

            render_fn = _render_registry.get(self.node_type)
            if render_fn:
                return render_fn(self)
        except Exception as err:
            import warnings

            warnings.warn(f"Problem drawing logical plan - {err}")
        return self.node_type.name


def get_subplan_schemas(sub_plan: Graph) -> List[str]:
    """
    Collects all schema aliases used within a given sub-plan.

    This function traverses the sub-plan graph to collect aliases, including those from subqueries.
    Aliases define the schemas used at exit and entry points of the sub-plan.

    Parameters:
        sub_plan: Graph
            The sub-plan object representing a branch of the logical plan.

    Returns:
        List[str]:
            A sorted list of unique schema aliases found within the sub-plan.
    """

    def collect_aliases(node: dict) -> List[str]:
        """
        Recursively traverse the graph to collect schema aliases.

        Parameters:
            node: dict
                The current node in the graph.

        Returns:
            List[str]:
                A list of unique schema aliases collected from the current node and its children.
        """
        current_node = sub_plan[node["name"]]

        # Start with the alias of the current node, if it exists
        aliases = [current_node.alias] if current_node.alias else []

        # If this node is a subquery, stop traversal here
        if current_node.node_type == LogicalPlanStepType.Subquery:
            return aliases

        # Recursively collect aliases from children
        for child in node.get("children", []):
            aliases.extend(collect_aliases(child))

        return aliases

    # Start the traversal from the root node
    root_node = sub_plan.depth_first_search()
    aliases = collect_aliases(root_node)

    # Return sorted list of unique aliases
    return sorted(set(aliases))


def get_subplan_reads(sub_plan: Graph) -> List[str]:
    def collect_reads(node: dict) -> List[str]:
        current_node = sub_plan[node["name"]]

        # If this node is a subquery, stop traversal here
        if current_node.node_type in (
            LogicalPlanStepType.Scan,
            LogicalPlanStepType.FunctionDataset,
        ):
            return [current_node.uuid]

        readers = []
        # Recursively collect aliases from children
        for child in node.get("children", []):
            readers.extend(collect_reads(child))

        return readers

    # Start the traversal from the root node
    root_node = sub_plan.depth_first_search()
    readers = collect_reads(root_node)

    # Return sorted list of unique aliases
    return sorted(set(readers))


"""
CLAUSE PLANNERS
"""


def extract_ctes(branch, planner):
    ctes = {}
    if branch.get("Query", branch).get("with"):
        for _ast in branch.get("Query", branch)["with"]["cte_tables"]:
            alias = _ast.get("alias")["name"]["value"]
            logical_plan = planner(_ast["query"]["body"])
            # CTEs don't have an exit node
            plan_head = logical_plan.get_exit_points()[0]
            logical_plan.remove_node(plan_head, True)
            ctes[alias] = logical_plan
    return ctes


def extract_value(clause):
    if len(clause) == 1:
        return logical_planner_builders.build(clause[0])
    return [logical_planner_builders.build(token) for token in clause]


def extract_variable(clause):
    if len(clause) == 1:
        return clause[0]["Identifier"]["value"]
    return [token["Identifier"]["value"] for token in clause]


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
    return ".".join(part["Identifier"]["value"] for part in branch["relation"][key]["name"])


def inner_query_planner(ast_branch: dict) -> LogicalPlan:
    if "Query" in ast_branch:
        # Sometimes we get a full query plan here (e.g. when queries in set
        # functions are in parenthesis)
        return plan_query(ast_branch)

    inner_plan = LogicalPlan()
    step_id = None

    # TOP used?
    if ast_branch["Select"].get("top") is not None:
        raise UnsupportedSyntaxError(
            "SELECT TOP to limit number of returned records not supported, use LIMIT instead."
        )

    # from
    _relations = ast_branch["Select"].get("from", [])
    for relation in _relations:
        step_id, sub_plan = create_node_relation(relation)
        inner_plan += sub_plan

    # If there's any peer relations, they are implicit cross joins
    if len(_relations) > 1:
        join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
        join_step.type = "cross join"
        join_step.implied_join = True

        join_step.relation_names = [_table_name(_relation) for _relation in _relations]

        reader_nodes = list(inner_plan._nodes.values())
        join_step.readers = [r.uuid for r in reader_nodes]

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
                        "name": [{"Identifier": {"value": "$no_table"}}],
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
        selection_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        selection_step.condition = _selection
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, selection_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # groups
    _projection = logical_planner_builders.build(ast_branch["Select"].get("projection")) or []
    if len(_projection) > 1 and any(
        p.node_type == NodeType.WILDCARD for p in _projection if p.value is None
    ):
        from opteryx.exceptions import SqlError

        raise SqlError("SELECT * cannot coexist with additional columns.")

    if len(_projection) > 1 and any(p.node_type == NodeType.WILDCARD for p in _projection[1:]):
        from opteryx.exceptions import SqlError

        raise SqlError(
            "Qualified wild cards (`table.*`) must be the first column when used with additional columns."
        )

    _aggregates = get_all_nodes_of_type(_projection, select_nodes=(NodeType.AGGREGATOR,))
    _aggregates, _projection = decompose_aggregates(_aggregates, _projection)
    _groups = logical_planner_builders.build(ast_branch["Select"].get("group_by"))[0]
    if _groups is not None and _groups != []:
        if any(p.node_type == NodeType.WILDCARD for p in _projection):
            raise UnsupportedSyntaxError(
                "SELECT * cannot be used with GROUP BY, Did you mean `GROUP BY ALL`."
            )
        # WILDCARD is used to represent GROUP BY ALL, we group by all columns in the projection
        # which aren't aggregates
        if _groups == NodeType.WILDCARD:
            _groups = [
                p
                for p in _projection
                if len(get_all_nodes_of_type(p, select_nodes=(NodeType.AGGREGATOR,))) == 0
            ]

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

        known_columns = {
            hash(n) for n in get_all_nodes_of_type(_groups + _aggregates, (NodeType.IDENTIFIER,))
        }
        project_columns = [
            n
            for n in get_all_nodes_of_type(_projection, (NodeType.IDENTIFIER,))
            if hash(n) not in known_columns
        ]

        if len(project_columns) > 0:
            from opteryx.exceptions import SqlError

            column = project_columns.pop().source_column
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
    if _order_by and _order_by.get("kind") and _order_by["kind"].get("Expressions"):
        _order_by = [
            (
                logical_planner_builders.build(item["expr"]),
                True if item["options"]["asc"] is None else item["options"]["asc"],
            )
            for item in _order_by["kind"]["Expressions"]
        ]
        if any(c[0].node_type == NodeType.LITERAL for c in _order_by):
            raise UnsupportedSyntaxError("Cannot ORDER BY constant values")
        _order_by_columns = [exp[0] for exp in _order_by]

    # projection
    if not (
        len(_projection) == 1
        and _projection[0].node_type == NodeType.WILDCARD
        and _projection[0].except_columns is None
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
        project_step.except_columns = _projection[0].except_columns
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

        if join_operator == {"Join": "Natural"}:
            return "natural join"
        elif join_operator == "CrossJoin":
            return "cross join"

        join_operator = next(iter(join["join_operator"]))

        return {
            "Anti": "left anti",  # ANTI JOIN is a LEFT ANTI JOIN
            "FullOuter": "full outer",
            "Join": "inner",
            "Inner": "inner",
            "LeftAnti": "left anti",
            "Left": "left outer",
            "LeftOuter": "left outer",
            "LeftSemi": "left semi",
            "RightAnti": "right anti",  # not supported
            "Right": "right outer",
            "RightOuter": "right outer",
            "RightSemi": "right semi",  # not supported
            "Semi": "left semi",  # SEMI JOIN is a LEFT SEMI JOIN
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
                logical_planner_builders.build(identifier[0])
                for identifier in join["join_operator"][join_operator][join_condition]
            ]

        return join_on, join_using

    def create_unnest_node(join: dict, join_step: Node) -> Node:
        """
        Extracts information for an UNNEST dataset from the AST node representing the join.
        """
        if join_step.type != "cross join":
            raise UnsupportedSyntaxError("JOIN on UNNEST only supported for CROSS joins.")
        unnest_column = logical_planner_builders.build(join["relation"]["Table"]["args"]["args"][0])
        if join["relation"]["Table"].get("alias") is None:
            raise UnnamedColumnError(
                "Column created by UNNEST has no name, use AS to name the column."
            )
        unnest_alias = join["relation"]["Table"]["alias"]["name"]["value"]

        # if we're a UNNEST JOIN, we're a different node type
        join_step.node_type = LogicalPlanStepType.Unnest
        join_step.unnest_column = unnest_column
        join_step.unnest_alias = unnest_alias
        join_step.alias = f"$unnest-{random_string(6)}"

        # return the updated node
        return join_step

    join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join)

    join_step.type = extract_join_type(join)

    if join_step.type in ("right semi", "right anti"):
        raise UnsupportedSyntaxError(
            f"{join_step.type.upper()} JOIN not supported, use LEFT variations only."
        )

    join_step.on, join_step.using = extract_join_condition(join)
    if not join_step.on and not join_step.using and join_step.type in ("left outer", "right outer"):
        raise UnsupportedSyntaxError(
            f"{join_step.type.upper()} JOIN must have an ON or USING clause."
        )

    # JOIN UNNEST needs to be handled differently
    if "Table" in join.get("relation", {}):
        relation_name = ".".join(
            logical_planner_builders.build(p).value for p in join["relation"]["Table"]["name"]
        )
        if relation_name.upper() == "UNNEST":
            join_step = create_unnest_node(join, join_step)

    return join_step


def create_node_relation(relation: dict):
    sub_plan = LogicalPlan()
    root_node = None

    relation_name = None
    if "Table" in relation["relation"]:
        relation_name = ".".join(
            logical_planner_builders.build(p).value for p in relation["relation"]["Table"]["name"]
        )

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
                        "Ensure you provide a name for all subqueries in FROM or JOIN clauses by using AS)."
                    )

                subquery_step = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
                subquery_step.alias = subquery["alias"]["name"]["value"]
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
                values_step.columns = tuple(
                    col["name"]["value"] for col in subquery["alias"]["columns"]
                )
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
        # If we have args, we're a function dataset (like FAKE or UNNEST)
        function = relation["relation"]["Table"]
        function_name = relation_name.upper()

        if function["alias"] is None:
            from opteryx.exceptions import UnnamedColumnError

            raise UnnamedColumnError(
                f"Column or Relation created by {function_name} has no name, use AS to give it a name."
            )

        function_step = LogicalPlanNode(
            node_type=LogicalPlanStepType.FunctionDataset, function=function_name
        )
        if function_name == "UNNEST":
            function_step.alias = f"$unnest-{random_string(6)}"
            function_step.relation = function_step.alias
            function_step.unnest_target = function["alias"]["name"]["value"]
        else:
            function_step.alias = function["alias"]["name"]["value"]

        function_step.args = [
            logical_planner_builders.build(arg) for arg in function["args"]["args"]
        ]
        function_step.columns = tuple(col["name"]["value"] for col in function["alias"]["columns"])

        step_id = random_string()
        sub_plan.add_node(step_id, function_step)
        root_node = step_id
        relation["step_id"] = step_id
    else:
        # SCAN nodes are where we read relations; these can be from memory, disk or a remote
        # system. This has many physical implementations but at this point all we have is the
        # name/location of the relation (relation), what the relation is called inside the
        # query (alias) and if there are any hints (hints)
        from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
        table = relation["relation"]["Table"]
        from_step.relation = relation_name
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
        # this is the convention: select * from LEFT join RIGHT

        join_step = process_join_tree(join)

        if join_step.node_type == LogicalPlanStepType.Unnest:
            # UNNEST joins don't have a LEFT and RIGHT side
            join_step_id = random_string()
            sub_plan.add_node(join_step_id, join_step)
            sub_plan.add_edge(root_node, join_step_id, "left")
            root_node = join_step_id
            continue

        right_node_id, right_plan = create_node_relation(join)

        # add the left and right relation names - we sometimes need these later
        join_step.left_relation_names = get_subplan_schemas(sub_plan)
        join_step.left_readers = get_subplan_reads(sub_plan)
        join_step.right_relation_names = get_subplan_schemas(right_plan)
        join_step.right_readers = get_subplan_reads(right_plan)

        # add the right side of the join
        sub_plan += right_plan

        join_step_id = random_string()
        sub_plan.add_node(join_step_id, join_step)

        # add the from table as the left side of the join
        sub_plan.add_edge(root_node, join_step_id, "left")
        sub_plan.add_edge(right_node_id, join_step_id, "right")

        root_node = join_step_id

    return root_node, sub_plan


def plan_execute_query(statement, **kwargs) -> LogicalPlan:
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

    statement_name = statement["Execute"]["name"][0]["Identifier"]["value"].upper()
    parameters = dict(build_parm(p) for p in statement["Execute"]["parameters"])
    try:
        with open("prepared_statements.json", "r") as ps:
            prepared_statatements = {str(k).upper(): v for k, v in orjson.loads(ps.read()).items()}
    except (OSError, ValueError):
        prepared_statatements = {}

    # we have an inbuilt statements as a fallback
    prepared_statatements["PLANETS_BY_ID"] = {
        "statement": "SELECT * FROM $planets WHERE id = :id",
        "parameters": [{"id": "name", "type": "INTEGER"}],
    }
    prepared_statatements["VERSION"] = {
        "statement": "SELECT version()",
        "parameters": [],
    }
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
        parsed_statements = sqloxide.parse_sql(clean_sql, _dialect="opteryx")
    except ValueError as parser_error:
        raise SqlError(parser_error) from parser_error

    parsed_statements = do_ast_rewriter(
        parsed_statements,
        temporal_filters=temporal_filters,
        parameters=parameters,
        connection=None,
    )
    return do_logical_planning_phase(parsed_statements[0])[0]


def plan_explain(statement, **kwargs) -> LogicalPlan:
    plan = LogicalPlan()
    explain_node = LogicalPlanNode(node_type=LogicalPlanStepType.Explain)
    explain_node.analyze = statement["Explain"]["analyze"]
    explain_node.format = statement["Explain"]["format"] or "TEXT"

    if explain_node.format == "GRAPHVIZ":
        explain_node.format = "MERMAID"

    explain_id = random_string()
    plan.add_node(explain_id, explain_node)

    sub_plan = plan_query(statement=statement["Explain"]["statement"])
    sub_plan_id = sub_plan.get_exit_points()[0]
    plan += sub_plan
    plan.add_edge(sub_plan_id, explain_id)

    return plan


def plan_query(statement: dict) -> LogicalPlan:
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

    # DEBUG: print("LOGICAL PLAN")
    # DEBUG: print(planned_query.draw())

    return planned_query


def plan_set_variable(statement, **kwargs):
    root_node = "SetVariable"
    plan = LogicalPlan()
    set_step = LogicalPlanNode(
        node_type=LogicalPlanStepType.Set,
        variable=extract_variable(statement[root_node]["variables"]["One"]),
        value=extract_value(statement[root_node]["value"]),
    )
    plan.add_node(random_string(), set_step)
    return plan


def plan_show_columns(statement, **kwargs):
    root_node = "ShowColumns"
    plan = LogicalPlan()

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    table = statement[root_node]["show_options"]["show_in"]["parent_name"]
    from_step.relation = ".".join(part["Identifier"]["value"] for part in table)
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

    _filter = statement[root_node]["show_options"].get("filter_position")
    if _filter:
        _filter = _filter["Suffix"]
        filter_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        filter_node.condition = extract_simple_filter(_filter, "name")
        previous_step_id, step_id = step_id, random_string()
        plan.add_node(step_id, filter_node)
        plan.add_edge(previous_step_id, step_id)
        raise UnsupportedSyntaxError("Unable to filter colmns in SHOW COLUMNS")

    return plan


def plan_show_create_query(statement, **kwargs):
    root_node = "ShowCreate"
    plan = LogicalPlan()
    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.Show)
    show_step.object_type = statement[root_node]["obj_type"].upper()
    show_step.object_name = extract_variable(statement[root_node]["obj_name"])
    if isinstance(show_step.object_name, list):
        show_step.object_name = ".".join(show_step.object_name)
    plan.add_node(random_string(), show_step)
    return plan


QUERY_BUILDERS = {
    # "Analyze": analyze_query,
    "Execute": plan_execute_query,
    "Explain": plan_explain,
    "Query": plan_query,
    "SetVariable": plan_set_variable,
    "ShowColumns": plan_show_columns,
    "ShowCreate": plan_show_create_query,
    # "ShowFunctions": show_functions_query,
    # "ShowVariable": plan_show_variable,  # generic SHOW handler
    # "ShowVariables": plan_show_variables,
    # "Use": plan_use
}


def apply_visibility_filters(
    logical_plan: LogicalPlan, visibility_filters: dict, statistics
) -> LogicalPlan:
    def build_expression_tree(relation, dnf_list):
        """
        Recursively build an expression tree from a DNF list structure.
        The DNF list consists of ORs of ANDs of simple predicates.
        """
        while isinstance(dnf_list, list) and len(dnf_list) == 1 and isinstance(dnf_list[0], list):
            # This means we a list with a single element, so we unpack it
            dnf_list = dnf_list[0]

        if isinstance(dnf_list[0], list):
            # This means we have a list of lists, so it's a disjunction (OR)
            or_node = None
            for conjunction in dnf_list:
                and_node = None
                for predicate in conjunction:
                    while isinstance(predicate, list):
                        # This means we a list with a single element, so we unpack it
                        predicate = predicate[0]

                    # Unpack the predicate (assume it comes as [identifier, operator, value])
                    identifier, operator, value = predicate
                    comparison_node = Node(
                        node_type=NodeType.COMPARISON_OPERATOR,
                        value=operator,
                        left=LogicalColumn(
                            NodeType.IDENTIFIER, source_column=identifier, source=relation
                        ),
                        right=build_literal_node(value),
                    )
                    if operator.startswith("AnyOp"):
                        comparison_node.left, comparison_node.right = (
                            comparison_node.right,
                            comparison_node.left,
                        )
                    if and_node is None:
                        and_node = comparison_node
                    else:
                        # Build a new AND node
                        and_node = Node(
                            node_type=NodeType.AND, left=and_node, right=comparison_node
                        )
                if or_node is None:
                    or_node = and_node
                else:
                    # Build a new OR node
                    or_node = Node(node_type=NodeType.OR, left=or_node, right=and_node)
            return or_node
        else:
            # Single conjunction (list of predicates)
            and_node = None
            for predicate in dnf_list:
                while isinstance(predicate, list):
                    # This means we a list with a single element, so we unpack it
                    predicate = predicate[0]

                identifier, operator, value = predicate
                # we have special handling for True and False literals in the place of identifiers
                if identifier is True or identifier is False:
                    left_node = build_literal_node(identifier)
                else:
                    left_node = LogicalColumn(
                        NodeType.IDENTIFIER, source_column=identifier, source=relation
                    )
                comparison_node = Node(
                    node_type=NodeType.COMPARISON_OPERATOR,
                    value=operator,
                    left=left_node,
                    right=build_literal_node(value),
                )
                if operator.startswith("AnyOp"):
                    comparison_node.left, comparison_node.right = (
                        comparison_node.right,
                        comparison_node.left,
                    )
                if and_node is None:
                    and_node = comparison_node
                else:
                    # Build a new AND node
                    and_node = Node(node_type=NodeType.AND, left=and_node, right=comparison_node)
            return and_node

    for nid, node in list(logical_plan.nodes(True)):
        if node.node_type == LogicalPlanStepType.Scan:
            filter_dnf = visibility_filters.get(node.relation)
            if filter_dnf == []:
                # TODO: This is a hack to make sure that an empty list of filters
                # means that the relation should not be visible
                expression_tree = Node(
                    node_type=NodeType.COMPARISON_OPERATOR,
                    value="Eq",
                    left=build_literal_node(True),
                    right=build_literal_node(False),
                )

                # If the filter is an empty list, it means that the relation should not be visible
                filter_node = LogicalPlanNode(
                    node_type=LogicalPlanStepType.Filter,
                    condition=expression_tree,  # Use the built expression tree
                    all_relations={node.relation, node.alias},
                )
                logical_plan.insert_node_after(random_string(), filter_node, nid)
                statistics.visibility_filters_blank_condition_added += 1
            if filter_dnf:
                # Apply the transformation from DNF to an expression tree
                expression_tree = build_expression_tree(node.alias, filter_dnf)

                filter_node = LogicalPlanNode(
                    node_type=LogicalPlanStepType.Filter,
                    condition=expression_tree,  # Use the built expression tree
                    all_relations={node.relation, node.alias},
                )

                logical_plan.insert_node_after(random_string(), filter_node, nid)
                statistics.visibility_filters_condition_added += 1
    return logical_plan


def do_logical_planning_phase(parsed_statement: dict) -> tuple:
    # The sqlparser ast is an array of asts

    statement_type = next(iter(parsed_statement))
    if statement_type not in QUERY_BUILDERS:
        from opteryx.exceptions import UnsupportedSyntaxError
        from opteryx.utils.sql import convert_camel_to_sql_case

        raise UnsupportedSyntaxError(
            f"Opteryx does not support '{convert_camel_to_sql_case(statement_type)}' type queries."
        )
    # CTEs are Common Table Expressions, they're variations of subqueries
    ctes = extract_ctes(parsed_statement, inner_query_planner)
    return QUERY_BUILDERS[statement_type](parsed_statement), parsed_statement, ctes
