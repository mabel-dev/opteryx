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
from opteryx import operators
from opteryx.connectors import connector_factory
from opteryx.exceptions import ProgrammingError, SqlError
from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.managers.expression import NodeType
from opteryx.managers.planner.logical import builders, custom_builders
from opteryx.models import ExecutionTree


def explain_query(ast, properties):
    # we're handling two plans here:
    # - plan - this is the plan for the query we're exlaining
    # - my_plan - this is the plan for this query

    from opteryx.managers.planner import QueryPlanner

    query_planner = QueryPlanner(properties=properties)
    plan = query_planner.create_logical_plan(ast["Explain"]["statement"])
    plan = query_planner.optimize_plan(plan)
    my_plan = ExecutionTree()
    explain_node = operators.ExplainNode(properties, query_plan=plan)
    my_plan.add_operator("explain", explain_node)
    return my_plan


def select_query(ast, properties):
    """
    The planner creates the naive query plan.

    The goal here is to create a plan that's only guarantee is the response is correct.
    It doesn't try to make it performant, low-memory or any other measure of 'good'
    beyond correctness.
    """
    plan = ExecutionTree()

    all_identifiers = (
        set(custom_builders.extract_identifiers(ast)) - custom_builders.WELL_KNOWN_HINTS
    )
    try:
        _relations = list(
            custom_builders.extract_relations(
                ast["Query"]["body"]["Select"]["from"], properties.qid
            )
        )
    except IndexError:
        _relations = []

    # if we have no relations, use the $no_table relation
    if len(_relations) == 0:
        _relations = [
            custom_builders.RelationDescription(
                dataset="$no_table", kind="Internal", cache=properties.cache
            )
        ]

    # We always have a data source - even if it's 'no table'
    relation = _relations[0]

    # external comes in different flavours
    reader = None
    if relation.kind == "External":
        reader = connector_factory(relation.dataset)
        relation.kind = reader.__mode__

    plan.add_operator(
        "from",
        operators.reader_factory(relation.kind)(
            properties=properties,
            alias=relation.alias,
            dataset=relation.dataset,
            reader=reader,
            cache=relation.cache,
            start_date=relation.start_date,
            end_date=relation.end_date,
            hints=relation.hints,
            selection=all_identifiers,
        ),
    )
    last_node = "from"

    _joins = list(custom_builders.extract_joins(ast, properties.qid))
    if len(_joins) == 0 and len(_relations) == 2:
        # If there's no explicit JOIN but the query has two relations, we
        # use a CROSS JOIN
        _joins = [("CrossJoin", _relations[1], None, None)]
    for join_id, _join in enumerate(_joins):
        if _join:
            join_type, right, join_on, join_using = _join
            if join_type == "CrossJoin" and right.kind == "Function":
                join_type = "CrossJoinUnnest"
            else:

                dataset = right.dataset
                if isinstance(dataset, ExecutionTree):
                    mode = "Blob"  # subqueries are here due to legacy reasons
                    reader = None
                elif isinstance(dataset, dict) and dataset.get("function") is not None:
                    mode = "Function"
                    reader = None
                elif dataset[0:1] == "$":
                    mode = "Internal"
                    reader = None
                else:
                    reader = connector_factory(dataset)
                    mode = reader.__mode__

                # Otherwise, the right table needs to come from the Reader
                right = operators.reader_factory(mode)(
                    properties=properties,
                    dataset=right.dataset,
                    alias=right.alias,
                    reader=reader,
                    cache=relation.cache,
                    start_date=right.start_date,
                    end_date=right.end_date,
                    hints=right.hints,
                )

            join_node = operators.join_factory(join_type)
            if join_node is None:
                raise SqlError(f"Join type not supported - `{_join[0]}`")

            plan.add_operator(
                f"join-{join_id}",
                join_node(
                    properties=properties,
                    join_type=join_type,
                    join_on=join_on,
                    join_using=join_using,
                ),
            )
            plan.link_operators(last_node, f"join-{join_id}")

            plan.add_operator(f"join-{join_id}-right", right)
            plan.link_operators(f"join-{join_id}-right", f"join-{join_id}", "right")

            last_node = f"join-{join_id}"

    _selection = builders.build(ast["Query"]["body"]["Select"]["selection"])
    if _selection:
        plan.add_operator(
            "where",
            operators.SelectionNode(properties, filter=_selection),
        )
        plan.link_operators(last_node, "where")
        last_node = "where"

    _projection = builders.build(ast["Query"]["body"]["Select"]["projection"])
    _groups = builders.build(ast["Query"]["body"]["Select"]["group_by"])
    if _groups or get_all_nodes_of_type(
        _projection, select_nodes=(NodeType.AGGREGATOR, NodeType.COMPLEX_AGGREGATOR)
    ):
        _aggregates = _projection.copy()
        if isinstance(_aggregates, dict):
            raise SqlError("GROUP BY cannot be used with SELECT *")
        plan.add_operator(
            "agg",
            operators.AggregateNode(properties, aggregates=_aggregates, groups=_groups),
        )
        plan.link_operators(last_node, "agg")
        last_node = "agg"

    _having = builders.build(ast["Query"]["body"]["Select"]["having"])
    if _having:
        plan.add_operator(
            "having",
            operators.SelectionNode(properties, filter=_having),
        )
        plan.link_operators(last_node, "having")
        last_node = "having"

    # qualified wildcards have the qualifer in the value
    # e.g. SELECT table.* -> node.value = table
    if (_projection[0].token_type != NodeType.WILDCARD) or (
        _projection[0].value is not None
    ):
        plan.add_operator(
            "select",
            operators.ProjectionNode(properties, projection=_projection),
        )
        plan.link_operators(last_node, "select")
        last_node = "select"

    _distinct = custom_builders.extract_distinct(ast)
    if _distinct:
        plan.add_operator("distinct", operators.DistinctNode(properties))
        plan.link_operators(last_node, "distinct")
        last_node = "distinct"

    _order = custom_builders.extract_order(ast)
    if _order:
        plan.add_operator("order", operators.SortNode(properties, order=_order))
        plan.link_operators(last_node, "order")
        last_node = "order"

    _offset = custom_builders.extract_offset(ast)
    if _offset:
        plan.add_operator(
            "offset",
            operators.OffsetNode(properties, offset=_offset),
        )
        plan.link_operators(last_node, "offset")
        last_node = "offset"

    _limit = custom_builders.extract_limit(ast)
    # 0 limit is valid
    if _limit is not None:
        plan.add_operator("limit", operators.LimitNode(properties, limit=_limit))
        plan.link_operators(last_node, "limit")
        last_node = "limit"

    _insert = custom_builders.extract_into(ast)

    return plan


def set_variable_query(ast, properties):
    """put variables defined in SET statements into context"""
    key = ast["SetVariable"]["variable"][0]["value"]
    value = builders.build(ast["SetVariable"]["value"][0]["Value"])
    if key[0] == "@":  # pragma: no cover
        properties.variables[key] = value
    else:
        key = key.lower()
        if key in properties.read_only_properties:
            raise ProgrammingError(f"Invalid parameter '{key}'")
        if hasattr(properties, key):
            setattr(properties, key, value.value)
        else:
            raise ProgrammingError(
                f"Unknown parameter, variables must be prefixed with a '@' - '{key}'"
            )

    # return a plan, because it's expected
    plan = ExecutionTree()
    operator = operators.ShowValueNode(
        key="result", value="Complete", properties=properties
    )
    plan.add_operator("show", operator=operator)
    return plan


def show_columns_query(ast, properties):

    plan = ExecutionTree()
    dataset = ".".join([part["value"] for part in ast["ShowColumns"]["table_name"]])

    if dataset[0:1] == "$":
        mode = "Internal"
        reader = None
    else:
        reader = connector_factory(dataset)
        mode = reader.__mode__

    plan.add_operator(
        "reader",
        operators.reader_factory(mode)(
            properties=properties,
            dataset=dataset,
            alias=None,
            reader=reader,
            cache=None,  # never read from cache
            start_date=ast["ShowColumns"]["table_name"][0]["start_date"],
            end_date=ast["ShowColumns"]["table_name"][0]["end_date"],
        ),
    )
    last_node = "reader"

    filters = custom_builders.extract_show_filter(ast["ShowColumns"])
    if filters:
        plan.add_operator(
            "filter",
            operators.ColumnFilterNode(properties=properties, filter=filters),
        )
        plan.link_operators(last_node, "filter")
        last_node = "filter"

    plan.add_operator(
        "columns",
        operators.ShowColumnsNode(
            properties=properties,
            full=ast["ShowColumns"]["full"],
            extended=ast["ShowColumns"]["extended"],
        ),
    )
    plan.link_operators(last_node, "columns")
    last_node = "columns"

    return plan


def show_create_query(ast, properties):

    plan = ExecutionTree()

    if ast["ShowCreate"]["obj_type"] != "Table":
        raise SqlError("SHOW CREATE only supports tables")

    dataset = ".".join([part["value"] for part in ast["ShowCreate"]["obj_name"]])

    if dataset[0:1] == "$":
        mode = "Internal"
        reader = None
    else:
        reader = connector_factory(dataset)
        mode = reader.__mode__

    plan.add_operator(
        "reader",
        operators.reader_factory(mode)(
            properties=properties,
            dataset=dataset,
            alias=None,
            reader=reader,
            cache=None,  # never read from cache
            start_date=ast["ShowCreate"]["start_date"],
            end_date=ast["ShowCreate"]["end_date"],
        ),
    )
    last_node = "reader"

    plan.add_operator(
        "show_create",
        operators.ShowCreateNode(properties=properties, table=dataset),
    )
    plan.link_operators(last_node, "show_create")
    last_node = "show_create"

    return plan


def show_variable_query(ast, properties):
    """
    This is the generic SHOW <variable> handler - there are specific handlers
    for some keywords after SHOW, like SHOW COLUMNS.

    SHOW <variable> only really has a single node.

    All of the keywords should up as a 'values' list in the variable in the ast.
    """

    plan = ExecutionTree()

    keywords = [value["value"].upper() for value in ast["ShowVariable"]["variable"]]
    if keywords[0] == "PARAMETER":
        if len(keywords) != 2:
            raise SqlError("`SHOW PARAMETER` expects a single parameter name.")
        key = keywords[1].lower()
        if not hasattr(properties, key) or key == "variables":
            raise SqlError(f"Unknown parameter '{key}'.")
        value = getattr(properties, key)

        show_node = "show_parameter"
        node = operators.ShowValueNode(properties=properties, key=key, value=value)
        plan.add_operator(show_node, operator=node)
    elif keywords[0] == "STORES":
        if len(keywords) != 1:
            raise SqlError(f"`SHOW STORES` end expected, got '{keywords[1]}'")
        show_node = "show_stores"
        node = operators.ShowStoresNode(properties=properties)
        plan.add_operator(show_node, operator=node)
    else:  # pragma: no cover
        raise SqlError(f"SHOW statement type not supported for `{keywords[0]}`.")

    name_column = ExpressionTreeNode(NodeType.IDENTIFIER, value="name")

    order_by_node = operators.SortNode(
        properties=properties,
        order=[([name_column], "ascending")],
    )
    plan.add_operator("order", operator=order_by_node)
    plan.link_operators(show_node, "order")

    return plan


def show_functions_query(ast, properties):
    """show the supported functions, optionally filter them"""
    plan = ExecutionTree()

    show = operators.ShowFunctionsNode(properties=properties)
    plan.add_operator("show", show)
    last_node = "show"

    filters = custom_builders.extract_show_filter(ast["ShowFunctions"])
    if filters:
        plan.add_operator(
            "filter",
            operators.SelectionNode(properties=properties, filter=filters),
        )
        plan.link_operators(last_node, "filter")

    return plan


def show_variables_query(ast, properties):
    """show the known variables, optionally filter them"""
    plan = ExecutionTree()

    show = operators.ShowVariablesNode(properties=properties)
    plan.add_operator("show", show)
    last_node = "show"

    filters = custom_builders.extract_show_filter(ast["ShowVariables"])
    if filters:
        plan.add_operator(
            "filter",
            operators.SelectionNode(properties=properties, filter=filters),
        )
        plan.link_operators(last_node, "filter")

    return plan


def analyze_query(ast, properties):
    """build statistics for a table"""
    plan = ExecutionTree()
    dataset = ".".join([part["value"] for part in ast["Analyze"]["table_name"]])

    if dataset[0:1] == "$":
        mode = "Internal"
        reader = None
    else:
        reader = connector_factory(dataset)
        mode = reader.__mode__

    plan.add_operator(
        "reader",
        operators.reader_factory(mode)(
            properties=properties,
            dataset=dataset,
            alias=None,
            reader=reader,
            cache=None,  # never read from cache
            start_date=ast["Analyze"]["table_name"][0]["start_date"],
            end_date=ast["Analyze"]["table_name"][0]["end_date"],
        ),
    )
    last_node = "reader"

    plan.add_operator(
        "buildstats",
        operators.BuildStatisticsNode(properties=properties),
    )
    plan.link_operators(last_node, "buildstats")

    return plan


# wrappers for the query builders
QUERY_BUILDER = {
    "Analyze": analyze_query,
    "Explain": explain_query,
    "Query": select_query,
    "SetVariable": set_variable_query,
    "ShowColumns": show_columns_query,
    "ShowCreate": show_create_query,
    "ShowFunctions": show_functions_query,
    "ShowVariable": show_variable_query,  # generic SHOW handler
    "ShowVariables": show_variables_query,
}
