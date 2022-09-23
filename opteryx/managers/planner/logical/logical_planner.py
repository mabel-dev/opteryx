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
This builds a logical plan can resolve the query from the user.

This doesn't attempt to do optimization, this just build a convenient plan which will
respond to the query correctly.

The effective order of operations must be:

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

So we just build it in that order.
"""
import pyarrow

from opteryx import operators
from opteryx.connectors import connector_factory
from opteryx.exceptions import SqlError, UnsupportedSyntaxError

from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.managers.expression import NodeType


from opteryx.managers.planner.logical import builders, queries

from opteryx.models import Columns


def create_plan(ast, properties):

    last_query = None
    for query in ast:
        query_type = next(iter(query))
        builder = queries.QUERY_BUILDER.get(query_type)
        if builder is None:
            raise UnsupportedSyntaxError(f"Statement not supported `{query_type}`")
        last_query = builder(query, properties)
    return last_query


def _check_hints(hints):

    from opteryx.third_party.mbleven import compare

    well_known_hints = (
        "NO_CACHE",
        "NO_PARTITION",
        "NO_PUSH_PROJECTION",
        "PARALLEL_READ",
    )

    for hint in hints:
        if hint not in well_known_hints:
            best_match_hint = None
            best_match_score = 100

            for known_hint in well_known_hints:
                my_dist = compare(hint, known_hint)
                if my_dist > 0 and my_dist < best_match_score:
                    best_match_score = my_dist
                    best_match_hint = known_hint

            if best_match_hint:
                _statistics.warn(
                    f"Hint `{hint}` is not recognized, did you mean `{best_match_hint}`?"
                )
            else:
                _statistics.warn(f"Hint `{hint}` is not recognized.")


def _extract_relations(ast, default_path: bool = True):
    """ """
    relations = ast
    if default_path:
        try:
            relations = ast["Query"]["body"]["Select"]["from"]
        except IndexError:
            return "$no_table"

    for relation in relations:
        if "Table" in relation["relation"]:
            # is the relation a builder function
            if relation["relation"]["Table"]["args"]:
                function = relation["relation"]["Table"]["name"][0]["value"].lower()
                alias = function
                if relation["relation"]["Table"]["alias"] is not None:
                    alias = relation["relation"]["Table"]["alias"]["name"]["value"]
                args = [
                    builders.build(a["Unnamed"])
                    for a in relation["relation"]["Table"]["args"]
                ]
                yield (alias, {"function": function, "args": args}, "Function", [])
            else:
                alias = None
                if relation["relation"]["Table"]["alias"] is not None:
                    alias = relation["relation"]["Table"]["alias"]["name"]["value"]
                hints = []
                if relation["relation"]["Table"]["with_hints"] is not None:
                    hints = [
                        hint["Identifier"]["value"]
                        for hint in relation["relation"]["Table"]["with_hints"]
                    ]
                    # hint checks
                    _check_hints(hints)
                dataset = ".".join(
                    [part["value"] for part in relation["relation"]["Table"]["name"]]
                )
                if dataset[0:1] == "$":
                    yield (alias, dataset, "Internal", hints)
                else:
                    yield (alias, dataset, "External", hints)

        if "Derived" in relation["relation"]:
            subquery = relation["relation"]["Derived"]["subquery"]["body"]
            try:
                alias = relation["relation"]["Derived"]["alias"]["name"]["value"]
            except (KeyError, TypeError):
                alias = None
            if "Select" in subquery:
                ast = {}
                ast["Query"] = relation["relation"]["Derived"]["subquery"]
                subquery_plan = copy()
                subquery_plan.create_plan(ast=[ast])

                yield (alias, subquery_plan, "SubQuery", [])
            if "Values" in subquery:
                body = []
                headers = [
                    h["value"]
                    for h in relation["relation"]["Derived"]["alias"]["columns"]
                ]
                for value_set in subquery["Values"]:
                    values = [builders.build(v["Value"]).value for v in value_set]
                    body.append(dict(zip(headers, values)))
                yield (alias, {"function": "values", "args": body}, "Function", [])


def _extract_joins(ast):
    try:
        joins = ast["Query"]["body"]["Select"]["from"][0]["joins"]
    except IndexError:
        return None

    for join in joins:
        join_using = None
        join_on = None
        join_mode = join["join_operator"]
        if isinstance(join_mode, dict):
            join_mode = list(join["join_operator"].keys())[0]
            if "Using" in join["join_operator"][join_mode]:
                join_using = [
                    v["value"]
                    for v in join["join_operator"][join_mode].get("Using", [])
                ]
            if "On" in join["join_operator"][join_mode]:
                join_on = builders.build(join["join_operator"][join_mode]["On"])

        right = next(builders.build([join]))
        yield (join_mode, right, join_on, join_using)


def _extract_distinct(ast):
    return ast["Query"]["body"]["Select"]["distinct"]


def _extract_limit(ast):
    limit = ast["Query"].get("limit")
    if limit is not None:
        return int(limit["Value"]["Number"][0])
    return None


def _extract_offset(ast):
    offset = ast["Query"].get("offset")
    if offset is not None:
        return int(offset["value"]["Value"]["Number"][0])
    return None


def _extract_order(ast):
    order = ast["Query"].get("order_by")
    if order is not None:
        orders = []
        for col in order:
            column = builders.build([col["expr"]])
            orders.append(
                (
                    column,
                    "descending" if str(col["asc"]) == "False" else "ascending",
                ),
            )
        return orders


def _extract_having(ast):
    having = ast["Query"]["body"]["Select"]["having"]
    return builders.build(having)


def _explain_planner(ast, statistics):
    explain_plan = copy()
    explain_plan.create_plan(ast=[ast["Explain"]["statement"]])
    explain_node = operators.ExplainNode(
        properties, statistics, query_plan=explain_plan
    )
    add_operator("explain", explain_node)


def _show_columns_planner(ast, statistics):

    dataset = ".".join([part["value"] for part in ast["ShowColumns"]["table_name"]])

    if dataset[0:1] == "$":
        mode = "Internal"
        reader = None
    else:
        reader = connector_factory(dataset)
        mode = reader.__mode__

    add_operator(
        "reader",
        operators.reader_factory(mode)(
            properties=properties,
            statistics=statistics,
            dataset=dataset,
            alias=None,
            reader=reader,
            cache=None,  # never read from cache
            start_date=start_date,
            end_date=end_date,
        ),
    )
    last_node = "reader"

    filters = builders.build(ast["ShowColumns"])
    if filters:
        add_operator(
            "filter",
            operators.ColumnFilterNode(
                properties=properties, statistics=statistics, filter=filters
            ),
        )
        link_operators(last_node, "filter")
        last_node = "filter"

    add_operator(
        "columns",
        operators.ShowColumnsNode(
            properties=properties,
            statistics=statistics,
            full=ast["ShowColumns"]["full"],
            extended=ast["ShowColumns"]["extended"],
        ),
    )
    link_operators(last_node, "columns")
    last_node = "columns"


def _show_create_planner(ast, statistics):

    if ast["ShowCreate"]["obj_type"] != "Table":
        raise SqlError("SHOW CREATE only supports tables")

    dataset = ".".join([part["value"] for part in ast["ShowCreate"]["obj_name"]])

    if dataset[0:1] == "$":
        mode = "Internal"
        reader = None
    else:
        reader = connector_factory(dataset)
        mode = reader.__mode__

    add_operator(
        "reader",
        operators.reader_factory(mode)(
            properties=properties,
            statistics=statistics,
            dataset=dataset,
            alias=None,
            reader=reader,
            cache=None,  # never read from cache
            start_date=start_date,
            end_date=end_date,
        ),
    )
    last_node = "reader"

    add_operator(
        "show_create",
        operators.ShowCreateNode(
            properties=properties, statistics=statistics, table=dataset
        ),
    )
    link_operators(last_node, "show_create")
    last_node = "show_create"


def _extract_identifiers(ast):
    identifiers = []
    if isinstance(ast, dict):
        for key, value in ast.items():
            if key in ("Identifier",):
                identifiers.append(value["value"])
            if key in ("Using",):
                for item in ast["Using"]:
                    identifiers.append(item["value"])
            if key in ("QualifiedWildcard",):
                identifiers.append("*")
            identifiers.extend(_extract_identifiers(value))
    if isinstance(ast, list):
        for item in ast:
            if item in ("Wildcard",):
                identifiers.append("*")
            identifiers.extend(_extract_identifiers(item))

    return list(set(identifiers))


def _show_variable_planner(ast, statistics):
    """
    SHOW <variable> only really has a single node.

    All of the keywords should up as a 'values' list in the variable in the ast.

    The last word is the variable, preceeding words are modifiers.
    """

    keywords = [value["value"].upper() for value in ast["ShowVariable"]["variable"]]
    if keywords[0] == "FUNCTIONS":
        show_node = "show_functions"
        node = operators.ShowFunctionsNode(
            properties=properties,
            statistics=statistics,
        )
        add_operator(show_node, operator=node)
    elif keywords[0] == "PARAMETER":
        if len(keywords) != 2:
            raise SqlError("`SHOW PARAMETER` expects a single parameter name.")
        key = keywords[1].lower()
        if not hasattr(properties, key) or key == "variables":
            raise SqlError(f"Unknown parameter '{key}'.")
        value = getattr(properties, key)

        show_node = "show_parameter"
        node = operators.ShowValueNode(
            properties=properties, statistics=statistics, key=key, value=value
        )
        add_operator(show_node, operator=node)
    else:  # pragma: no cover
        raise SqlError(f"SHOW statement type not supported for `{keywords[0]}`.")

    name_column = ExpressionTreeNode(NodeType.IDENTIFIER, value="name")

    order_by_node = operators.SortNode(
        properties=properties,
        statistics=statistics,
        order=[([name_column], "ascending")],
    )
    add_operator("order", operator=order_by_node)
    link_operators(show_node, "order")


def _naive_select_planner(ast, statistics):
    """
    The planner creates the naive query plan.

    The goal here is to create a plan to respond to the user, it creates has
    no clever tricks to improve performance.
    """
    all_identifiers = _extract_identifiers(ast)

    _relations = [r for r in _extract_relations(ast)]
    if len(_relations) == 0:
        _relations = [(None, "$no_table", "Internal", [])]

    # We always have a data source - even if it's 'no table'
    alias, dataset, mode, hints = _relations[0]

    # external comes in different flavours
    reader = None
    if mode == "External":
        reader = connector_factory(dataset)
        mode = reader.__mode__

    add_operator(
        "from",
        operators.reader_factory(mode)(
            properties=properties,
            statistics=statistics,
            alias=alias,
            dataset=dataset,
            reader=reader,
            cache=_cache,
            start_date=start_date,
            end_date=end_date,
            hints=hints,
            selection=all_identifiers,
        ),
    )
    last_node = "from"

    _joins = list(_extract_joins(ast))
    if len(_joins) == 0 and len(_relations) == 2:
        # If there's no explicit JOIN but the query has two relations, we
        # use a CROSS JOIN
        _joins = [("CrossJoin", _relations[1], None, None)]
    for join_id, _join in enumerate(_joins):
        if _join:
            join_type, right, join_on, join_using = _join
            if join_type == "CrossJoin" and right[2] == "Function":
                join_type = "CrossJoinUnnest"
            else:

                dataset = right[1]
                if isinstance(dataset, QueryPlanner):
                    mode = "Blob"  # this is still here until it's moved
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
                    statistics=statistics,
                    dataset=dataset,
                    alias=right[0],
                    reader=reader,
                    cache=_cache,
                    start_date=start_date,
                    end_date=end_date,
                    hints=right[3],
                )

            join_node = operators.join_factory(join_type)
            if join_node is None:
                raise SqlError(f"Join type not supported - `{_join[0]}`")

            add_operator(
                f"join-{join_id}",
                join_node(
                    properties=properties,
                    statistics=statistics,
                    join_type=join_type,
                    join_on=join_on,
                    join_using=join_using,
                ),
            )
            link_operators(last_node, f"join-{join_id}")

            add_operator(f"join-{join_id}-right", right)
            link_operators(f"join-{join_id}-right", f"join-{join_id}", "right")

            last_node = f"join-{join_id}"

    _selection = builders.build(ast["Query"]["body"]["Select"]["selection"])
    if _selection:
        add_operator(
            "where",
            operators.SelectionNode(properties, statistics, filter=_selection),
        )
        link_operators(last_node, "where")
        last_node = "where"

    _projection = _extract_field_list(ast["Query"]["body"]["Select"]["projection"])
    _groups = _extract_field_list(ast["Query"]["body"]["Select"]["group_by"])
    if _groups or get_all_nodes_of_type(
        _projection, select_nodes=(NodeType.AGGREGATOR,)
    ):
        _aggregates = _projection.copy()
        if isinstance(_aggregates, dict):
            raise SqlError("GROUP BY cannot be used with SELECT *")
        if not any(
            a.token_type == NodeType.AGGREGATOR
            for a in _aggregates
            if isinstance(a, ExpressionTreeNode)
        ):
            wildcard = ExpressionTreeNode(NodeType.WILDCARD)
            _aggregates.append(
                ExpressionTreeNode(
                    NodeType.AGGREGATOR, value="COUNT", parameters=[wildcard]
                )
            )
        add_operator(
            "agg",
            operators.AggregateNode(
                properties, statistics, aggregates=_aggregates, groups=_groups
            ),
        )
        link_operators(last_node, "agg")
        last_node = "agg"

    _having = _extract_having(ast)
    if _having:
        add_operator(
            "having",
            operators.SelectionNode(properties, statistics, filter=_having),
        )
        link_operators(last_node, "having")
        last_node = "having"

    _projection = _extract_field_list(ast["Query"]["body"]["Select"]["projection"])
    # qualified wildcards have the qualifer in the value
    # e.g. SELECT table.* -> node.value = table
    if (_projection[0].token_type != NodeType.WILDCARD) or (
        _projection[0].value is not None
    ):
        add_operator(
            "select",
            operators.ProjectionNode(properties, statistics, projection=_projection),
        )
        link_operators(last_node, "select")
        last_node = "select"

    _distinct = _extract_distinct(ast)
    if _distinct:
        add_operator("distinct", operators.DistinctNode(properties, statistics))
        link_operators(last_node, "distinct")
        last_node = "distinct"

    _order = _extract_order(ast)
    if _order:
        add_operator("order", operators.SortNode(properties, statistics, order=_order))
        link_operators(last_node, "order")
        last_node = "order"

    _offset = _extract_offset(ast)
    if _offset:
        add_operator(
            "offset",
            operators.OffsetNode(properties, statistics, offset=_offset),
        )
        link_operators(last_node, "offset")
        last_node = "offset"

    _limit = _extract_limit(ast)
    # 0 limit is valid
    if _limit is not None:
        add_operator("limit", operators.LimitNode(properties, statistics, limit=_limit))
        link_operators(last_node, "limit")
        last_node = "limit"


def explain(self):
    def _inner_explain(node, depth):
        if depth == 1:
            operator = get_operator(node)
            yield {
                "operator": operator.name,
                "config": operator.config,
                "depth": depth - 1,
            }
        incoming_operators = get_incoming_links(node)
        for operator_name in incoming_operators:
            operator = get_operator(operator_name[0])
            if isinstance(operator, operators.BasePlanNode):
                yield {
                    "operator": operator.name,
                    "config": operator.config,
                    "depth": depth,
                }
            yield from _inner_explain(operator_name[0], depth + 1)

    head = list(set(get_exit_points()))
    # print(head, _edges)
    if len(head) != 1:
        raise SqlError(f"Problem with the plan - it has {len(head)} heads.")
    plan = list(_inner_explain(head[0], 1))

    table = pyarrow.Table.from_pylist(plan)
    table = Columns.create_table_metadata(table, table.num_rows, "plan", None)
    yield table
