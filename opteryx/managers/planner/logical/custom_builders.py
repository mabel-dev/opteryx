"""
Builders which require special handling
"""

from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType
from opteryx.utils import fuzzy_search


def extract_show_filter(ast):
    """filters are used in SHOW queries"""
    filters = ast["filter"]
    if filters is None:
        return None
    if "Like" in filters:
        left = ExpressionTreeNode(NodeType.IDENTIFIER, value="name")
        right = ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=filters["Like"])
        root = ExpressionTreeNode(
            NodeType.COMPARISON_OPERATOR,
            value="Like",
            left_node=left,
            right_node=right,
        )
        return root


def extract_distinct(ast):
    return ast["Query"]["body"]["Select"]["distinct"]


def extract_limit(ast):
    limit = ast["Query"].get("limit")
    if limit is not None:
        return int(limit["Value"]["Number"][0])
    return None


def extract_offset(ast):
    offset = ast["Query"].get("offset")
    if offset is not None:
        return int(offset["value"]["Value"]["Number"][0])
    return None


def extract_order(ast):
    from opteryx.managers.planner.logical import builders

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


def extract_identifiers(ast):
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
            identifiers.extend(extract_identifiers(value))
    if isinstance(ast, list):
        for item in ast:
            if item in ("Wildcard",):
                identifiers.append("*")
            identifiers.extend(extract_identifiers(item))

    return list(set(identifiers))


def extract_joins(ast):
    from opteryx.managers.planner.logical import builders

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

        right = next(extract_relations([join]))
        yield (join_mode, right, join_on, join_using)


def extract_relations(branch):
    """ """
    from opteryx.managers.planner.logical import builders
    from opteryx.managers.planner import QueryPlanner

    def _check_hints(hints):

        well_known_hints = (
            "NO_CACHE",
            "NO_PARTITION",
            "NO_PUSH_PROJECTION",
            "PARALLEL_READ",
        )

        for hint in hints:
            if hint not in well_known_hints:
                best_match_hint = fuzzy_search(hint, well_known_hints)

    #                if best_match_hint:
    #                    _statistics.warn(
    #                        f"Hint `{hint}` is not recognized, did you mean `{best_match_hint}`?"
    #                    )
    #                else:
    #                    _statistics.warn(f"Hint `{hint}` is not recognized.")

    for relation in branch:
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

                subquery_planner = QueryPlanner()
                plan = subquery_planner.create_logical_plan(ast)
                plan = subquery_planner.optimize_plan(plan)

                yield (alias, plan, "SubQuery", [])
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
