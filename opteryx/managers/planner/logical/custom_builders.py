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
Builders which require special handling
"""
import datetime

from dataclasses import dataclass, field
from typing import Any

from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType
from opteryx.shared import QueryStatistics
from opteryx.utils import fuzzy_search

WELL_KNOWN_HINTS = {
    "NO_CACHE",
    "NO_PARTITION",
    "NO_PUSH_PROJECTION",
    "PARALLEL_READ",
}


@dataclass
class RelationDescription:
    dataset: str = None
    alias: str = None
    kind: str = None
    hints: list = field(default_factory=list)
    start_date: datetime.date = None
    end_date: datetime.date = None
    cache: Any = None


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
            value="ILike",  # we're case insensitive for SHOW filters
            left=left,
            right=right,
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
    if ast == {
        "name": [{"value": "COUNT", "quote_style": None}],
        "args": [{"Unnamed": "Wildcard"}],
        "over": None,
        "distinct": False,
        "special": False,
    } or ast == {
        "name": [{"value": "count", "quote_style": None}],
        "args": [{"Unnamed": "Wildcard"}],
        "over": None,
        "distinct": False,
        "special": False,
    }:
        identifiers.append("count_*")
    elif isinstance(ast, dict):
        for key, value in ast.items():
            if key in ("Identifier",):
                identifiers.append(value["value"])
            if key in ("Using",):
                for item in ast["Using"]:
                    identifiers.append(item["value"])
            if key in ("QualifiedWildcard",):
                identifiers.append("*")
            identifiers.extend(extract_identifiers(value))
    elif isinstance(ast, list):
        for item in ast:
            if item in ("Wildcard", {"Unnamed": "Wildcard"}):
                identifiers.append("*")
            identifiers.extend(extract_identifiers(item))

    return identifiers


def extract_joins(ast, qid):
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

        right = next(extract_relations([join], qid))
        yield (join_mode, right, join_on, join_using)


def extract_relations(branch, qid):
    """ """
    from opteryx.managers.planner.logical import builders
    from opteryx.managers.planner import QueryPlanner

    def _check_hints(hints):

        for hint in hints:
            if hint not in WELL_KNOWN_HINTS:
                best_match_hint = fuzzy_search(hint, WELL_KNOWN_HINTS)
                statistics = QueryStatistics(qid)
                if best_match_hint:
                    statistics.add_message(
                        f"Hint `{hint}` is not recognized, did you mean `{best_match_hint}`?"
                    )
                else:
                    statistics.add_message(f"Hint `{hint}` is not recognized.")

    for relation in branch:
        relation_desc = RelationDescription()
        if "Table" in relation["relation"]:
            # is the relation a builder function
            if relation["relation"]["Table"]["args"]:
                function = relation["relation"]["Table"]["name"][0]["value"].lower()
                relation_desc.alias = function
                if relation["relation"]["Table"]["alias"] is not None:
                    relation_desc.alias = relation["relation"]["Table"]["alias"][
                        "name"
                    ]["value"]
                args = [
                    builders.build(a["Unnamed"])
                    for a in relation["relation"]["Table"]["args"]
                ]
                relation_desc.kind = "Function"
                relation_desc.dataset = {"function": function, "args": args}
                yield relation_desc
            else:
                if relation["relation"]["Table"]["alias"] is not None:
                    relation_desc.alias = relation["relation"]["Table"]["alias"][
                        "name"
                    ]["value"]
                if relation["relation"]["Table"]["with_hints"] is not None:
                    relation_desc.hints = [
                        hint["Identifier"]["value"]
                        for hint in relation["relation"]["Table"]["with_hints"]
                    ]
                    # hint checks
                    _check_hints(relation_desc.hints)
                relation_desc.dataset = ".".join(
                    [part["value"] for part in relation["relation"]["Table"]["name"]]
                )
                relation_desc.start_date = relation["relation"]["Table"]["start_date"]
                relation_desc.end_date = relation["relation"]["Table"]["end_date"]
                relation_desc.cache = relation["relation"]["Table"]["cache"]
                if relation_desc.dataset[0:1] == "$":
                    relation_desc.kind = "Internal"
                else:
                    relation_desc.kind = "External"
                yield relation_desc

        if "Derived" in relation["relation"]:
            subquery = relation["relation"]["Derived"]["subquery"]["body"]
            try:
                relation_desc.alias = relation["relation"]["Derived"]["alias"]["name"][
                    "value"
                ]
            except (KeyError, TypeError):
                pass
            if "Select" in subquery:
                ast = {}
                ast["Query"] = relation["relation"]["Derived"]["subquery"]

                subquery_planner = QueryPlanner(qid=qid)
                plan = subquery_planner.create_logical_plan(ast)
                plan = subquery_planner.optimize_plan(plan)

                relation_desc.dataset = plan
                relation_desc.kind = "SubQuery"
                yield relation_desc
            if "Values" in subquery:
                body = []
                headers = [
                    h["value"]
                    for h in relation["relation"]["Derived"]["alias"]["columns"]
                ]
                for value_set in subquery["Values"]:
                    values = [builders.build(v["Value"]).value for v in value_set]
                    body.append(dict(zip(headers, values)))
                relation_desc.dataset = {"function": "values", "args": body}
                relation_desc.kind = "Function"
                yield relation_desc


def extract_into(branch):
    return None
