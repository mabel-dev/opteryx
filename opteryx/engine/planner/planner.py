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
Query Planner
-------------

This builds a DAG which describes a query.

This doesn't attempt to do optimization, this just decomposes the query.

The effective order of operations must be:
    01. FROM
    02. < temporal filters
    03. JOIN
    04. < expressions and aliases
    05. WHERE
    06. GROUP BY
    07. HAVING
    08. SELECT
    09. DISTINCT
    10. ORDER BT
    11. OFFSET
    12. LIMIT

However, this doesn't preclude the order being different to achieve optimizations, as
long as the functional outcode would be the same. Expressions and aliases technically
should not be evaluated until the SELECT statement.

note: This module does not handle temporal filters, those as part of the FOR clause, 
these are not supported by SqlOxide and so are in a different module which strips
temporal aspects out of the query.
"""
import datetime

import numpy
import pyarrow
import sqloxide

from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.engine.functions import is_function
from opteryx.engine.planner import operations
from opteryx.engine.planner.execution_tree import ExecutionTree
from opteryx.engine.planner.expression import ExpressionTreeNode
from opteryx.engine.planner.expression import NodeType
from opteryx.engine.planner.expression import operator_type_factory
from opteryx.engine.planner.temporal import extract_temporal_filters
from opteryx.engine.query_directives import QueryDirectives
from opteryx.exceptions import SqlError
from opteryx.storage import get_adapter
from opteryx.utils import dates
from opteryx.utils.columns import Columns


class QueryPlanner(ExecutionTree):
    def __init__(self, statistics, cache=None):
        """
        Planner creates a plan (Execution Tree or DAG) which presents the plan to
        respond to the query.
        """
        super().__init__()

        self._ast = None

        self._statistics = statistics
        self._directives = QueryDirectives()
        self._cache = cache

        self.start_date = datetime.datetime.utcnow().date()
        self.end_date = datetime.datetime.utcnow().date()

    def __repr__(self):
        return "QueryPlanner"

    def copy(self):
        """copy a plan"""
        planner = QueryPlanner(
            statistics=self._statistics,
            cache=self._cache,
        )
        planner.start_date = self.start_date
        planner.end_date = self.end_date
        return planner

    def create_plan(self, sql: str = None, ast: dict = None):

        if sql:

            # if it's a byte string, convert to an ascii string
            if isinstance(sql, bytes):
                sql = sql.decode()

            # extract temporal filters, this isn't supported by sqloxide
            self.start_date, self.end_date, sql = extract_temporal_filters(sql)
            # Parse the SQL into a AST
            try:
                self._ast = sqloxide.parse_sql(sql, dialect="mysql")
                # MySQL Dialect allows identifiers to be delimited with ` (backticks) and
                # identifiers to start with _ (underscore) and $ (dollar sign)
                # https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/dialect/mysql.rs
            except ValueError as exception:  # pragma: no cover
                raise SqlError from exception
        else:
            self._ast = ast

        # build a plan for the query
        if "Query" in self._ast[0]:
            self._naive_select_planner(self._ast, self._statistics)
        elif "Explain" in self._ast[0]:
            self._explain_planner(self._ast, self._statistics)
        elif "ShowColumns" in self._ast[0]:
            self._show_columns_planner(self._ast, self._statistics)
        else:  # pragma: no cover
            raise SqlError("Unknown or unsupported Query type.")

    def _build_literal_node(self, value):
        """
        extract values from a value node in the AST and create a ExpressionNode for it
        """
        if value is None or value in ("None", "Null"):
            return ExpressionTreeNode(NodeType.LITERAL_NONE)
        if "SingleQuotedString" in value:
            # quoted strings are either VARCHAR or TIMESTAMP
            str_value = value["SingleQuotedString"]
            dte_value = dates.parse_iso(str_value)
            if dte_value:
                return ExpressionTreeNode(NodeType.LITERAL_TIMESTAMP, value=dte_value)
            #            ISO_8601 = r"^\d{4}(-\d\d(-\d\d([T\W]\d\d:\d\d(:\d\d)?(\.\d+)?(([+-]\d\d:\d\d)|Z)?)?)?)?$"
            #            if re.match(ISO_8601, str_value):
            #                return (numpy.datetime64(str_value), TOKEN_TYPES.TIMESTAMP)
            return ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=str_value)
        if "Number" in value:
            # we have one internal numeric type
            return ExpressionTreeNode(
                NodeType.LITERAL_NUMERIC, value=numpy.float64(value["Number"][0])
            )
        if "Boolean" in value:
            return ExpressionTreeNode(NodeType.LITERAL_BOOLEAN, value=value["Boolean"])
        if "Tuple" in value:
            return ExpressionTreeNode(
                NodeType.LITERAL_LIST,
                value=[
                    self._build_literal_node(t["Value"]).value for t in value["Tuple"]
                ],
            )
        if "Value" in value:
            print("VALUE")
            if value["Value"] == "Null":
                return ExpressionTreeNode(NodeType.LITERAL_NONE)
            return ExpressionTreeNode(NodeType.UNKNOWN, value=value["Value"])

    def _build_filters(self, filters):

        # None is None
        if filters is None:
            return None

        if filters == "Wildcard":
            return ExpressionTreeNode(NodeType.WILDCARD)

        if not isinstance(filters, dict):
            return filters

        if "Identifier" in filters:  # we're an identifier
            return ExpressionTreeNode(
                NodeType.IDENTIFIER, value=filters["Identifier"]["value"]
            )
        if "CompoundIdentifier" in filters:
            return ExpressionTreeNode(
                NodeType.IDENTIFIER,
                value=".".join(i["value"] for i in filters["CompoundIdentifier"]),
            )
        if "Value" in filters:  # we're a literal
            return self._build_literal_node(filters["Value"])
        if "BinaryOp" in filters:
            left_node = self._build_filters(filters["BinaryOp"]["left"])
            right_node = self._build_filters(filters["BinaryOp"]["right"])
            operator = filters["BinaryOp"]["op"]

            node_type = operator_type_factory(operator)

            return ExpressionTreeNode(
                node_type,
                value=operator,
                left_node=left_node,
                right_node=right_node,
            )

        if "UnaryOp" in filters:
            if filters["UnaryOp"]["op"] == "Not":
                right = self._build_filters(filters["UnaryOp"]["expr"])
                return ExpressionTreeNode(token_type=NodeType.NOT, centre_node=right)
            if filters["UnaryOp"]["op"] == "Minus":
                number = 0 - numpy.float64(
                    filters["UnaryOp"]["expr"]["Value"]["Number"][0]
                )
                return ExpressionTreeNode(NodeType.LITERAL_NUMERIC, value=number)
        if "Between" in filters:
            expr = self._build_filters(filters["Between"]["expr"])
            low = self._build_filters(filters["Between"]["low"])
            high = self._build_filters(filters["Between"]["high"])
            inverted = filters["Between"]["negated"]

            if inverted:
                # LEFT <= LOW AND LEFT >= HIGH (not between)
                left_node = ExpressionTreeNode(
                    NodeType.COMPARISON_OPERATOR,
                    value="Lt",
                    left_node=expr,
                    right_node=low,
                )
                right_node = ExpressionTreeNode(
                    NodeType.COMPARISON_OPERATOR,
                    value="Gt",
                    left_node=expr,
                    right_node=high,
                )
            else:
                # LEFT > LOW and LEFT < HIGH (between)
                left_node = ExpressionTreeNode(
                    NodeType.COMPARISON_OPERATOR,
                    value="GtEq",
                    left_node=expr,
                    right_node=low,
                )
                right_node = ExpressionTreeNode(
                    NodeType.COMPARISON_OPERATOR,
                    value="LtEq",
                    left_node=expr,
                    right_node=high,
                )

            return ExpressionTreeNode(
                NodeType.AND, left_node=left_node, right_node=right_node
            )

        if "InSubquery" in filters:
            # if it's a sub-query we create a plan for it
            left = self._build_filters(filters["InSubquery"]["expr"])
            ast = {}
            ast["Query"] = filters["InSubquery"]["subquery"]
            subquery_plan = self.copy()
            subquery_plan.create_plan(ast=[ast])
            operator = "NotInList" if filters["InSubquery"]["negated"] else "InList"
            return (left, operator, (subquery_plan, TOKEN_TYPES.QUERY_PLAN))
        try_unary_filter = list(filters.keys())[0]
        if try_unary_filter in ("IsTrue", "IsFalse", "IsNull", "IsNotNull"):
            right = self._build_filters(filters[try_unary_filter])
            return (try_unary_filter, right)
        if "InList" in filters:
            left_node = self._build_filters(filters["InList"]["expr"])
            list_values = {
                self._build_filters(v).value for v in filters["InList"]["list"]
            }
            operator = "NotInList" if filters["InList"]["negated"] else "InList"
            right_node = ExpressionTreeNode(
                token_type=NodeType.LITERAL_LIST, value=list_values
            )
            return ExpressionTreeNode(
                token_type=NodeType.COMPARISON_OPERATOR,
                value=operator,
                left_node=left_node,
                right_node=right_node,
            )
        if "Function" in filters:
            func = filters["Function"]["name"][0]["value"].upper()
            args = [self._build_filters(a) for a in filters["Function"]["args"]]
            if is_function(func):
                node_type = NodeType.FUNCTION
            else:
                node_type = NodeType.AGGREGATOR
            return ExpressionTreeNode(token_type=node_type, value=func, parameters=args)
        if "Unnamed" in filters:
            return self._build_filters(filters["Unnamed"])
        if "Expr" in filters:
            return self._build_filters(filters["Expr"])
        if "Nested" in filters:
            return ExpressionTreeNode(
                token_type=NodeType.NESTED,
                centre_node=self._build_filters(filters["Nested"]),
            )
        if "MapAccess" in filters:
            # Identifier[key] -> GET(Identifier, key) -> alias of I[k] or alias
            identifier = filters["MapAccess"]["column"]["Identifier"]["value"]
            key_dict = filters["MapAccess"]["keys"][0]["Value"]
            if "SingleQuotedString" in key_dict:
                key = key_dict["SingleQuotedString"]
                key_node = ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=key)
            if "Number" in key_dict:
                key = key_dict["Number"][0]
                key_node = ExpressionTreeNode(NodeType.LITERAL_NUMERIC, value=key)
            alias = [f"{identifier}[{key}]"]

            identifier_node = ExpressionTreeNode(NodeType.IDENTIFIER, value=identifier)
            return ExpressionTreeNode(
                NodeType.FUNCTION,
                value="GET",
                parameters=[identifier_node, key_node],
                alias=alias,
            )
        if "Tuple" in filters:
            return ExpressionTreeNode(
                NodeType.LITERAL_LIST,
                value=[
                    self._build_literal_node(t["Value"]).value for t in filters["Tuple"]
                ],
            )

    def _check_hints(self, hints):

        from opteryx.third_party.mbleven import compare

        well_known_hints = ("NO_CACHE", "NO_PARTITION", "NO_PUSH_PROJECTION")

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
                    self._statistics.warn(
                        f"Hint `{hint}` is not recognized, did you mean `{best_match_hint}`?"
                    )
                else:
                    self._statistics.warn(f"Hint `{hint}` is not recognized.")

    def _extract_relations(self, ast, default_path: bool = True):
        """ """

        def _safe_get(iterable, index):
            try:
                return iterable[index]
            except IndexError:
                return None

        relations = ast
        if default_path:
            try:
                relations = ast[0]["Query"]["body"]["Select"]["from"]
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
                        self._build_filters(a["Unnamed"])
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
                        self._check_hints(hints)
                    dataset = ".".join(
                        [
                            part["value"]
                            for part in relation["relation"]["Table"]["name"]
                        ]
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
                    subquery_plan = self.copy()
                    subquery_plan.create_plan(ast=[ast])

                    yield (alias, subquery_plan, "SubQuery", [])
                if "Values" in subquery:
                    body = []
                    headers = [
                        h["value"]
                        for h in relation["relation"]["Derived"]["alias"]["columns"]
                    ]
                    for value_set in subquery["Values"]:
                        values = [
                            _safe_get(self._build_literal_node(v["Value"]), 0)
                            for v in value_set
                        ]
                        body.append(dict(zip(headers, values)))
                    yield (alias, {"function": "values", "args": body}, "Function", [])

    def _extract_joins(self, ast):
        try:
            joins = ast[0]["Query"]["body"]["Select"]["from"][0]["joins"]
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
                    join_on = self._build_filters(
                        join["join_operator"][join_mode]["On"]
                    )

            right = next(self._extract_relations([join], default_path=False))
            yield (join_mode, right, join_on, join_using)

    def _extract_field_list(self, projection):
        """
        Projections are lists of attributes, the most obvious one is in the SELECT
        statement but they can exist elsewhere to limit the amount of data
        processed at each step.
        """
        if projection == ["Wildcard"]:
            return [ExpressionTreeNode(token_type=NodeType.WILDCARD)]

        def _inner(attribute):
            function = None
            alias = []

            # get any alias information for a field (usually means we're in a SELECT clause)
            if "UnnamedExpr" in attribute:
                function = attribute["UnnamedExpr"]
            if "ExprWithAlias" in attribute:
                function = attribute["ExprWithAlias"]["expr"]
                alias = [attribute["ExprWithAlias"]["alias"]["value"]]
            if "QualifiedWildcard" in attribute:
                return ExpressionTreeNode(
                    NodeType.WILDCARD, value=attribute["QualifiedWildcard"][0]["value"]
                )
            if function is None:
                function = attribute

            if "Identifier" in function:
                return ExpressionTreeNode(
                    token_type=NodeType.IDENTIFIER,
                    value=function["Identifier"]["value"],
                    alias=alias,
                )
            if "CompoundIdentifier" in function:
                return ExpressionTreeNode(
                    token_type=NodeType.IDENTIFIER,
                    value=".".join(p["value"] for p in function["CompoundIdentifier"]),
                    alias=".".join(p["value"] for p in function["CompoundIdentifier"]),
                )
            if "Function" in function:
                func = function["Function"]["name"][0]["value"].upper()
                args = [self._build_filters(a) for a in function["Function"]["args"]]
                if is_function(func):
                    node_type = NodeType.FUNCTION
                else:
                    node_type = NodeType.AGGREGATOR
                return ExpressionTreeNode(
                    token_type=node_type, value=func, parameters=args, alias=alias
                )
            if "BinaryOp" in function:
                left = self._build_filters(function["BinaryOp"]["left"])
                operator = function["BinaryOp"]["op"]
                right = self._build_filters(function["BinaryOp"]["right"])

                raise Exception("IS THIS EVER CALLED?")
            if "Cast" in function:
                # CAST(<var> AS <type>) - convert to the form <type>(var), e.g. BOOLEAN(on)
                args = [self._build_filters(function["Cast"]["expr"])]
                data_type = function["Cast"]["data_type"]
                if data_type == "Timestamp":
                    data_type = "TIMESTAMP"
                elif "Varchar" in data_type:
                    data_type = "VARCHAR"
                elif "Decimal" in data_type:
                    data_type = "NUMERIC"
                elif "Boolean" in data_type:
                    data_type = "BOOLEAN"
                else:
                    raise SqlError(f"Unsupported type for CAST  - '{data_type}'")

                alias.append(f"CAST({args[0].value} AS {data_type})")

                return ExpressionTreeNode(
                    NodeType.FUNCTION,
                    value=data_type.upper(),
                    parameters=args,
                    alias=alias,
                )

            if "TryCast" in function:
                # CAST(<var> AS <type>) - convert to the form <type>(var), e.g. BOOLEAN(on)
                args = [self._build_filters(function["TryCast"]["expr"])]
                data_type = function["TryCast"]["data_type"]
                if data_type == "Timestamp":
                    data_type = "TIMESTAMP"
                elif "Varchar" in data_type:
                    data_type = "VARCHAR"
                elif "Decimal" in data_type:
                    data_type = "NUMERIC"
                elif "Boolean" in data_type:
                    data_type = "BOOLEAN"
                else:
                    raise SqlError(f"Unsupported type for TRY_CAST  - '{data_type}'")

                alias.append(f"TRY_CAST({args[0].value} AS {data_type})")

                return ExpressionTreeNode(
                    NodeType.FUNCTION,
                    value=f"TRY_{data_type.upper()}",
                    parameters=args,
                    alias=alias,
                )

            if "Extract" in function:
                # EXTRACT(part FROM timestamp)

                datepart = (
                    function["Extract"]["field"],
                    TOKEN_TYPES.INTERVAL,
                )
                value = self._build_filters(function["Extract"]["expr"])

                alias.append(f"EXTRACT({datepart[0]} FROM {value[0]})")
                alias.append(f"DATEPART({datepart[0]}, {value[0]}")

                return {
                    "function": "DATEPART",
                    "args": (
                        datepart,
                        value,
                    ),
                    "alias": alias,
                }

            if "MapAccess" in function:
                # Identifier[key] -> GET(Identifier, key) -> alias of I[k] or alias
                identifier = function["MapAccess"]["column"]["Identifier"]["value"]
                key_dict = function["MapAccess"]["keys"][0]["Value"]
                if "SingleQuotedString" in key_dict:
                    key = key_dict["SingleQuotedString"]
                    key_node = ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=key)
                if "Number" in key_dict:
                    key = key_dict["Number"][0]
                    key_node = ExpressionTreeNode(NodeType.LITERAL_NUMERIC, value=key)
                alias.append(f"{identifier}[{key}]")

                identifier_node = ExpressionTreeNode(
                    NodeType.IDENTIFIER, value=identifier
                )
                return ExpressionTreeNode(
                    NodeType.FUNCTION,
                    value="GET",
                    parameters=[identifier_node, key_node],
                    alias=alias,
                )
            if "Value" in function:
                return self._build_literal_node(function["Value"])

        projection = [_inner(attribute) for attribute in projection]
        return projection

    def _extract_selection(self, ast):
        """
        Although there is a SELECT statement in a SQL Query, Selection refers to the
        filter or WHERE statement.
        """
        selections = ast[0]["Query"]["body"]["Select"]["selection"]
        return self._build_filters(selections)

    def _extract_filter(self, ast):
        """ """
        filters = ast[0]["ShowColumns"]["filter"]
        if filters is None:
            return None
        if "Where" in filters:
            return self._build_filters(filters["Where"])
        if "Like" in filters:
            left = ExpressionTreeNode(NodeType.IDENTIFIER, value="column_name")
            right = ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=filters["Like"])
            root = ExpressionTreeNode(
                NodeType.COMPARISON_OPERATOR,
                value="Like",
                left_node=left,
                right_node=right,
            )
            return root

    def _extract_distinct(self, ast):
        return ast[0]["Query"]["body"]["Select"]["distinct"]

    def _extract_limit(self, ast):
        limit = ast[0]["Query"].get("limit")
        if limit is not None:
            return int(limit["Value"]["Number"][0])
        return None

    def _extract_offset(self, ast):
        offset = ast[0]["Query"].get("offset")
        if offset is not None:
            return int(offset["value"]["Value"]["Number"][0])
        return None

    def _extract_order(self, ast):
        order = ast[0]["Query"].get("order_by")
        if order is not None:
            orders = []
            for col in order:
                column = self._extract_field_list([col["expr"]])
                orders.append(
                    (
                        column,
                        "descending" if str(col["asc"]) == "False" else "ascending",
                    ),
                )
            return orders

    def _extract_having(self, ast):
        having = ast[0]["Query"]["body"]["Select"]["having"]
        return self._build_filters(having)

    def _extract_directives(self, ast):
        return QueryDirectives()

    def _explain_planner(self, ast, statistics):
        directives = self._extract_directives(ast)
        explain_plan = self.copy()
        explain_plan.create_plan(ast=[ast[0]["Explain"]["statement"]])
        explain_node = operations.ExplainNode(
            directives, statistics, query_plan=explain_plan
        )
        self.add_operator("explain", explain_node)

    def _show_columns_planner(self, ast, statistics):

        directives = self._extract_directives(ast)

        dataset = ".".join(
            [part["value"] for part in ast[0]["ShowColumns"]["table_name"]]
        )

        if dataset[0:1] == "$":
            mode = "Internal"
            reader = None
        else:
            reader = get_adapter(dataset)
            mode = reader.__mode__

        self.add_operator(
            "reader",
            operations.reader_factory(mode)(
                directives=directives,
                statistics=statistics,
                dataset=dataset,
                alias=None,
                reader=reader,
                cache=None,  # never read from cache
                start_date=self.start_date,
                end_date=self.end_date,
            ),
        )
        last_node = "reader"

        filters = self._extract_filter(ast)
        if filters:
            self.add_operator(
                "filter",
                operations.ColumnSelectionNode(
                    directives=directives, statistics=statistics, filter=filters
                ),
            )
            self.link_operators(last_node, "filter")
            last_node = "filter"

        self.add_operator(
            "columns",
            operations.ShowColumnsNode(
                directives=directives,
                statistics=statistics,
                full=ast[0]["ShowColumns"]["full"],
                extended=ast[0]["ShowColumns"]["extended"],
            ),
        )
        self.link_operators(last_node, "columns")
        last_node = "columns"

    def _naive_select_planner(self, ast, statistics):
        """
        The naive planner only works on single tables and always puts operations in
        this order.

            FROM clause
            WHERE clause
            AGGREGATE (GROUP BY clause)
            HAVING clause
            SELECT clause
            DISTINCT
            ORDER BY clause
            LIMIT clause
            OFFSET clause

        This is phase one of the rewrite, to essentially mimick the existing
        functionality.
        """
        directives = self._extract_directives(ast)

        # TODO [#196]: move all information collection upfront so we can identify all
        # the identifiers for selection pushdown. is parameter 'selection' for the
        # reader
        # all_identifiers = get_all_identifiers(self._groups)

        _relations = [r for r in self._extract_relations(ast)]
        if len(_relations) == 0:
            _relations = [(None, "$no_table", "Internal", [])]

        # We always have a data source - even if it's 'no table'
        alias, dataset, mode, hints = _relations[0]

        # external comes in different flavours
        reader = None
        if mode == "External":
            reader = get_adapter(dataset)
            mode = reader.__mode__

        self.add_operator(
            "from",
            operations.reader_factory(mode)(
                directives=directives,
                statistics=statistics,
                alias=alias,
                dataset=dataset,
                reader=reader,
                cache=self._cache,
                start_date=self.start_date,
                end_date=self.end_date,
                hints=hints,
            ),
        )
        last_node = "from"

        _joins = list(self._extract_joins(ast))
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
                    elif dataset[0:1] == "$":
                        mode = "Internal"
                        reader = None
                    else:
                        reader = get_adapter(dataset)
                        mode = reader.__mode__

                    # Otherwise, the right table needs to come from the Reader
                    right = operations.reader_factory(mode)(
                        directives=directives,
                        statistics=statistics,
                        dataset=dataset,
                        alias=right[0],
                        reader=reader,
                        cache=self._cache,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        hints=right[3],
                    )

                join_node = operations.join_factory(join_type)
                if join_node is None:
                    raise SqlError(f"Join type not supported - `{_join[0]}`")

                self.add_operator(
                    f"join-{join_id}",
                    join_node(
                        directives=directives,
                        statistics=statistics,
                        join_type=join_type,
                        join_on=join_on,
                        join_using=join_using,
                    ),
                )
                self.link_operators(last_node, f"join-{join_id}")

                self.add_operator(f"join-{join_id}-right", right)
                self.link_operators(f"join-{join_id}-right", f"join-{join_id}", "right")

                last_node = f"join-{join_id}"

        _selection = self._extract_selection(ast)
        if _selection:
            self.add_operator(
                "where",
                operations.SelectionNode(directives, statistics, filter=_selection),
            )
            self.link_operators(last_node, "where")
            last_node = "where"

        _projection = self._extract_field_list(
            ast[0]["Query"]["body"]["Select"]["projection"]
        )
        _groups = self._extract_field_list(
            ast[0]["Query"]["body"]["Select"]["group_by"]
        )
        if _groups or any(
            a.token_type == NodeType.AGGREGATOR
            for a in _projection
            if isinstance(a, ExpressionTreeNode)
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
            self.add_operator(
                "agg",
                operations.AggregateNode(
                    directives, statistics, aggregates=_aggregates, groups=_groups
                ),
            )
            self.link_operators(last_node, "agg")
            last_node = "agg"

        _having = self._extract_having(ast)
        if _having:
            self.add_operator(
                "having",
                operations.SelectionNode(directives, statistics, filter=_having),
            )
            self.link_operators(last_node, "having")
            last_node = "having"

        _projection = self._extract_field_list(
            ast[0]["Query"]["body"]["Select"]["projection"]
        )
        if _projection[0].token_type != NodeType.WILDCARD:
            self.add_operator(
                "select",
                operations.ProjectionNode(
                    directives, statistics, projection=_projection
                ),
            )
            self.link_operators(last_node, "select")
            last_node = "select"

        _distinct = self._extract_distinct(ast)
        if _distinct:
            self.add_operator(
                "distinct", operations.DistinctNode(directives, statistics)
            )
            self.link_operators(last_node, "distinct")
            last_node = "distinct"

        _order = self._extract_order(ast)
        if _order:
            self.add_operator(
                "order", operations.SortNode(directives, statistics, order=_order)
            )
            self.link_operators(last_node, "order")
            last_node = "order"

        _offset = self._extract_offset(ast)
        if _offset:
            self.add_operator(
                "offset", operations.OffsetNode(directives, statistics, offset=_offset)
            )
            self.link_operators(last_node, "offset")
            last_node = "offset"

        _limit = self._extract_limit(ast)
        # 0 limit is valid
        if _limit is not None:
            self.add_operator(
                "limit", operations.LimitNode(directives, statistics, limit=_limit)
            )
            self.link_operators(last_node, "limit")
            last_node = "limit"

    def explain(self):
        def _inner_explain(node, depth):
            if depth == 1:
                operator = self.get_operator(node)
                yield {
                    "operator": operator.name,
                    "config": operator.config,
                    "depth": depth - 1,
                }
            incoming_operators = self.get_incoming_links(node)
            for operator_name in incoming_operators:
                operator = self.get_operator(operator_name[0])
                if isinstance(operator, operations.BasePlanNode):
                    yield {
                        "operator": operator.name,
                        "config": operator.config,
                        "depth": depth,
                    }
                yield from _inner_explain(operator_name[0], depth + 1)

        head = self.get_exit_points()
        # print(head, self._edges)
        if len(head) != 1:
            raise SqlError(f"Problem with the plan - it has {len(head)} heads.")
        plan = list(_inner_explain(head[0], 1))

        table = pyarrow.Table.from_pylist(plan)
        table = Columns.create_table_metadata(table, table.num_rows, "plan", None)
        yield table

    #    def __repr__(self):
    #        return "\n".join(list(self._draw()))

    def _inner(self, nodes):
        for node in nodes:
            producers = self.get_incoming_links(node)

            # print(node, producers)
            operator = self.get_operator(node)
            if producers:
                operator.set_producers([self.get_operator(i[0]) for i in producers])
                self._inner(i[0] for i in producers)

    def execute(self):
        # we get the tail of the query - the first steps
        head = list(set(self.get_exit_points()))
        # print(head, self._edges)
        if len(head) != 1:
            raise SqlError(
                f"Problem with the plan - it has {len(head)} heads, this is quite unexpected."
            )
        self._inner(head)

        operator = self.get_operator(head[0])
        yield from operator.execute()
