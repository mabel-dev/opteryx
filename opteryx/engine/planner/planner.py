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

from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.engine.functions import is_function
from opteryx.engine.planner.execution_tree import ExecutionTree
from opteryx.engine.planner.operations import *
from opteryx.engine.planner.temporal import extract_temporal_filters
from opteryx.engine.query_directives import QueryDirectives
from opteryx.exceptions import SqlError
from opteryx.utils import dates

OPERATOR_XLAT = {
    "Eq": "=",
    "NotEq": "<>",
    "Gt": ">",
    "GtEq": ">=",
    "Lt": "<",
    "LtEq": "<=",
    "Like": "like",
    "ILike": "ilike",
    "NotLike": "not like",
    "NotILike": "not ilike",
    "InList": "in",
    "PGRegexMatch": "~",
}


class QueryPlanner(ExecutionTree):
    def __init__(self, statistics, reader, cache, partition_scheme):
        """
        Planner creates a plan (Execution Tree or DAG) which presents the plan to
        respond to the query.
        """
        super().__init__()

        self._ast = None

        self._statistics = statistics
        self._directives = QueryDirectives()
        self._reader = reader
        self._cache = cache
        self._partition_scheme = partition_scheme

        self._start_date = datetime.datetime.utcnow().date()
        self._end_date = datetime.datetime.utcnow().date()

    def __repr__(self):
        return "QueryPlanner"

    def copy(self):
        planner = QueryPlanner(
            statistics=self._statistics,
            reader=self._reader,
            cache=self._cache,
            partition_scheme=self._partition_scheme,
        )
        planner._start_date = self._start_date
        planner._end_date = self._end_date
        return planner

    def create_plan(self, sql: str = None, ast: dict = None):

        if sql:
            import sqloxide

            # extract temporal filters, this isn't supported by sqloxide
            self._start_date, self._end_date, sql = extract_temporal_filters(sql)
            # Parse the SQL into a AST
            try:
                self._ast = sqloxide.parse_sql(sql, dialect="mysql")
                # MySQL Dialect allows identifiers to be delimited with ` (backticks) and
                # identifiers to start with _ (underscore) and $ (dollar sign)
                # https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/dialect/mysql.rs
            except ValueError as exception:  # pragma: no cover
                raise SqlError(exception)
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

    def _extract_value(self, value):
        """
        extract values from a value node
        """
        if value is None:
            return (None, None)
        if "SingleQuotedString" in value:
            # quoted strings are either VARCHAR or TIMESTAMP
            str_value = value["SingleQuotedString"]
            dte_value = dates.parse_iso(str_value)
            if dte_value:
                return (dte_value, TOKEN_TYPES.TIMESTAMP)
            return (str_value, TOKEN_TYPES.VARCHAR)
        if "Number" in value:
            # we have one internal numeric type
            return (numpy.float64(value["Number"][0]), TOKEN_TYPES.NUMERIC)
        if "Boolean" in value:
            return (value["Boolean"], TOKEN_TYPES.BOOLEAN)
        if "Tuple" in value:
            return (
                [self._extract_value(t["Value"])[0] for t in value["Tuple"]],
                TOKEN_TYPES.LIST,
            )

    def _build_dnf_filters(self, filters):

        # None is None
        if filters is None:
            return None

        if "Identifier" in filters:  # we're an identifier
            return (filters["Identifier"]["value"], TOKEN_TYPES.IDENTIFIER)
        if "CompoundIdentifier" in filters:
            return (
                ".".join([i["value"] for i in filters["CompoundIdentifier"]]),
                TOKEN_TYPES.IDENTIFIER,
            )
        if "Value" in filters:  # we're a literal
            return self._extract_value(filters["Value"])
        if "BinaryOp" in filters:
            left = self._build_dnf_filters(filters["BinaryOp"]["left"])
            operator = filters["BinaryOp"]["op"]
            right = self._build_dnf_filters(filters["BinaryOp"]["right"])

            if operator in ("And"):
                if isinstance(left, list):
                    left.append(right)
                    return left
                if isinstance(right, list):
                    right.append(left)
                    return right
                return [left, right]
            if operator in ("Or"):
                return [[left], [right]]
            return (left, OPERATOR_XLAT[operator], right)
        if "UnaryOp" in filters:
            if filters["UnaryOp"]["op"] == "Not":
                right = self._build_dnf_filters(filters["UnaryOp"]["expr"])
                return ("NOT", right)
            if filters["UnaryOp"]["op"] == "Minus":
                number = 0 - numpy.float64(
                    filters["UnaryOp"]["expr"]["Value"]["Number"][0]
                )
                return (number, TOKEN_TYPES.NUMERIC)
        if "Between" in filters:
            left = self._build_dnf_filters(filters["Between"]["expr"])
            low = self._build_dnf_filters(filters["Between"]["low"])
            high = self._build_dnf_filters(filters["Between"]["high"])
            inverted = filters["Between"]["negated"]

            if inverted:
                # LEFT <= LOW AND LEFT >= HIGH (not between)
                return [[(left, "<", low)], [(left, ">", high)]]
            # LEFT > LOW and LEFT < HIGH (between)
            return [(left, ">=", low), (left, "<=", high)]
        if "InSubquery" in filters:
            # if it's a sub-query we create a plan for it
            left = self._build_dnf_filters(filters["InSubquery"]["expr"])
            ast = {}
            ast["Query"] = filters["InSubquery"]["subquery"]
            subquery_plan = self.copy()
            subquery_plan.create_plan(ast=[ast])
            operator = "not in" if filters["InSubquery"]["negated"] else "in"
            return (left, operator, (subquery_plan, TOKEN_TYPES.QUERY_PLAN))
        if "IsNull" in filters:
            left = self._build_dnf_filters(filters["IsNull"])
            return (left, "=", None)
        if "IsNotNull" in filters:
            left = self._build_dnf_filters(filters["IsNotNull"])
            return (left, "<>", None)
        if "InList" in filters:
            left = self._build_dnf_filters(filters["InList"]["expr"])
            right = (
                [self._build_dnf_filters(v)[0] for v in filters["InList"]["list"]],
                TOKEN_TYPES.LIST,
            )
            operator = "not in" if filters["InList"]["negated"] else "in"
            return (left, operator, right)
        if filters == "Wildcard":
            return ("Wildcard", TOKEN_TYPES.WILDCARD)
        if "Function" in filters:
            func = filters["Function"]["name"][0]["value"].upper()
            args = [
                self._build_dnf_filters(a["Unnamed"])
                for a in filters["Function"]["args"]
            ]
            select_args = [(str(a[0]) if a[0] != "Wildcard" else "*") for a in args]
            select_args = [
                ((f"({','.join(a[0])})",) if isinstance(a[0], list) else a)
                for a in select_args
            ]
            column_name = f"{func}({','.join(select_args)})"
            # we pass the function definition so if needed we can execute the function
            return (
                column_name,
                TOKEN_TYPES.IDENTIFIER,
                {"function": func, "args": args},
            )
        if "Unnamed" in filters:
            return self._build_dnf_filters(filters["Unnamed"])
        if "Expr" in filters:
            return self._build_dnf_filters(filters["Expr"])
        if "Nested" in filters:
            return (self._build_dnf_filters(filters["Nested"]),)
        if "MapAccess" in filters:
            identifier = filters["MapAccess"]["column"]["Identifier"]["value"]
            key_dict = filters["MapAccess"]["keys"][0]["Value"]
            if "SingleQuotedString" in key_dict:
                key = f"'{key_dict['SingleQuotedString']}'"
            if "Number" in key_dict:
                key = key_dict["Number"][0]
            return (f"{identifier}[{key}]", TOKEN_TYPES.IDENTIFIER)
        if "Tuple" in filters:
            return (
                [self._extract_value(t["Value"])[0] for t in filters["Tuple"]],
                TOKEN_TYPES.LIST,
            )

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
                if len(relation["relation"]["Table"]["args"]) > 0:
                    function = relation["relation"]["Table"]["name"][0]["value"].lower()
                    alias = function
                    if relation["relation"]["Table"]["alias"] is not None:
                        alias = relation["relation"]["Table"]["alias"]["name"]["value"]
                    args = [
                        self._build_dnf_filters(a["Unnamed"])
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
                            _safe_get(self._extract_value(v["Value"]), 0)
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
                    join_on = self._build_dnf_filters(
                        join["join_operator"][join_mode]["On"]
                    )

            right = next(self._extract_relations([join], default_path=False))
            yield (join_mode, right, join_on, join_using)

    def _extract_projections(self, ast):
        """
        Projections are lists of attributes, the most obvious one is in the SELECT
        statement but they can exist elsewhere to limit the amount of data
        processed at each step.
        """
        projection = ast[0]["Query"]["body"]["Select"]["projection"]
        # print(projection)
        if projection == ["Wildcard"]:
            return {"*": "*"}

        def _inner(attribute):
            function = None
            alias = []
            if "UnnamedExpr" in attribute:
                function = attribute["UnnamedExpr"]
            if "ExprWithAlias" in attribute:
                function = attribute["ExprWithAlias"]["expr"]
                alias = [attribute["ExprWithAlias"]["alias"]["value"]]
            if "QualifiedWildcard" in attribute:
                return {"*": attribute["QualifiedWildcard"][0]["value"]}

            if function:

                if "Identifier" in function:
                    return {
                        "identifier": function["Identifier"]["value"],
                        "alias": alias,
                    }
                if "CompoundIdentifier" in function:
                    return {
                        "identifier": [
                            ".".join(
                                [p["value"] for p in function["CompoundIdentifier"]]
                            )
                        ].pop(),
                        "alias": [
                            ".".join(
                                [p["value"] for p in function["CompoundIdentifier"]]
                            )
                        ],
                    }
                if "Function" in function:
                    func = function["Function"]["name"][0]["value"].upper()
                    args = [
                        self._build_dnf_filters(a) for a in function["Function"]["args"]
                    ]
                    if is_function(func):
                        return {"function": func, "args": args, "alias": alias}
                    return {"aggregate": func, "args": args, "alias": alias}
                if "BinaryOp" in function:
                    raise NotImplementedError(
                        "Operations in the SELECT clause are not supported"
                    )
                if "Cast" in function:
                    # CAST(<var> AS <type>) - convert to the form <type>(var), e.g. BOOLEAN(on)
                    args = [self._build_dnf_filters(function["Cast"]["expr"])]
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
                        raise SqlError("Unsupported CAST function")

                    alias.append(f"CAST({args[0][0]} AS {data_type})")

                    return {"function": data_type, "args": args, "alias": alias}
                if "MapAccess" in function:
                    # Identifier[key] -> GET(Identifier, key) -> alias of I[k] or alias
                    identifier = function["MapAccess"]["column"]["Identifier"]["value"]
                    key_dict = function["MapAccess"]["keys"][0]["Value"]
                    if "SingleQuotedString" in key_dict:
                        key_value = (
                            key_dict["SingleQuotedString"],
                            TOKEN_TYPES.VARCHAR,
                        )
                        key = f"'{key_dict['SingleQuotedString']}'"
                    if "Number" in key_dict:
                        key_value = (
                            int(key_dict["Number"][0]),
                            TOKEN_TYPES.NUMERIC,
                        )
                        key = key_dict["Number"][0]
                    alias.append(f"{identifier}[{key}]")

                    return {
                        "function": "GET",
                        "args": [(identifier, TOKEN_TYPES.IDENTIFIER), key_value],
                        "alias": alias,
                    }

        projection = [_inner(attribute) for attribute in projection]
        # print(projection)
        return projection

    def _extract_selection(self, ast):
        """
        Although there is a SELECT statement in a SQL Query, Selection refers to the
        filter or WHERE statement.
        """
        selections = ast[0]["Query"]["body"]["Select"]["selection"]
        return self._build_dnf_filters(selections)

    def _extract_filter(self, ast):
        """ """
        filters = ast[0]["ShowColumns"]["filter"]
        if filters is None:
            return None
        if "Where" in filters:
            return self._build_dnf_filters(filters["Where"])
        if "Like" in filters:
            return (
                (
                    "column_name",
                    TOKEN_TYPES.IDENTIFIER,
                ),
                "like",
                (filters["Like"], TOKEN_TYPES.VARCHAR),
            )

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
                column = col["expr"]
                if "Identifier" in column:
                    column = column["Identifier"]["value"]
                if "CompoundIdentifier" in column:
                    column = ".".join(
                        [i["value"] for i in column["CompoundIdentifier"]]
                    )
                if "Function" in column:
                    func = column["Function"]["name"][0]["value"].upper()
                    args = [
                        self._build_dnf_filters(a)[0]
                        for a in column["Function"]["args"]
                    ]
                    args = ["*" if i == "Wildcard" else i for i in args]
                    args = [
                        ((f"({','.join(a[0])})",) if isinstance(a[0], list) else a)
                        for a in args
                    ]
                    alias = f"{func.upper()}({','.join([str(a[0]) for a in args])})"
                    column = {"function": func, "args": args, "alias": alias}
                orders.append(
                    (
                        column,
                        "descending" if str(col["asc"]) == "False" else "ascending",
                    ),
                )
            return orders

    def _extract_groups(self, ast):
        def _inner(element):
            if element:
                if "Identifier" in element:
                    return element["Identifier"]["value"]
                if "Function" in element:
                    func = element["Function"]["name"][0]["value"].upper()
                    args = [
                        self._build_dnf_filters(a) for a in element["Function"]["args"]
                    ]
                    args = [
                        ((f"({','.join(a[0])})",) if isinstance(a[0], list) else a)
                        for a in args
                    ]
                    return f"{func.upper()}({','.join([str(a[0]) for a in args])})"
                if "Cast" in element:
                    args = [self._build_dnf_filters(element["Cast"]["expr"])]
                    data_type = list(element["Cast"]["data_type"].keys())[0]
                    return f"CAST({args[0][0]} AS {str(data_type).upper()})"
                if "MapAccess" in element:
                    identifier = element["MapAccess"]["column"]["Identifier"]["value"]
                    key_dict = element["MapAccess"]["keys"][0]["Value"]
                    if "SingleQuotedString" in key_dict:
                        key = f"'{key_dict['SingleQuotedString']}'"
                    if "Number" in key_dict:
                        key = key_dict["Number"][0]
                    return f"{identifier}[{key}]"

        groups = ast[0]["Query"]["body"]["Select"]["group_by"]
        return [_inner(g) for g in groups]

    def _extract_having(self, ast):
        having = ast[0]["Query"]["body"]["Select"]["having"]
        return self._build_dnf_filters(having)

    def _extract_directives(self, ast):
        return QueryDirectives()

    def _explain_planner(self, ast, statistics):
        directives = self._extract_directives(ast)
        explain_plan = self.copy()
        explain_plan.create_plan(ast=[ast[0]["Explain"]["statement"]])
        explain_node = ExplainNode(directives, statistics, query_plan=explain_plan)
        self.add_operator("explain", explain_node)

    def _show_columns_planner(self, ast, statistics):

        directives = self._extract_directives(ast)

        dataset = ".".join(
            [part["value"] for part in ast[0]["ShowColumns"]["table_name"]]
        )

        if dataset[0:1] == "$":
            mode = "Internal"
        else:
            mode = "External"

        self.add_operator(
            "reader",
            reader_factory(mode)(
                directives=directives,
                statistics=statistics,
                dataset=dataset,
                alias=None,
                reader=self._reader,
                cache=None,  # never read from cache
                partition_scheme=self._partition_scheme,
                start_date=self._start_date,
                end_date=self._end_date,
            ),
        )
        last_node = "reader"

        self.add_operator(
            "columns",
            ShowColumnsNode(
                directives=directives,
                statistics=statistics,
                full=ast[0]["ShowColumns"]["full"],
                extended=ast[0]["ShowColumns"]["extended"],
            ),
        )
        self.link_operators(last_node, "columns")
        last_node = "columns"

        filters = self._extract_filter(ast)
        if filters:
            self.add_operator(
                "filter",
                SelectionNode(
                    directives=directives, statistics=statistics, filter=filters
                ),
            )
            self.link_operators(last_node, "filter")
            last_node = "filter"

    def _naive_select_planner(self, ast, statistics):
        """
        The naive planner only works on single tables and always puts operations in
        this order.

            FROM clause
            EVALUATE
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

        _relations = [r for r in self._extract_relations(ast)]
        if len(_relations) == 0:
            _relations = [(None, "$no_table", "Internal", [])]

        # We always have a data source - even if it's 'no table'
        alias, dataset, mode, hints = _relations[0]
        self.add_operator(
            "from",
            reader_factory(mode)(
                directives=directives,
                statistics=statistics,
                alias=alias,
                dataset=dataset,
                reader=self._reader,
                cache=self._cache,
                partition_scheme=self._partition_scheme,
                start_date=self._start_date,
                end_date=self._end_date,
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
                    # Otherwise, the right table needs to come from the Reader
                    right = reader_factory(right[2])(
                        directives=directives,
                        statistics=statistics,
                        dataset=right[1],
                        alias=right[0],
                        reader=self._reader,
                        cache=self._cache,
                        partition_scheme=self._partition_scheme,
                        start_date=self._start_date,
                        end_date=self._end_date,
                        hints=right[3],
                    )

                join_node = join_factory(join_type)
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

        _projection = self._extract_projections(ast)
        if any(["function" in a for a in _projection]):
            self.add_operator(
                "eval", EvaluationNode(directives, statistics, projection=_projection)
            )
            self.link_operators(last_node, "eval")
            last_node = "eval"

        _selection = self._extract_selection(ast)
        if _selection:
            self.add_operator(
                "where", SelectionNode(directives, statistics, filter=_selection)
            )
            self.link_operators(last_node, "where")
            last_node = "where"

        _groups = self._extract_groups(ast)
        if _groups or any(["aggregate" in a for a in _projection]):
            _aggregates = _projection.copy()
            if isinstance(_aggregates, dict):
                raise SqlError("GROUP BY cannot be used with SELECT *")
            if not any(["aggregate" in a for a in _aggregates]):
                _aggregates.append(
                    {
                        "aggregate": "COUNT",
                        "args": [("Wildcard", TOKEN_TYPES.WILDCARD)],
                        "alias": None,
                    }
                )
            self.add_operator(
                "agg",
                AggregateNode(
                    directives, statistics, aggregates=_aggregates, groups=_groups
                ),
            )
            self.link_operators(last_node, "agg")
            last_node = "agg"

        _having = self._extract_having(ast)
        if _having:
            self.add_operator(
                "having", SelectionNode(directives, statistics, filter=_having)
            )
            self.link_operators(last_node, "having")
            last_node = "having"

        self.add_operator(
            "select", ProjectionNode(directives, statistics, projection=_projection)
        )
        self.link_operators(last_node, "select")
        last_node = "select"

        _distinct = self._extract_distinct(ast)
        if _distinct:
            self.add_operator("distinct", DistinctNode(directives, statistics))
            self.link_operators(last_node, "distinct")
            last_node = "distinct"

        _order = self._extract_order(ast)
        if _order:
            self.add_operator("order", SortNode(directives, statistics, order=_order))
            self.link_operators(last_node, "order")
            last_node = "order"

        _offset = self._extract_offset(ast)
        if _offset:
            self.add_operator(
                "offset", OffsetNode(directives, statistics, offset=_offset)
            )
            self.link_operators(last_node, "offset")
            last_node = "offset"

        _limit = self._extract_limit(ast)
        # 0 limit is valid
        if _limit is not None:
            self.add_operator("limit", LimitNode(directives, statistics, limit=_limit))
            self.link_operators(last_node, "limit")
            last_node = "limit"

    def explain(self):

        import pyarrow
        from opteryx.utils.columns import Columns

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
                if isinstance(operator, BasePlanNode):
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
        head = self.get_exit_points()
        # print(head, self._edges)
        if len(head) != 1:
            raise SqlError(
                f"Problem with the plan - it has {len(head)} heads, this is quite unexpected."
            )
        self._inner(head)

        operator = self.get_operator(head[0])
        yield from operator.execute()
