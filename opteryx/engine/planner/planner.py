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
import numpy

from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.engine.functions import is_function
from opteryx.engine.planner.operations import *
from opteryx.engine.planner.operations.inner_join_node import InnerJoinNode
from opteryx.engine.planner.temporal import extract_temporal_filters
from opteryx.exceptions import SqlError
from opteryx.utils import dates

JSON_TYPES = {numpy.bool_: bool, numpy.int64: int, numpy.float64: float}

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


class QueryPlanner(object):
    def __init__(self, statistics, reader, cache, partition_scheme):
        """
        PLan represents Directed Acyclic Graphs which are used to describe data
        pipelines.
        """
        import datetime

        self.nodes: dict = {}
        self.edges: list = []

        self._ast = None

        self._statistics = statistics
        self._reader = reader
        self._cache = cache
        self._partition_scheme = partition_scheme

        self._start_date = datetime.datetime.today()
        self._end_date = datetime.datetime.today()

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

    def _extract_relations(self, ast):
        """ """

        def _safe_get(l, i):
            try:
                return l[i]
            except IndexError:
                return None

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
                    yield (alias, {"function": function, "args": args})  # <- function
                else:
                    alias = None
                    if relation["relation"]["Table"]["alias"] is not None:
                        alias = relation["relation"]["Table"]["alias"]["name"]["value"]
                    dataset = ".".join(
                        [
                            part["value"]
                            for part in relation["relation"]["Table"]["name"]
                        ]
                    )
                    yield (alias, dataset)

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

                    yield (alias, subquery_plan)
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
                    yield (alias, body)  # <- a literal table

    def _extract_joins(self, ast):
        try:
            joins = ast[0]["Query"]["body"]["Select"]["from"][0]["joins"]
        except IndexError:
            return None

        for join in joins:
            using = None
            join_on = None
            alias = None
            mode = join["join_operator"]
            if isinstance(mode, dict):
                mode = list(join["join_operator"].keys())[0]
                if "Using" in join["join_operator"][mode]:
                    using = [
                        v["value"] for v in join["join_operator"][mode].get("Using", [])
                    ]
                if "On" in join["join_operator"][mode]:
                    join_on = self._build_dnf_filters(join["join_operator"][mode]["On"])
            if join["relation"]["Table"]["alias"] is not None:
                alias = join["relation"]["Table"]["alias"]["name"]["value"]
            dataset = ".".join(
                [part["value"] for part in join["relation"]["Table"]["name"]]
            )
            # if we have args, we're probably calling UNNEST
            if "args" in join["relation"]["Table"]:
                args = [
                    self._build_dnf_filters(a)
                    for a in join["relation"]["Table"]["args"]
                ]
                # CROSS JOINT _ UNNEST() needs specifically handling because the UNNEST is
                # probably a function of the data in the left table, which means we can't
                # use the table join code
                if len(args) > 0 and dataset == "UNNEST" and mode == "CrossJoin":
                    mode = "CrossJoinUnnest"
                    dataset = (dataset, args)
            yield (mode, (alias, dataset), join_on, using)

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

    def _explain_planner(self, ast, statistics):
        explain_plan = self.copy()
        explain_plan.create_plan(ast=[ast[0]["Explain"]["statement"]])
        explain_node = ExplainNode(statistics, query_plan=explain_plan)
        self.add_operator("explain", explain_node)

    def _show_columns_planner(self, ast, statistics):

        #        relation = ast[0]["ShowColumns"]["table_name"][0]["value"]

        relation = ".".join(
            [part["value"] for part in ast[0]["ShowColumns"]["table_name"]]
        )

        self.add_operator(
            "reader",
            DatasetReaderNode(
                statistics,
                dataset=(None, relation),
                reader=self._reader,
                cache=self._cache,
                partition_scheme=self._partition_scheme,
                start_date=self._start_date,
                end_date=self._end_date,
            ),
        )
        self.add_operator("columns", ShowColumnsNode(statistics))
        self.link_operators("reader", "columns")

        filters = self._extract_filter(ast)
        if filters:
            self.add_operator(
                "filter", SelectionNode(statistics=statistics, filter=filters)
            )
            self.link_operators("columns", "filter")

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
        _relations = [r for r in self._extract_relations(ast)]
        if len(_relations) == 0:
            _relations = [(None, "$no_table")]

        # We always have a data source - even if it's 'no table'
        self.add_operator(
            "from",
            DatasetReaderNode(
                statistics,
                dataset=_relations[0],
                reader=self._reader,
                cache=self._cache,
                partition_scheme=self._partition_scheme,
                start_date=self._start_date,
                end_date=self._end_date,
            ),
        )
        last_node = "from"

        _joins = list(self._extract_joins(ast))
        if len(_joins) == 0 and len(_relations) == 2:
            # If there's no stated JOIN but the query has two relations, we
            # use a CROSS JOIN
            _joins = [("CrossJoin", _relations[1], None, None)]
        for join_id, _join in enumerate(_joins):
            if _join or len(_relations) == 2:
                if _join[0] == "CrossJoinUnnest":
                    # If we're doing a CROSS JOIN UNNEST, the right table is an UNNEST function
                    right = _join[1]
                else:
                    # Otherwise, the right table needs to come from the Reader
                    right = DatasetReaderNode(
                        statistics,
                        dataset=_join[1],
                        reader=self._reader,
                        cache=self._cache,
                        partition_scheme=self._partition_scheme,
                        start_date=self._start_date,
                        end_date=self._end_date,
                    )

                # map join types to their implementations
                join_nodes = {
                    "CrossJoin": CrossJoinNode,
                    "CrossJoinUnnest": CrossJoinNode,
                    "FullOuter": OuterJoinNode,
                    "Inner": InnerJoinNode,
                    "LeftOuter": OuterJoinNode,
                    "RightOuter": OuterJoinNode,
                }

                join_node = join_nodes.get(_join[0])
                if join_node is None:
                    raise SqlError(f"Join type not supported - `{_join[0]}`")

                self.add_operator(
                    f"join-{join_id}",
                    join_node(
                        statistics,
                        right_table=right,
                        join_type=_join[0],
                        join_on=_join[2],
                        join_using=_join[3],
                    ),
                )
                self.link_operators(last_node, f"join-{join_id}")
                last_node = f"join-{join_id}"

        _projection = self._extract_projections(ast)
        if any(["function" in a for a in _projection]):
            self.add_operator(
                "eval", EvaluationNode(statistics, projection=_projection)
            )
            self.link_operators(last_node, "eval")
            last_node = "eval"

        _selection = self._extract_selection(ast)
        if _selection:
            self.add_operator("where", SelectionNode(statistics, filter=_selection))
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
                "agg", AggregateNode(statistics, aggregates=_aggregates, groups=_groups)
            )
            self.link_operators(last_node, "agg")
            last_node = "agg"

        _having = self._extract_having(ast)
        if _having:
            self.add_operator("having", SelectionNode(statistics, filter=_having))
            self.link_operators(last_node, "having")
            last_node = "having"

        self.add_operator("select", ProjectionNode(statistics, projection=_projection))
        self.link_operators(last_node, "select")
        last_node = "select"

        _distinct = self._extract_distinct(ast)
        if _distinct:
            self.add_operator("distinct", DistinctNode(statistics))
            self.link_operators(last_node, "distinct")
            last_node = "distinct"

        _order = self._extract_order(ast)
        if _order:
            self.add_operator("order", SortNode(statistics, order=_order))
            self.link_operators(last_node, "order")
            last_node = "order"

        _offset = self._extract_offset(ast)
        if _offset:
            self.add_operator("offset", OffsetNode(statistics, offset=_offset))
            self.link_operators(last_node, "offset")
            last_node = "offset"

        _limit = self._extract_limit(ast)
        # 0 limit is valid
        if _limit is not None:
            self.add_operator("limit", LimitNode(statistics, limit=_limit))
            self.link_operators(last_node, "limit")
            last_node = "limit"

    def explain(self):

        import pyarrow

        from opteryx.utils.columns import Columns

        def _inner_explain(operator_name, depth):
            depth += 1
            operator = self.get_operator(operator_name)
            yield {"operator": operator.name, "config": operator.config, "depth": depth}
            out_going_links = self.get_outgoing_links(operator_name)
            if out_going_links:
                for next_operator_name in out_going_links:
                    yield from _inner_explain(next_operator_name, depth)

        entry_points = self.get_entry_points()
        nodes = []
        for entry_point in entry_points:
            nodes += list(_inner_explain(entry_point, 0))

        table = pyarrow.Table.from_pylist(nodes)
        table = Columns.create_table_metadata(table, table.num_rows, "plan", None)
        yield table

    def add_operator(self, name, operator):
        """
        Add a step to the DAG

        Parameters:
            name: string
                The name of the step, must be unique
            Operator: BaseOperator
                The Operator
        """
        self.nodes[name] = operator

    def link_operators(self, source_operator, target_operator):
        """
        Link steps in a flow.

        Parameters:
            source_operator: string
                The name of the source step
            target_operator: string
                The name of the target step
        """
        edge = (source_operator, target_operator)
        if edge not in self.edges:
            self.edges.append((source_operator, target_operator))

    def get_outgoing_links(self, name):
        """
        Get the names of outgoing links from a given step.

        Paramters:
            name: string
                The name of the step to search from
        """
        retval = {target for source, target in self.edges if source == name}
        return sorted(retval)

    def get_exit_points(self):
        """
        Get steps in the flow with no outgoing steps.
        """
        sources = {source for source, target in self.edges}
        retval = {target for source, target in self.edges if target not in sources}
        return sorted(retval)

    def get_entry_points(self):
        """
        Get steps in the flow with no incoming steps.
        """
        if len(self.nodes) == 1:
            return list(self.nodes.keys())
        targets = {target for source, target in self.edges}
        retval = {source for source, target in self.edges if source not in targets}
        return sorted(retval)

    def get_operator(self, name):
        """
        Get the Operator class by name.

        Parameters:
            name: string
                The name of the step
        """
        return self.nodes.get(name)

    def merge(self, assimilatee):
        """
        Merge a flow into the current flow.

        Parameters:
            assimilatee: Flow
                The flow to assimilate into the current flows
        """
        self.nodes = {**self.nodes, **assimilatee.nodes}
        self.edges += assimilatee.edges
        self.edges = list(set(self.edges))

    #    def __repr__(self):
    #        return "\n".join(list(self._draw()))

    def _inner_execute(self, operator_name, relation):
        # print(f"***********{operator_name}***************")
        operator = self.get_operator(operator_name)
        out_going_links = self.get_outgoing_links(operator_name)
        outcome = operator.execute(relation)
        if out_going_links:
            for next_operator_name in out_going_links:
                return self._inner_execute(next_operator_name, outcome)
        else:
            return outcome

    def execute(self):
        entry_points = self.get_entry_points()
        rel = None
        for entry_point in entry_points:
            rel = self._inner_execute(entry_point, None)
        return rel
