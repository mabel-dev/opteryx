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
    02. JOIN
    03. < expressions and aliases
    04. WHERE
    05. GROUP BY
    06. HAVING
    07. SELECT
    08. DISTINCT
    09. ORDER BT
    10. OFFSET
    11. LIMIT

However, this doesn't preclude the order being different to achieve optimizations, as
long as the functional outcode would be the same. Expressions and aliases technically
should not be evaluated until the SELECT statement.
"""
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import numpy

from opteryx.engine.planner.operations import *
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.exceptions import SqlError
from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.storage.schemes import DefaultPartitionScheme
from opteryx.storage.adapters import DiskStorage
from opteryx.engine.functions import is_function


JSON_TYPES = {numpy.bool_: bool, numpy.int64: int, numpy.float64: float}

def _serializer(obj):
    return JSON_TYPES[type(obj)](obj)


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
    #"Plus": "+",
    #"Minus": "-",
    # : "||"  # <- concatenate
}

def _extract_value(value):
    if value is None:
        return (None, None)
    if "SingleQuotedString" in value:
        return (value["SingleQuotedString"], TOKEN_TYPES.VARCHAR)
    if "Number" in value:
        return (numpy.float64(value["Number"][0]), TOKEN_TYPES.NUMERIC)
    if "Boolean" in value:
        return (value["Boolean"], TOKEN_TYPES.BOOLEAN)


def _build_dnf_filters(filters):

    # None is None
    if filters is None:
        return None

    if "Identifier" in filters:  # we're an identifier
        if filters["Identifier"]["value"][:2] == "$$":
            try:
                from opteryx.engine.functions import PLACEHOLDERS

                return (
                    PLACEHOLDERS[filters["Identifier"]["value"].upper()](),
                    TOKEN_TYPES.LIST,
                )
            except KeyError:
                raise SqlError(
                    f"Unknown placeholder `{filters['Identifier']['value']}`."
                )
        return (filters["Identifier"]["value"], TOKEN_TYPES.IDENTIFIER)
    if "Value" in filters:  # we're a literal
        return _extract_value(filters["Value"])
    if "BinaryOp" in filters:
        left = _build_dnf_filters(filters["BinaryOp"]["left"])
        operator = filters["BinaryOp"]["op"]
        right = _build_dnf_filters(filters["BinaryOp"]["right"])

        if operator in ("And"):
            return [left, right]
        if operator in ("Or"):
            return [[left], [right]]
        return (left, OPERATOR_XLAT[operator], right)
    if "UnaryOp" in filters:
        if filters["UnaryOp"]["op"] == "Not":
            left = _build_dnf_filters(filters["UnaryOp"]["expr"])
            return (left, "<>", True)
        if filters["UnaryOp"]["op"] == "Minus":
            number = 0 - numpy.float64(
                filters["UnaryOp"]["expr"]["Value"]["Number"][0]
            )
            return (number, TOKEN_TYPES.NUMERIC)
    if "Between" in filters:
        left = _build_dnf_filters(filters["Between"]["expr"])
        low = _build_dnf_filters(filters["Between"]["low"])
        high = _build_dnf_filters(filters["Between"]["high"])
        inverted = filters["Between"]["negated"]

        if inverted:
            # LEFT <= LOW AND LEFT >= HIGH (not between)
            return [[(left, "<", low)], [(left, ">", high)]]
        else:
            # LEFT > LOW and LEFT < HIGH (between)
            return [(left, ">=", low), (left, "<=", high)]
    if "InSubquery" in filters:
        # WHERE g in (select * from b)
        # {'InSubquery': {'expr': {'Identifier': {'value': 'g', 'quote_style': None}}, 'subquery': {'with': None, 'body': {'Select': {'distinct': False, 'top': None, 'projection': ['Wildcard'], 'from': [{'relation': {'Table': {'name': [{'value': 'b', 'quote_style': None}], 'alias': None, 'args': [], 'with_hints': []}}, 'joins': []}], 'lateral_views': [], 'selection': None, 'group_by': [], 'cluster_by': [], 'distribute_by': [], 'sort_by': [], 'having': None}}, 'order_by': [], 'limit': None, 'offset': None, 'fetch': None}, 'negated': False}}
        raise NotImplementedError("IN SUBQUERIES are not implemented.")
    if "IsNull" in filters:
        left = _build_dnf_filters(filters["IsNull"])
        return (left, "=", None)
    if "IsNotNull" in filters:
        left = _build_dnf_filters(filters["IsNotNull"])
        return (left, "<>", None)
    if "InList" in filters:
        left = _build_dnf_filters(filters["InList"]["expr"])
        right = (
            [_build_dnf_filters(v)[0] for v in filters["InList"]["list"]],
            TOKEN_TYPES.LIST,
        )
        operator = "not in" if filters["InList"]["negated"] else "in"
        return (left, operator, right)
    if filters == "Wildcard":
        return ("Wildcard", TOKEN_TYPES.WILDCARD)
    if "Function" in filters:
        func = filters["Function"]["name"][0]["value"].upper()
        args = [_build_dnf_filters(a["Unnamed"]) for a in filters["Function"]["args"]]
        args = [(str(a[0]) if a[0] != "Wildcard" else "*") for a in args]
        column_name = f"{func}({','.join(args)})"
        return (column_name, TOKEN_TYPES.IDENTIFIER)
    if "Unnamed" in filters:
        return _build_dnf_filters(filters["Unnamed"])
    if "Expr" in filters:
        return _build_dnf_filters(filters["Expr"])
    if "Nested" in filters:
        return (_build_dnf_filters(filters["Nested"]),)

#def _extract_date_filters(ast):
#
#    def _inner():
#        filters = _build_dnf_filters(ast[0]["Query"]["body"]["Select"]["selection"])
#        for filter in [filters]:
#            if filter[0] == "$DATE":
#                yield filter
#
#    return list(_inner())

def _extract_relations(ast):
    """ """

    def _safe_get(l, i):
        try:
            return l[i]
        except:
            return None

    try:
        relations = ast[0]["Query"]["body"]["Select"]["from"][0]
    except IndexError:
        return "$no_table"
    if "Table" in relations["relation"]:
        dataset = ".".join(
            [part["value"] for part in relations["relation"]["Table"]["name"]]
        )
        return dataset

    if "Derived" in relations["relation"]:
        subquery = relations["relation"]["Derived"]["subquery"]["body"]
        try:
            alias = relations["relation"]["Derived"]["alias"]["name"]["value"]
        except KeyError:
            alias = None
        if "Select" in subquery:
            raise NotImplementedError("SUBQUERIES in FROM statements not supported")
            # {'Select': {'distinct': False, 'top': None, 'projection': ['Wildcard'], 'from': [{'relation': {'Table': {'name': [{'value': 't', 'quote_style': None}], 'alias': None, 'args': [], 'with_hints': []}}, 'joins': []}], 'lateral_views': [], 'selection': None, 'group_by': [], 'cluster_by': [], 'distribute_by': [], 'sort_by': [], 'having': None}}
        if "Values" in subquery:
            import orjson
            body = bytearray()
            headers = [h["value"] for h in relations["relation"]["Derived"]["alias"]["columns"]]
            for value_set in subquery["Values"]:
                values = [_safe_get(_extract_value(v["Value"]), 0) for v in value_set]
                body.extend(orjson.dumps(dict(zip(headers, values)), default=_serializer))
            return (alias, body) # <- a literal table


def _extract_projections(ast):
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
        alias = None
        if "UnnamedExpr" in attribute:
            function = attribute["UnnamedExpr"]
        if "ExprWithAlias" in attribute:
            function = attribute["ExprWithAlias"]["expr"]
            alias = attribute["ExprWithAlias"]["alias"]["value"]

        if function:
            if "Identifier" in function:
                return {"identifier": function["Identifier"]["value"], "alias": alias}
            if "Function" in function:
                func = function["Function"]["name"][0]["value"].upper()
                args = [_build_dnf_filters(a) for a in function["Function"]["args"]]
                if is_function(func):
                    return {"function": func.upper(), "args": args, "alias": alias}
                else:
                    return {"aggregate": func.upper(), "args": args, "alias": alias}
            if "BinaryOp" in function:
                raise NotImplementedError("Operations in the SELECT clause are not supported")
                return {"operation": _build_dnf_filters(function)}
            if "Cast" in function:
                # CAST(<var> AS <type>) - convert to the form <type>(var), e.g. BOOLEAN(on)
                args = [_build_dnf_filters(function["Cast"]["expr"])]
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

                return {"function": data_type, "args": args, "alias": alias }

    projection = [_inner(attribute) for attribute in projection]
    # print(projection)
    return projection


def _extract_selection(ast):
    """
    Although there is a SELECT statement in a SQL Query, Selection refers to the
    filter or WHERE statement.
    """
    selections = ast[0]["Query"]["body"]["Select"]["selection"]
    return _build_dnf_filters(selections)


def _extract_distinct(ast):
    return ast[0]["Query"]["body"]["Select"]["distinct"]


def _extract_limit(ast):
    limit = ast[0]["Query"]["limit"]
    if limit is not None:
        return int(limit["Value"]["Number"][0])
    return None


def _extract_offset(ast):
    offset = ast[0]["Query"]["offset"]
    if offset is not None:
        return int(offset["value"]["Value"]["Number"][0])
    return None


def _extract_order(ast):
    order = ast[0]["Query"]["order_by"]
    if order is not None:
        orders = []
        for col in order:
            column = col["expr"]
            if "Identifier" in column:
                column = column["Identifier"]["value"]
            if "Function" in column:
                func = column["Function"]["name"][0]["value"].upper()
                args = [_build_dnf_filters(a)[0] for a in column["Function"]["args"]]
                args = ["*" if i == "Wildcard" else i for i in args]
                column = f"{func}({','.join(args)})"
            orders.append(
                (
                    column,
                    "descending" if str(col["asc"]) == "False" else "ascending",
                ),
            )
        return orders


def _extract_groups(ast):
    def _inner(element):
        if element:
            if "Identifier" in element:
                return element["Identifier"]["value"]
            if "Function" in element:
                func = element["Function"]["name"][0]["value"].upper()
                args = [_build_dnf_filters(a) for a in element["Function"]["args"]]
                return f"{func.upper()}({','.join([str(a[0]) for a in args])})"

    groups = ast[0]["Query"]["body"]["Select"]["group_by"]
    return [_inner(g) for g in groups]


def _extract_having(ast):
    having = ast[0]["Query"]["body"]["Select"]["having"]
    return _build_dnf_filters(having)


class QueryPlan(object):
    def __init__(self, sql: str, statistics, reader, partition_scheme):
        """
        PLan represents Directed Acyclic Graphs which are used to describe data
        pipelines.
        """
        import sqloxide

        self.nodes: dict = {}
        self.edges: list = []

        self._reader = reader
        self._partition_scheme = partition_scheme

        # Parse the SQL into a AST
        try:
            self._ast = sqloxide.parse_sql(sql, dialect="mysql")
            # MySQL Dialect allows identifiers to be delimited with ` (backticks) and
            # identifiers to start with _ (underscore) and $ (dollar sign)
            # https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/dialect/mysql.rs
        except ValueError as e:
            # print(sql)
            raise SqlError(e)

        # print(self._ast)

        # build a plan for the query
        self._naive_planner(self._ast, statistics)

    def _naive_planner(self, ast, statistics):
        """
        The naive planner only works on single tables and puts operations in this
        order.

            FROM clause
            WHERE clause
            GROUP BY clause
            HAVING clause
            SELECT clause
            ORDER BY clause
            LIMIT clause
            OFFSET clause

        This is phase one of the rewrite, to essentially mimick the existing
        functionality.
        """
        self.add_operator(
            "from",
            DatasetReaderNode(
                statistics,
                dataset=_extract_relations(ast),
                reader=self._reader,
                partition_scheme=self._partition_scheme,
            ),
        )
        last_node = "from"

        _projection = _extract_projections(ast)
        if any(["function" in a for a in _projection]):
            self.add_operator(
                "eval", EvaluationNode(statistics, projection=_projection)
            )
            self.link_operators(last_node, "eval")
            last_node = "eval"

        _selection = _extract_selection(ast)
        if _selection:
            self.add_operator("where", SelectionNode(statistics, filter=_selection))
            self.link_operators(last_node, "where")
            last_node = "where"

        _groups = _extract_groups(ast)
        #        _columns = _extract_columns(ast)
        if _groups or any(["aggregate" in a for a in _projection]):
            _aggregates = _projection.copy()
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

        _having = _extract_having(ast)
        if _having:
            self.add_operator("having", SelectionNode(statistics, filter=_having))
            self.link_operators(last_node, "having")
            last_node = "having"

        self.add_operator("select", ProjectionNode(statistics, projection=_projection))
        self.link_operators(last_node, "select")
        last_node = "select"

        _distinct = _extract_distinct(ast)
        if _distinct:
            self.add_operator("distinct", DistinctNode(statistics))
            self.link_operators(last_node, "distinct")
            last_node = "distinct"

        _order = _extract_order(ast)
        if _order:
            self.add_operator("order", SortNode(statistics, order=_order))
            self.link_operators(last_node, "order")
            last_node = "order"

        _offset = _extract_offset(ast)
        if _offset:
            self.add_operator("offset", OffsetNode(statistics, offset=_offset))
            self.link_operators(last_node, "offset")
            last_node = "offset"

        _limit = _extract_limit(ast)
        if _limit:
            self.add_operator("limit", LimitNode(statistics, limit=_limit))
            self.link_operators(last_node, "limit")
            last_node = "limit"

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

    def __repr__(self):
        return "\n".join(list(self._draw()))

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
        for entry_point in entry_points:
            rel = self._inner_execute(entry_point, None)
        return rel

    def _draw(self):
        for entry in self.get_entry_points():
            node = self.get_operator(entry)
            yield (f"{str(entry)} ({repr(node)})")
            t = self._tree(entry, "")
            yield ("\n".join(t))

    def _tree(self, node, prefix=""):

        space = "    "
        branch = " │  "
        tee = " ├─ "
        last = " └─ "

        contents = self.get_outgoing_links(node)
        # contents each get pointers that are ├── with a final └── :
        pointers = [tee] * (len(contents) - 1) + [last]
        for pointer, child_node in zip(pointers, contents):
            operator = self.get_operator(child_node)
            yield prefix + pointer + str(child_node) + " (" + repr(operator) + ")"
            if len(self.get_outgoing_links(node)) > 0:
                # extend the prefix and recurse:
                extension = branch if pointer == tee else space
                # i.e. space because last, └── , above so no more |
                yield from self._tree(str(child_node), prefix=prefix + extension)


def test(SQL):

    from mabel.utils import timer

    import time
    from opteryx.third_party.pyarrow_ops import head

    statistics = QueryStatistics()
    statistics.start_time = time.time_ns()
    q = QueryPlan(
        SQL,
        statistics,
        reader=DiskStorage(),
        partition_scheme=DefaultPartitionScheme(""),
    )
    statistics.time_planning = time.time_ns() - statistics.start_time
    # print(q)

    from opteryx.utils.display import ascii_table
    from opteryx.utils.pyarrow import fetchmany, fetchall

    with timer.Timer():
        # do this to go over the records
        r = q.execute()
        print(ascii_table(fetchmany(r, size=10), limit=10))

        [a for a in fetchall(r)]

        statistics.end_time = time.time_ns()
        print(statistics.as_dict())
        print((time.time_ns() - statistics.start_time) / 1e9)


if __name__ == "__main__":

    import sqloxide

    # SQL = "SELECT count(*) from `tests.data.zoned` where followers < 10 group by followers"
    # SQL = "SELECT username, count(*) from `tests.data.tweets` group by username"
    SQL = "SELECT COUNT(user_verified) FROM tests.data.set"

    # SQL = """
    # SELECT DISTINCT user_verified, MIN(followers), MAX(followers), COUNT(*)
    #  FROM tests.data.huge
    # GROUP BY user_verified
    # """

    # SQL = "SELECT username from `tests.data.tweets`"

    SQL = "SELECT * FROM $satellites"
    SQL = "SELECT COUNT(*) FROM $satellites"
    SQL = "SELECT MAX(planetId), MIN(planetId), SUM(gm), count(*) FROM $satellites group by planetId"
    SQL = "SELECT upper(name), length(name) FROM $satellites WHERE magnitude = 5.29"
    SQL = "SELECT planetId, Count(*) FROM $satellites group by planetId having count(*) > 5"
    SQL = "SELECT * FROM $satellites order by magnitude, name"
    SQL = "SELECT AVG(gm), MIN(gm), MAX(gm), FIRST(gm), COUNT(*) FROM $satellites GROUP BY planetId"
    SQL = "SELECT COUNT(name) FROM $satellites;"

    SQL = "SELECT name as what_it_known_as FROM $satellites"
    SQL = "SELECT name, id, planetId FROM $satellites"
    SQL = "SELECT COUNT(*), planetId FROM $satellites GROUP BY planetId"
    SQL = "SELECT ROUND(magnitude) FROM $satellites group by ROUND(magnitude)"
    SQL = "SELECT COUNT(*) FROM $satellites"
    SQL = "SELECT * FROM $satellites WHERE (id = 6 OR id = 7 OR id = 8) OR name = 'Europa'"
    SQL = (
        "SELECT BOOLEAN(planetId) FROM $satellites GROUP BY planetId, BOOLEAN(planetId)"
    )
    SQL = "SELECT planetId as pid, round(magnitude) as minmag FROM $satellites"
    SQL = "SELECT RANDOM() FROM $planets"
    SQL = "SELECT RANDOM() FROM $planets"
    SQL = "SELECT TIME()"
    SQL = "SELECT LOWER('NAME')"
    SQL = "SELECT HASH('NAME')"
    SQL = "SELECT * FROM (VALUES (1,2),(3,4),(340,455)) AS t(a,b)"
    #SQL = "SELECT id - 1, name, mass FROM $planets"
    SQL = "SELECT * FROM tests.data.partitioned WHERE $DATE IN ($$PREVIOUS_MONTH)"
    SQL = """
SELECT * FROM (VALUES 
(7369, 'SMITH', 'CLERK', 7902, '02-MAR-1970', 8000, NULL, 20),
(7499, 'ALLEN', 'SALESMAN', 7698, '20-MAR-1971', 1600, 3000, 30),
(7521, 'WARD', 'SALESMAN', 7698, '07-FEB-1983', 1250, 5000, 30),
(7566, 'JONES', 'MANAGER', 7839, '02-JUN-1961', 2975, 50000, 20),
(7654, 'MARTIN', 'SALESMAN', 7698, '28-FEB-1971', 1250, 14000, 30),
(7698, 'BLAKE', 'MANAGER', 7839, '01-JAN-1988', 2850, 12000, 30),
(7782, 'CLARK', 'MANAGER', 7839, '09-APR-1971', 2450, 13000, 10),
(7788, 'SCOTT', 'ANALYST', 7566, '09-DEC-1982', 3000, 1200, 20),
(7839, 'KING', 'PRESIDENT', NULL, '17-JUL-1971', 5000, 1456, 10),
(7844, 'TURNER', 'SALESMAN', 7698, '08-AUG-1971', 1500, 0, 30),
(7876, 'ADAMS', 'CLERK', 7788, '12-MAR-1973', 1100, 0, 20),
(7900, 'JAMES', 'CLERK', 7698, '03-NOV-1971', 950, 0, 30),
(7902, 'FORD', 'ANALYST', 7566, '04-MAR-1961', 3000, 0, 20),
(7934, 'MILLER', 'CLERK', 7782, '21-JAN-1972', 1300, 0, 10))
AS employees (EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO);
    """
    SQL = """
    SELECT * FROM Employee
  FOR DATE
    BETWEEN '2014-01-01 00:00:00.0000000' AND '2015-01-01 00:00:00.0000000'
      WHERE EmployeeID = 1000 ORDER BY ValidFrom;
      """
    SQL = """
    SELECT * FROM Employee
  FOR ALL;
      """
    SQL = "SELECT CAST(planetId AS varchar) FROM $satellites"
    SQL = "SELECT planetId, count(*) FROM $satellites group by planetId order by count(*) desc"


    ast = sqloxide.parse_sql(SQL, dialect="mysql")
    print(ast)

    print()
#    print(_extract_date_filters(ast))

    # _projection = _extract_projections(ast)
    # print(_projection)

    import pyarrow
    import opteryx.samples
    from opteryx.third_party.pyarrow_ops import head

    # p = opteryx.samples.planets().select(["name"])

    # en = EvaluationNode(None, projection=_projection)

    # head(pyarrow.concat_tables(en.execute([p])))
    import cProfile

    with cProfile.Profile(subcalls=False) as pr:
        test(SQL)

    # pr.dump_stats("perf")

    # import pstats

    # p = pstats.Stats("perf")
    # p.sort_stats("tottime").print_stats(10)
