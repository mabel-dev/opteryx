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
"""
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import decimal

from opteryx.engine.planner.operations import *
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.exceptions import SqlError
from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.storage.schemes import DefaultPartitionScheme
from opteryx.storage.adapters.local.disk_store import DiskStorage

"""
BinaryOperator::Plus => "+",
BinaryOperator::Minus => "-",
BinaryOperator::Multiply => "*",
BinaryOperator::Divide => "/",
BinaryOperator::Modulo => "%",
BinaryOperator::StringConcat => "||",
BinaryOperator::Spaceship => "<=>",
BinaryOperator::And => "AND",
BinaryOperator::Or => "OR",
BinaryOperator::Xor => "XOR",
BinaryOperator::BitwiseOr => "|",
BinaryOperator::BitwiseAnd => "&",
BinaryOperator::BitwiseXor => "^",
BinaryOperator::PGBitwiseXor => "#",
BinaryOperator::PGBitwiseShiftLeft => "<<",
BinaryOperator::PGBitwiseShiftRight => ">>",
BinaryOperator::PGRegexIMatch => "~*",
BinaryOperator::PGRegexNotMatch => "!~",
BinaryOperator::PGRegexNotIMatch => "!~*",
"""

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


def _build_dnf_filters(filters):
    """
    This is a basic version of the filter builder.
    """
    # None is None
    if filters is None:
        return None

    print(filters)
    if "Identifier" in filters:  # we're an identifier
        return (filters["Identifier"]["value"], TOKEN_TYPES.IDENTIFIER)
    if "Value" in filters:  # we're a literal
        value = filters["Value"]
        if "SingleQuotedString" in value:
            return (value["SingleQuotedString"], TOKEN_TYPES.VARCHAR)
        if "Number" in value:
            return (decimal.Decimal(value["Number"][0]), TOKEN_TYPES.NUMERIC)
        if "Boolean" in value:
            return (value["Boolean"], TOKEN_TYPES.BOOLEAN)
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
    if "Between" in filters:
        left = _build_dnf_filters(filters["Between"]["expr"])
        low = _build_dnf_filters(filters["Between"]['low'])
        high = _build_dnf_filters(filters["Between"]['high'])
        inverted = filters["Between"]["negated"]

        if inverted:
            # LEFT <= LOW AND LEFT >= HIGH (not between)
            return ([(left, "<=", low), (left, ">=", high)]) 
        else:
            # LEFT > LOW and LEFT < HIGH (between)
            return ([(left, ">", low), (left, "<", high)])
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
        right = ([_build_dnf_filters(v)[0] for v in filters["InList"]["list"]], TOKEN_TYPES.LIST,)
        operator = "not in" if filters["InList"]["negated"] else "in"
        return (left, operator, right)


def _extract_relations(ast):
    """ """
    relations = ast[0]["Query"]["body"]["Select"]["from"][0]
    if "Table" in relations["relation"]:
        dataset = ".".join(
            [part["value"] for part in relations["relation"]["Table"]["name"]]
        )
        return dataset

    if "Derived" in relations["relation"]:
        subquery = relations["relation"]["Derived"]["subquery"]["body"]
        raise NotImplementedError("SUBQUERIES in FROM statements not supported")
        # {'Select': {'distinct': False, 'top': None, 'projection': ['Wildcard'], 'from': [{'relation': {'Table': {'name': [{'value': 't', 'quote_style': None}], 'alias': None, 'args': [], 'with_hints': []}}, 'joins': []}], 'lateral_views': [], 'selection': None, 'group_by': [], 'cluster_by': [], 'distribute_by': [], 'sort_by': [], 'having': None}}


def _extract_projections(ast):
    """
    Projections are lists of attributes, the most obvious one is in the SELECT
    statement but they can exist elsewhere to limit the amount of data
    processed at each step.
    """
    projection = ast[0]["Query"]["body"]["Select"]["projection"]
    print(projection)
    if projection == ["Wildcard"]:
        return {"*": "*"}

    def _inner(attribute):
        if "UnnamedExpr" in attribute:
            unnamed = attribute["UnnamedExpr"]
            if "Identifier" in unnamed:
                return unnamed["Identifier"]["value"]
            if "Function" in attribute:
                # {'Function': {'name': [{'value': 'APPROX_SIZE', 'quote_style': None}], 'args': [{'Unnamed': {'Identifier': {'value': 'name', 'quote_style': None}}}]
                raise NotImplementedError("functions are currently not suppored")
        if "ExprWithAlias" in attribute:
            # [{'ExprWithAlias': {'expr': {'Function': {'name': [{'value': 'APPROX_SIZE', 'quote_style': None}], 'args': [{'Unnamed': {'Identifier': {'value': 'name', 'quote_style': None}}}], 'over': None, 'distinct': False}}, 'alias': {'value': 'APPLE', 'quote_style': None}}}]
            raise NotImplementedError("aliases aren't supported")

    projection = [_inner(attribute) for attribute in projection]
    return projection


def _extract_selection(ast):
    """
    Although there is a SELECT statement in a SQL Query, Selection refers to the
    filter or WHERE statement.
    """
    selections = ast[0]["Query"]["body"]["Select"]["selection"]
    return _build_dnf_filters(selections)


class QueryPlan(object):
    def __init__(self, sql: str, statistics, reader, partition_scheme):
        """
        PLan represents Directed Acyclic Graphs which are used to describe data
        pipelines.
        """
        import sqloxide

        self.nodes = {}
        self.edges = []

        self._reader = reader
        self._partition_scheme = partition_scheme

        # Parse the SQL into a AST
        try:
            self._ast = sqloxide.parse_sql(sql, dialect="mysql")
            # MySQL Dialect allows identifiers to be delimited with ` (backticks) and
            # identifiers to start with _ (underscore) and $ (dollar sign)
            # https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/dialect/mysql.rs
        except ValueError as e:
            print(sql)
            raise SqlError(e)

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
        self.add_operator(
            "where", SelectionNode(statistics, filter=_extract_selection(ast))
        )
        # self.add_operator("group", GroupByNode(ast["select"]["group_by"]))
        # self.add_operator("having", SelectionNode(ast["select"]["having"]))
        self.add_operator(
            "select", ProjectionNode(statistics, projection=_extract_projections(ast))
        )
        # self.add_operator("order", OrderNode(ast["order_by"]))
        # self.add_operator("limit", LimitNode(ast["limit"]))

        # self.link_operators("from", "union")
        # self.link_operators("union", "where")
        # self.link_operators("where", "group")
        # self.link_operators("group", "having")
        # self.link_operators("having", "select")
        # self.link_operators("select", "order")
        # self.link_operators("order", "limit")

        self.link_operators("from", "where")
        self.link_operators("where", "select")

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
        print(f"***********{operator_name}***************")
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


if __name__ == "__main__":

    import time
    from opteryx.third_party.pyarrow_ops import head

    SQL = "SELECT * from `tests-data.zoned` where followers = 'name'"

    statistics = QueryStatistics()
    statistics.start_time = time.time_ns()
    q = QueryPlan(
        SQL,
        statistics,
        reader=DiskStorage(),
        partition_scheme=DefaultPartitionScheme(""),
    )
    print(q)

    from opteryx.engine.display import ascii_table
    from opteryx.utils.pyarrow import fetchmany, fetchall

    # do this to go over the records
    print([a for a in fetchall(q.execute())])

    print(statistics.as_dict())
    print((time.time_ns() - statistics.start_time) / 1e9)
