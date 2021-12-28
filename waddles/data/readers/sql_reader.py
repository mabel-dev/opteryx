# no-maintain-checks
"""
The SQL Reader works with JSONL, ZSTDed JSONL, PARQUET, AVRO and ORC files.

The SQL Reader returns a Relation.
"""

"""
This is a basic SQL parser and interpreter - we process the parts of the SQL statement
in the following order:

FROM clause
    - includes scalar functions
WHERE clause
GROUP BY clause
    - includes aggregate functions
HAVING clause
SELECT clause
    - includes AS renames
ORDER BY clause
LIMIT clause

"""

import re
from typing import Optional
from .internals.inline_evaluator import Evaluator, get_function_name
from ...utils.token_labeler import TOKENS, Tokenizer
from ...logging import get_logger


SQL_PARTS = [
    r"SELECT",
    r"DISTINCT",
    r"FROM",
    r"WHERE",
    r"GROUP BY",
    r"HAVING",
    r"ORDER BY",
    r"ASC",
    r"DESC",
    r"LIMIT",
]


class InvalidSqlError(Exception):
    pass


class SqlParser:
    def __init__(self, statement):
        self.select_expression: Optional[str] = None
        self.distinct: bool = False
        self.dataset: Optional[str] = None
        self.where_expression: Optional[str] = None
        self.group_by: Optional[str] = None
        self.having: Optional[str] = None
        self.order_by: Optional[str] = None
        self.order_descending: bool = False
        self.limit: Optional[int] = None

        self.parse(statement=statement)
        self.select_evaluator = Evaluator(self.select_expression)

    def __repr__(self):
        return str(
            {
                "select": self.select_expression,
                "disctinct": self.distinct,
                "from": self.dataset,
                "where": self.where_expression,
                "group by": self.group_by,
                "having": self.having,
                "order by": self.order_by,
                "descending": self.order_descending,
                "limit": self.limit,
            }
        )

    def sql_parts(self, string):
        reg = re.compile(
            r"(\(|\)|,|"
            + r"|".join([r"\b" + i.replace(r" ", r"\s") + r"\b" for i in SQL_PARTS])
            + r"|\s)",
            re.IGNORECASE,
        )
        parts = reg.split(string)
        return [part for part in parts if part != ""]

    def clean_statement(self, string):
        """
        Remove carriage returns and all whitespace to single spaces
        """
        _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
        return _RE_COMBINE_WHITESPACE.sub(" ", string).strip()

    def remove_comments(self, string):
        pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)"
        # first group captures quoted strings (double or single)
        # second group captures comments (//single-line or /* multi-line */)
        regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

        def _replacer(match):
            # if the 2nd group (capturing comments) is not None,
            # it means we have captured a non-quoted (real) comment string.
            if match.group(2) is not None:
                return ""  # so we will return empty to remove the comment
            else:  # otherwise, we will return the 1st group
                return match.group(1)  # captured quoted-string

        return regex.sub(_replacer, string)

    def validate_dataset(self, dataset):
        """
        The from clause is used to address resources in data storage; to prevent
        abusing this, we whitelist valid characters.

        Note that when this information is used, it is used in a glob query and
        only resources that exist are referenced with a reader, this should prevent
        exec flaws - but may still result in IDOR. To reduce this, use the
        `valid_dataset_prefixes` parameter on the Reader.
        """
        # start with a letter
        if not dataset[0].isalpha():
            raise InvalidSqlError("Malformed FROM clause - must start with a letter.")
        # can't be attempting path traversal
        if ".." in dataset or "//" in dataset or "--" in dataset:
            raise InvalidSqlError(
                "Malformed FROM clause - invalid repeated characters."
            )
        # can only contain limited character set (alpha num . / - _)
        if (
            not dataset.replace(".", "")
            .replace("/", "")
            .replace("-", "")
            .replace("_", "")
            .isalnum()
        ):
            raise InvalidSqlError("Malformed FROM clause - invalid characters.")
        return True

    def collector(self, labeler):
        collection = []
        while labeler.has_next():
            if labeler.next_token_value().upper() not in SQL_PARTS:
                collection.append(labeler.next_token_value())
                labeler.next()
            else:
                break
        return "".join(collection).strip()

    def parse(self, statement):
        # clean the string
        clean = self.remove_comments(statement)
        clean = self.clean_statement(clean)
        # split into bits by SQL keywords
        parts = self.sql_parts(clean)
        # put into a token labeller
        labeler = Tokenizer(parts)

        while labeler.has_next():
            if labeler.peek().strip() == "":
                labeler.next()
            elif labeler.peek().upper() == "SELECT":
                labeler.next()
                while labeler.has_next() and labeler.next_token_type() == TOKENS.EMPTY:
                    labeler.next()
                if labeler.next_token_value().upper() == "DISTINCT":
                    labeler.next()
                    self.distinct = True
                    while (
                        labeler.has_next() and labeler.next_token_type() == TOKENS.EMPTY
                    ):
                        labeler.next()
                self.select_expression = self.collector(labeler)
            elif labeler.peek().upper() == "FROM":

                labeler.next()
                while labeler.has_next() and labeler.next_token_type() == TOKENS.EMPTY:
                    labeler.next()

                if labeler.next_token_type() == TOKENS.LEFTPARENTHESES:
                    # we have a subquery - we're going to collect it based on matching
                    # parentheses
                    open_parentheses = 1
                    collector = []
                    labeler.next()
                    while open_parentheses > 0:
                        if not labeler.has_next():
                            break

                        if labeler.next_token_type() == TOKENS.RIGHTPARENTHESES:
                            open_parentheses -= 1
                            if open_parentheses != 0:
                                collector.append(labeler.peek())
                        elif labeler.next_token_type() == TOKENS.LEFTPARENTHESES:
                            open_parentheses += 1
                            collector.append(labeler.peek())
                        else:
                            collector.append(labeler.peek())
                        labeler.next()

                    if open_parentheses != 0:
                        raise InvalidSqlError(
                            "Malformed FROM clause - mismatched parenthesis."
                        )

                    self.dataset = collector
                else:
                    self.dataset = self.collector(labeler).replace(".", "/")
            elif labeler.peek().upper() == "WHERE":
                labeler.next()
                self.where_expression = self.collector(labeler)
            elif labeler.peek().upper() == "GROUP BY":
                labeler.next()
                self.group_by = self.collector(labeler)
            elif labeler.peek().upper() == "HAVING":
                labeler.next()
                self.having = self.collector(labeler)
            elif labeler.peek().upper() == "ORDER BY":
                labeler.next()
                self.order_by = self.collector(labeler)
                if labeler.has_next() and labeler.next_token_value().upper() in (
                    "ASC",
                    "DESC",
                ):
                    self.order_descending = labeler.next_token_value().upper() == "DESC"
                if labeler.has_next():
                    labeler.next()
            elif labeler.peek().upper() == "LIMIT":
                labeler.next()
                self.limit = int(self.collector(labeler))

        # validate inputs (currently only the FROM clause)
        if isinstance(self.dataset, str):
            self.validate_dataset(self.dataset)

        if self.dataset is None or self.select_expression is None:
            raise InvalidSqlError(
                "Invalid statement - all statements require SELECT and FROM clauses."
            )


def SqlReader(sql_statement: str, **kwargs):
    """
    Use basic SQL queries to filter Reader.

    Parameters:
        sql_statement: string
        kwargs: parameters to pass to the Reader

    Note:
        `select` is taken from SQL SELECT
        `dataset` is taken from SQL FROM
        `filters` is taken from SQL WHERE
    """

    # some imports here to remove cyclic imports
    from mabel import DictSet, Reader

    sql = SqlParser(sql_statement)
    get_logger().info(repr(sql))

    actual_select = sql.select_expression
    if sql.select_expression is None:
        actual_select = "*"
    elif sql.select_expression != "*":
        actual_select = sql.select_expression + ", *"

    reducer = None
    if sql.select_expression == "COUNT(*)":
        reducer = lambda x: {"*": "*"}

    # FROM clause
    # WHERE clause
    if isinstance(sql.dataset, list):
        # it's a list if it's been parsed into a SQL statement,
        # this is how subqueries are interpretted - the parser
        # doesn't extract a dataset name - it collects parts of
        # a SQL statement which it can then pass to a SqlReader
        # to get back a dataset - which we then use as the
        # dataset for the outer query.
        reader = SqlReader("".join(sql.dataset), **kwargs)
    else:
        reader = Reader(
            select=actual_select,
            dataset=sql.dataset,
            filters=sql.where_expression,
            **kwargs,
        )

    # GROUP BY clause
    if sql.group_by or any(
        [
            t["type"] == TOKENS.AGGREGATOR for t in sql.select_evaluator.tokens
        ]  # type:ignore
    ):
        from ..internals.group_by import GroupBy

        # convert the clause into something we can pass to GroupBy
        if sql.group_by:
            groups = [
                group.strip()
                for group in sql.group_by.split(",")
                if group.strip() != ""
            ]
        else:
            groups = ["*"]  # we're not really grouping

        aggregations = []
        renames = []
        for t in sql.select_evaluator.tokens:  # type:ignore
            if t["type"] == TOKENS.AGGREGATOR:
                aggregations.append((t["value"], t["parameters"][0]["value"]))
                if t["as"]:
                    t["raw"] = get_function_name(t)
                    renames.append(t)
            elif t["type"] == TOKENS.VARIABLE and t["value"] not in groups:
                raise InvalidSqlError(
                    "Invalid SQL - SELECT clause in a statement with a GROUP BY clause must be made of aggregations or items from the GROUP BY clause."
                )

        if aggregations:
            grouped = GroupBy(reader, groups).aggregate(aggregations)
        else:
            grouped = GroupBy(reader, groups).groups()

        # there could be 250000 groups, so we're not going to load them into memory
        reader = DictSet(grouped)

    # HAVING clause
    # if we have a HAVING clause, filter the grouped data by it
    if sql.having:
        reader = reader.filter(sql.having)

    # SELECT clause
    renames = {}  # type:ignore
    for t in sql.select_evaluator.tokens:  # type:ignore
        if t["as"]:
            renames[get_function_name(t)] = t["as"]

    def _perform_renames(row):
        for k, v in [(k, v) for k, v in row.items()]:
            if k in renames:
                row[renames[k]] = row.pop(k, row.get(renames[k]))
        return row

    if renames:
        reader = DictSet(map(_perform_renames, reader))

    reader = reader.project(sql.select_evaluator.fields())  # type:ignore
    # disctinct now we have only the columns we're interested in
    if sql.distinct:
        reader = reader.distinct()

    # ORDER BY clause
    if sql.order_by:
        take = 10000  # the Query UI is currently set to 2000
        if sql.limit:
            take = int(sql.limit)
        reader = DictSet(
            reader.sort_and_take(
                column=sql.order_by, take=take, descending=sql.order_descending
            )
        )

    # LIMIT clause
    if sql.limit:
        reader = reader.take(sql.limit)

    return reader
