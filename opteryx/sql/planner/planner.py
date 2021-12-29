"""
Query Planner
-------------

This builds a DAG which describes a query.

This doesn't attempt to do optimization, this just decomposes the query.
"""

import re
from typing import Optional
from ....utils.token_labeler import TOKENS, Tokenizer

NODE_TYPES = {
    "select": "select",  # filter tuples
    "project": "project",  # filter fields
    "read": "read",  # read file
    "join": "join",  # cartesian join
    "union": "union",  # acculate records
    "rename": "rename",  # rename fields
    "sort": "sort",  # order records
    "distinct": "distinct",  # deduplicate
    "aggregate": "aggregation",  # calculate aggregations
    "evaluate": "evaluation",  # calculate evaluations
}

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

## expressions are split, ANDs are separate items, ORs are kept together

"""

SELECT * FROM TABLE => reader(TABLE) -> project(*)
SELECT field FROM TABLE => reader(TABLE) -> project(field)
SELECT field FROM TABLE WHERE field > 7 => reader(TABLE) -> select(field > 7) -> project(field)

"""


class Node(object):
    def __init__(self):
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
