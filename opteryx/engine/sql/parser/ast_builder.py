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
This component is part of the SQL Query Engine for Opteryx.

Tokenizer -> Lexer -> AST -> Planner -> Optimizer -> Executer
"""
if __name__ == "__main__":
    import sys
    import os

    sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))


from opteryx.engine.sql.parser.constants import SQL_TOKENS
from opteryx.exceptions import SqlError
from opteryx.utils.list_consumer import ListConsumer


class AstNode:
    def __init__(self, token, token_type):
        self.token = token
        self.node_type = token_type
        self.children = []

        # if the node has an AS
        self.alias = None
        # if the value is different to the token (lists and structs)
        self.value = None
        # node config
        self.config = {}

    def __str__(self):
        import json

        def _inner(node, prefix=""):
            repr = f"{prefix}{node.token} [ `{node.node_type}` {('AS ' + node.alias + ' ') if node.alias else ''}{('value: ' + self.value + ' ' if self.value else '')}{('config: ' + json.dumps(self.config) + ' ') if self.config else ''}]\n"
            for child in node.children:
                repr += _inner(child, prefix + "    ")
            return repr

        return _inner(self).rstrip("\n")


def _case_correction(token, part_of_query):
    if part_of_query in (
        SQL_TOKENS.LITERAL,
        SQL_TOKENS.ATTRIBUTE,
        SQL_TOKENS.SUBQUERY,
    ):
        return (token, part_of_query)
    return (token.upper(), part_of_query)


def _aquire_expect(consumer, poq, error):
    """
    This is a simple helper function to replace have to write code to get and test the
    PoQ (Part of Query) of tokens every time.
    """
    token, _poq = consumer.get()
    if _poq != poq:
        raise SqlError(error)
    # move along
    consumer.next()
    return token


def _acquire_between_brackets(consumer):
    """
    Consumes a list from an opening bracket to it's closing bracket

    Parameters:
        consumer

    Returns:
        List of Tokens
    """

    # work out which parenthesis we're looking for
    opening_bracket = consumer.get()[0]
    closing_bracket = ")"
    if opening_bracket == "[":
        closing_bracket = "]"
    if opening_bracket == "{":
        closing_bracket = "}"

    # step over the tokens, until we get to the matching closing parenthesis
    open_parentheses = 1
    collector = []

    while open_parentheses > 0 and consumer.has_more():
        consumer.next()
        if consumer.get()[0] == closing_bracket:
            open_parentheses -= 1
            if open_parentheses != 0:
                collector.append(consumer.get())
        elif consumer.get()[0] == opening_bracket:
            open_parentheses += 1
            collector.append(consumer.get())
        else:
            collector.append(consumer.get())

    # if we've not closed all the parenthesis, we have a problem
    if open_parentheses != 0:
        raise SqlError("Unable to interpret Query - Mismatched parenthesis.")

    return collector


def build_ast(tokens):
    """
    Abstract Syntax Tree (AST) builder - this is the final form before planning.

    This uses a hand-crafted LR(1) Parser. We read over the tagged representation of
    the Query (from the Lexer) left to right. We look for markers (like SELECT, WHERE)
    which tell us how to interpret to we get to the next marker, or run out of tokens.

    To support this we use the ListBurner class, this wraps most of the functionality
    to step over a list, looking at the current and next tags.

    We also validate the syntax of the query, although we still don't know if the
    dataset or attributes exist so making it past this step doesn't mean it will work.
    """

    # create a list consumer
    consumer = ListConsumer(tokens)

    # get the first token, it defines the type of query
    query_type, token_type = _case_correction(*consumer.get())
    if query_type not in ("SELECT", "EXPLAIN SELECT", "CREATE INDEX ON", "ANALYZE"):
        raise SqlError(
            "Unable to interpret Query - Unable to determine the type of Query."
        )

    # create the root node, it's type is the first token
    root = AstNode("ROOT", query_type)

    # Syntax: ANALYZE <dataset>
    # if we're an analyze query, we expect a single token which is a dataset name
    if query_type == "ANALYZE":
        # we need one more token
        if not consumer.has_more():
            raise SqlError(
                "Unable to interpret Query - Dataset name expected after ANALYZE (missing name)."
            )
        consumer.next()
        # if the token isn't what we expect, we have a problem
        root.config["dataset"] = _aquire_expect(
            consumer,
            SQL_TOKENS.ATTRIBUTE,
            "Unable to interpret Query - Dataset name expected after ANALYZE (invalid name).",
        )

    # Syntax: CREATE INDEX ON <dataset> (<attribute_list>)
    # We're going to extract out the attribute list even though we currently only
    # support having one, for both future options and for meaningful error messages.
    if query_type == "CREATE INDEX ON":
        # the next token is the dataset name
        if not consumer.has_more():
            raise SqlError(
                "Unable to interpret Query - Dataset name expected after CREATE INDEX ON (missing name)."
            )
        consumer.next()
        # The first token is the dataset
        root.config["dataset"] = _aquire_expect(
            consumer,
            SQL_TOKENS.ATTRIBUTE,
            "Unable to interpret Query - Dataset name expected after CREATE INDEX ON (invalid name).",
        )
        if not consumer.has_more():
            raise SqlError(
                "Unable to interpret - Column names expected after Dataset name (missing)."
            )
        if not consumer.get()[1] == SQL_TOKENS.LEFTPARENTHESES:
            raise SqlError(
                "Unable to interpret - Column names expected after Dataset name (parenthesis expected)."
            )
        # the next set of tokens are columns
        columns = _acquire_between_brackets(consumer)
        if len(columns) != 1:
            raise SqlError("Unable to interpret - Indexes support one column only.")
        root.config["columns"] = [col[0] for col in columns if col[0] != ","]

    # Syntax: EXPLAIN <query>
    if query_type in ("SELECT", "EXPLAIN SELECT"):
        if consumer.has_more():
            token, poq = consumer.peek()
            root.config["DISTINCT"] = token == "DISTINCT"
            if root.config["DISTINCT"]:
                consumer.next()

        while consumer.has_more():
            # print(consumer.get())

            consumer.next()

    return root


if __name__ == "__main__":
    from opteryx.engine.sql import parser

    tokens = parser.tokenize("SELECT * from table.name")
    tokens = parser.tag(tokens)
    tokens = build_ast(tokens)
    print(tokens)

    tokens = parser.tokenize("analyze table.name")
    tokens = parser.tag(tokens)
    tokens = build_ast(tokens)
    print(tokens)

    tokens = parser.tokenize("create index on table.name (name)")
    tokens = parser.tag(tokens)
    tokens = build_ast(tokens)
    print(tokens)
