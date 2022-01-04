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
        # functions and aggregators
        self.parameters = []
        # if the value is different to the token (lists and structs)
        self.value = None
        # node config
        self.config = {}

    def __str__(self):
        import json

        def _inner(node, prefix=''):
            repr = f"{prefix}{node.token} [ `{node.node_type}`{('AS ' + node.alias) if node.alias else ''} ]\n"
            for child in node.children:
                repr += _inner(child, prefix + '    ')
            return repr

        return _inner(self).rstrip("\n")


def _aquire_expect(consumer, poq, error):
    """
    This is a simple helper function to replace have to write code to get and test the
    PoQ (Part of Query) of tokens every time.
    """
    token, _poq = consumer.get()
    if _poq != poq:
        raise SqlError(error)
    return token


def build_ast(tokens):

    # create a list consumer
    consumer = ListConsumer(tokens)

    # get the first token, it defines the type of query
    query_type, token_type = consumer.get()
    if query_type not in ("SELECT", "EXPLAIN SELECT", "CREATE INDEX ON", "ANALYZE"):
        raise SqlError("Unable to determine the type of Query")

    # create the root node, it's type is the first token
    root = AstNode("ROOT", query_type)

    # Syntax: ANALYZE <dataset>
    # if we're an analyze query, we expect a single token which is a dataset name
    if query_type == "ANALYZE":
        if consumer.has_more():
            consumer.next()
            root.config["dataset"] = _aquire_expect(
                consumer,
                SQL_TOKENS.ATTRIBUTE,
                "Unable to interpret Query - Expected Dataset name after ANALYZE",
            )

    # Syntax: CREATE INDEX ON <dataset> (<attribute_list>)
    # We're going to extract out the attribute list even though we currently only
    # support having one, for both future options and for meaningful error messages.
    if query_type == "CREATE INDEX ON":
        if consumer.has_more():
            consumer.next()
            root.config["dataset"] = _aquire_expect(
                consumer,
                SQL_TOKENS.ATTRIBUTE,
                "Unable to interpret Query - Expected Dataset name after CREATE INDEX ON"
            )

    # Syntax: EXPLAIN <query>
    if query_type in ("SELECT", "EXPLAIN SELECT"):
        if consumer.has_more():
            token, poq = consumer.peek()
            root.config["DISTINCT"] = token == "DISTINCT"
            if root.config["DISTINCT"]:
                consumer.next()

        while consumer.has_more():
            print(consumer.get())

            consumer.next()

    return root


if __name__ == "__main__":
    from opteryx.engine.sql import parser

    tokens = parser.tokenize("SELECT * from table.name")
    tokens = parser.tag(tokens)

    tokens = build_ast(tokens)

    print(tokens)
