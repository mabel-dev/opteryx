"""
The best way to test a SQL engine is to throw queries at it.

However, we should also unit test the components where possible, this is testing the
Query Abstract Syntax Tree (AST) builder, this is responsible for determining the
contextualizing tokens.

We're going to do that by throwing queries at it.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from opteryx.engine import parser


@pytest.mark.parametrize(
    "statement, expect",
    # fmt:off
    [
        # ANALYZE - there's not a lot of variation here
        ("ANALYZE table", "ROOT [ `ANALYZE` config: {\"dataset\": \"table\"} ]"),
        ("ANALYZE table;", "ROOT [ `ANALYZE` config: {\"dataset\": \"table\"} ]"),
        ("analyze table", "ROOT [ `ANALYZE` config: {\"dataset\": \"table\"} ]"),
        ("analyze\ntable", "ROOT [ `ANALYZE` config: {\"dataset\": \"table\"} ]"),

        # CREATE INDEX ON - again, pretty much no variation
        ("CREATE INDEX ON table.name (name)", 'ROOT [ `CREATE INDEX ON` config: {"dataset": "table.name", "columns": ["name"]} ]'),
        ("create index on table.name (name)", 'ROOT [ `CREATE INDEX ON` config: {"dataset": "table.name", "columns": ["name"]} ]'),
        ("CREATE INDEX ON table.name (name);", 'ROOT [ `CREATE INDEX ON` config: {"dataset": "table.name", "columns": ["name"]} ]'),
        ("CREATE INDEX ON table.name\n\t(select);", 'ROOT [ `CREATE INDEX ON` config: {"dataset": "table.name", "columns": ["select"]} ]'),
    ],
    # fmt:on
)
def test_ast_builder(statement, expect):
    """
    Test an assortment of statements
    """
    tokens = parser.tokenize(statement)
    tokens = parser.tag(tokens)
    tokens = parser.build_ast(tokens)

    assert (
        str(tokens) == expect
    ), f'AST interpreted ""{statement}"" as ""{str(tokens)}""'
