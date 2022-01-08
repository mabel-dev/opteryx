"""
The best way to test a SQL engine is to throw queries at it.

However, we should also unit test the components where possible, this is testing the
Query Lexer, this is responsible for determining the meaning of each token.

We're going to do that by throwing queries at it.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from opteryx.engine import parser
from opteryx.engine.parser.constants import SQL_TOKENS


@pytest.mark.parametrize(
    "token, expect",
    # fmt:off
    [
        # CONSTANT RECOGNISION
        ("*", SQL_TOKENS.EVERYTHING),
        ('"2021-12-31"', SQL_TOKENS.TIMESTAMP),
        ('"2021-12-31T00:00"', SQL_TOKENS.TIMESTAMP),
        ('"2021-12-31 00:00"', SQL_TOKENS.TIMESTAMP),
        ('"2021-12-31T00:00:00Z"', SQL_TOKENS.TIMESTAMP),
        ('"2021/12/31"', SQL_TOKENS.LITERAL),
        ("'2'", SQL_TOKENS.LITERAL),
        ("'word'", SQL_TOKENS.LITERAL),
        ("2", SQL_TOKENS.INTEGER),
        ("-2", SQL_TOKENS.INTEGER),
        ("2.0", SQL_TOKENS.DOUBLE),
        ("-2.0", SQL_TOKENS.DOUBLE),
        ("true", SQL_TOKENS.BOOLEAN),
        ("True", SQL_TOKENS.BOOLEAN),
        ("TRUE", SQL_TOKENS.BOOLEAN),
        ("Null", SQL_TOKENS.NULL),

        # KEYWORD RECOGNISION
        ("select", SQL_TOKENS.KEYWORD),
        ("SELECT", SQL_TOKENS.KEYWORD),
        ("CREATE", SQL_TOKENS.ATTRIBUTE), # <- it is a variable
        ("CREATE INDEX ON", SQL_TOKENS.KEYWORD),
        ("WHERE", SQL_TOKENS.KEYWORD),
        ("EXPLAIN SELECT", SQL_TOKENS.KEYWORD),
        ("`SELECT`", SQL_TOKENS.ATTRIBUTE),
        
        # OPERATORS
        ("LIKE", SQL_TOKENS.OPERATOR),
        ("NOT LIKE", SQL_TOKENS.OPERATOR),
        ("<", SQL_TOKENS.OPERATOR),
        ("<=", SQL_TOKENS.OPERATOR),

        # FUNCTION & AGG RECOGNITION
        ("COUNT", SQL_TOKENS.AGGREGATOR),
        ("AVG", SQL_TOKENS.AGGREGATOR),
        ("DATE", SQL_TOKENS.FUNCTION),
        ("DOUBLE", SQL_TOKENS.FUNCTION),
        ("count", SQL_TOKENS.AGGREGATOR),
        ("date", SQL_TOKENS.FUNCTION),
        ("`COUNT`", SQL_TOKENS.ATTRIBUTE),

        # PUNCTUATION
        ("(", SQL_TOKENS.LEFTPARENTHESES),
        (")", SQL_TOKENS.RIGHTPARENTHESES),
        (",", SQL_TOKENS.COMMA),
        ("{", SQL_TOKENS.LEFTSTRUCT),
        ("[", SQL_TOKENS.LEFTPARENTHESES),

        # MISC
        ("AND", SQL_TOKENS.AND),
        ("and", SQL_TOKENS.AND),
        ("NOT", SQL_TOKENS.NOT),
    ],
    # fmt:on
)
def test_individual_tokens(token, expect):
    """
    Test an assortment of token types
    """

    received = parser.get_token_type(token)
    assert (
        received == expect
    ), f"Lexer interpreted {token} as a {SQL_TOKENS(received).name}"
