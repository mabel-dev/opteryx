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
from opteryx.engine.sql.parser.lexer import get_token_type
from opteryx.engine.sql.parser.constants import SQL_TOKENS


# fmt:off
TOKENS = [

]
# fmt:on

@pytest.mark.parametrize(
    "token, expect",
    # fmt:off
    [
        ("COUNT", SQL_TOKENS.AGGREGATOR),
        ("*", SQL_TOKENS.EVERYTHING),
        ('"2021-12-31"', SQL_TOKENS.TIMESTAMP)
    ],
# fmt:on
)
def test_get_token_type(token, expect):

    received = get_token_type(token)
    assert received == expect, f"Lexer interpreted {token} as a {SQL_TOKENS(received).name}"

