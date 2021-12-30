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
Tokenizer -> Lexer -> Planner

Tokenizer deconstructs a string into it's parts
Lexer Interprets the tokens
Planner creates a naive plan for the query

We can interpret the following:

    *SELECT ...

    *ANALYZE dataset

    *EXPLAIN [NOOPT] query -> returns the plan for a query

    *CREATE INDEX index_name ON dataset (attribute1)
"""
import re
import fastnumbers
from opteryx.utils.dates import parse_iso
from opteryx.engine.functions import FUNCTIONS
from opteryx.engine.sql.parser.constants import SQL_TOKENS, OPERATORS
from opteryx.engine.aggregators.aggregators import AGGREGATORS


def _case_correction(tokens):
    for token in tokens:
        if get_token_type(token) in (
            SQL_TOKENS.LITERAL,
            SQL_TOKENS.VARIABLE,
            SQL_TOKENS.SUBQUERY,
        ):
            yield token
        else:
            yield token.upper()


def get_token_type(token):
    """
    Guess the token type.
    """
    token = str(token).strip()
    token_upper = token.upper()
    if len(token) == 0:
        return SQL_TOKENS.EMPTY
    if token[0] == token[-1] == "`":
        # tokens in ` quotes are variables, this is how we supersede all other
        # checks, e.g. if it looks like a number but is a variable.
        return SQL_TOKENS.VARIABLE
    if token == "*":  # nosec - not a password
        return SQL_TOKENS.EVERYTHING
    if token_upper in FUNCTIONS:
        return SQL_TOKENS.FUNCTION
    if token_upper in OPERATORS:
        return SQL_TOKENS.OPERATOR
    if token_upper in AGGREGATORS:
        return SQL_TOKENS.AGGREGATOR
    if token[0] == token[-1] == '"' or token[0] == token[-1] == "'":
        # tokens in quotes are either dates or string literals, if we can
        # parse to a date, it's a date
        if parse_iso(token[1:-1]):
            return SQL_TOKENS.TIMESTAMP
        else:
            return SQL_TOKENS.LITERAL
    if fastnumbers.isint(token):
        return SQL_TOKENS.INTEGER
    if fastnumbers.isfloat(token):
        return SQL_TOKENS.DOUBLE
    if token in ("(", "["):
        return SQL_TOKENS.LEFTPARENTHESES
    if token in (")", "]"):
        return SQL_TOKENS.RIGHTPARENTHESES
    if re.search(r"^[^\d\W][\w\-\.]*", token):
        if token_upper in ("TRUE", "FALSE"):
            # 'true' and 'false' without quotes are booleans
            return SQL_TOKENS.BOOLEAN
        if token_upper in ("NULL", "NONE"):
            # 'null' or 'none' without quotes are nulls
            return SQL_TOKENS.NULL
        if token_upper == "AND":  # nosec - not a password
            return SQL_TOKENS.AND
        if token_upper == "OR":  # nosec - not a password
            return SQL_TOKENS.OR
        if token_upper == "NOT":  # nosec - not a password
            return SQL_TOKENS.NOT
        if token_upper == "AS":  # nosec - not a password
            return SQL_TOKENS.AS
        # tokens starting with a letter, is made up of letters, numbers,
        # hyphens, underscores and dots are probably variables. We do this
        # last so we don't miss assign other items to be a variable
        return SQL_TOKENS.VARIABLE
    # at this point, we don't know what it is
    return SQL_TOKENS.UNKNOWN
