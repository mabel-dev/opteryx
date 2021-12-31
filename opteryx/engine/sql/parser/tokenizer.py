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

"""


from functools import lru_cache
import re

import fastnumbers
from opteryx.exceptions import ProgrammingError

from opteryx.utils.text import like, not_like, matches
from opteryx.utils.dates import parse_iso
from opteryx.engine.functions import FUNCTIONS
from opteryx.engine.aggregators.aggregators import AGGREGATORS
from opteryx.engine.sql.parser.constants import SQL_TOKENS, OPERATORS

# These are the characters we should escape in our regex
REGEX_CHARACTERS = {ch: "\\" + ch for ch in ".^$*+?{}[]|()\\"}


class TokenError(Exception):
    pass


def interpret_value(value):
    if not isinstance(value, str):
        return value
    if value.upper() in ("TRUE", "FALSE"):
        return value.upper() == "TRUE"
    try:
        # there appears to be a race condition with this library
        # so wrap in a SystemError
        num = fastnumbers.fast_real(value)
        if isinstance(num, (int, float)):
            return num
    except SystemError:
        pass
    value = value[1:-1]
    return parse_iso(value) or value


@lru_cache(1)
def build_splitter():
    # build the regex by building a list of all of the keywords
    keywords = []
    for item in FUNCTIONS:
        keywords.append(r"\b" + item + r"\b")
    for item in AGGREGATORS:
        keywords.append(r"\b" + item + r"\b")
    for item in OPERATORS:
        if item.replace(" ", "").isalpha():
            keywords.append(r"\b" + item + r"\b")
        else:
            keywords.append("".join([REGEX_CHARACTERS.get(ch, ch) for ch in item]))
    for item in [
        "AND",
        "OR",
        "NOT",
        "SELECT",
        "FROM",
        "WHERE",
        "LIMIT",
        r"GROUP\sBY",
        r"ORDER\sBY",
        "JOIN",
        r"INNER\sJOIN",
        r"OUTER\sJOIN",
        "DISTINCT",
        "ASC",
        "DESC",
        "IN",
        "ANALYZE",
        "EXPLAIN",
        "NOOPT",
        "CREATE\sINDEX\sON",
    ]:
        keywords.append(r"\b" + item + r"\b")
    for item in ("(", ")", "[", "]", ",", "*"):
        keywords.append("".join([REGEX_CHARACTERS.get(ch, ch) for ch in item]))
    splitter = re.compile(
        r"(" + r"|".join(keywords) + r"|\s)",
        re.IGNORECASE,
    )
    return splitter


class Tokenizer:

    __slots__ = ("i", "tokens")

    def __init__(self, exp):
        self.i = 0
        if isinstance(exp, str):
            self.tokens = self.tokenize(exp)
        else:
            self.tokens = exp

    def token_at_index(self, index):
        return self.tokens[index]

    def next(self):
        self.i += 1
        return self.tokens[self.i - 1]

    def peek(self):
        return self.tokens[self.i]

    def has_next(self):
        return self.i < len(self.tokens)

    def next_token_value(self):
        return self.tokens[self.i]

    def _fix_special_chars(self, tokens):
        """
        The splitter will cut quoted tokens if they contain characters or strings
        we generally want to split on - quotes can be used to say, this isn't an
        instance of that character to split on, so we join these quoted strings
        back together.
        """
        builder = ""
        looking_for_end_char = None

        for token in tokens:
            stripped_token = token.strip()

            if token == " " and looking_for_end_char:
                # we're building a token
                builder += token

            elif len(stripped_token) == 0:
                # the splitter creates unwanted empty strings
                pass

            elif not looking_for_end_char and stripped_token[0] not in ('"', "'", "`"):
                # nothing interesting here
                yield token

            elif (
                stripped_token[0] in ('"', "'", "`")
                and stripped_token[-1] == stripped_token[0]
                and len(stripped_token) > 1
            ):
                # the quotes wrap the entire token
                yield token

            elif stripped_token[-1] == looking_for_end_char:
                # we've found the end of the token, yield it and reset
                builder += token
                yield builder
                builder = ""
                looking_for_end_char = None

            elif stripped_token[0] in ('"', "'", "`") and (
                stripped_token[-1] != stripped_token[0] or len(stripped_token) == 1
            ):
                # we've found a new token to collect
                # the last character will always equal the last character if there's only one
                builder = token
                looking_for_end_char = stripped_token[0]

            elif looking_for_end_char:
                # we're building a token
                builder += token

            else:
                raise TokenError(
                    "Unable to determine quoted token boundaries, you may be missing a closing quote."
                )

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

    def clean_statement(self, string):
        """
        Remove carriage returns and all whitespace to single spaces
        """
        whitespace_cleaner = re.compile(r"\s+")
        return whitespace_cleaner.sub(" ", string).strip()

    def tokenize(self, expression):

        expression = self.remove_comments(expression)
        expression = self.clean_statement(expression)
        tokens = build_splitter().split(expression)
        # characters like '*' in literals break the tokenizer, so we need to fix them
        tokens = list(self._fix_special_chars(tokens))
        tokens = [t.strip() for t in tokens if t.strip() != ""]
        return tokens

    def __str__(self):
        return self.peek()
