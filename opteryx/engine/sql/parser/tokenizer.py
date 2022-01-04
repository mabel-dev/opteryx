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

This is the Tokenizer, it is responsible for breaking a Query into lexemes/morphemes,
that is into individual tokens (words) that can carry meaning. It is the Lexer's role
to assign meaning.

The Tokenizer does little more than split the Query by whitespace - accounting for
word groups which have meaning together (e.g. "ORDER BY") and for string delimiters
(e.g. tokens are created by putting quotes around text).
"""
import re

from opteryx.exceptions import ProgrammingError
from opteryx.engine.functions import FUNCTIONS
from opteryx.engine.aggregators.aggregators import AGGREGATORS
from opteryx.engine.sql.parser.constants import OPERATORS

__all__ = ["tokenize"]

# These are the characters we should escape in our regex
REGEX_CHARACTERS = {ch: "\\" + ch for ch in ".^$*+?{}[]|()\\"}


class TokenError(Exception):
    """
    Custom Error Type
    """

    pass


def _build_splitter():
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
    for item in [  # reserved words with whitespace in them
        r"GROUP\sBY",
        r"ORDER\sBY",
        r"INNER\sJOIN",
        r"OUTER\sJOIN",
        r"CREATE\sINDEX\sON",
        r"USING\sSAMPLE",
        r"IS\sNOT",
        r"NOT\sLIKE",
        r"NOT\sIN",
        r"EXPLAIN\sSELECT",
        r"SIMILAR\sTO",
    ]:
        keywords.append(r"\b" + item + r"\b")
    for item in ("(", ")", "[", "]", ",", "*", ";"):
        keywords.append("".join([REGEX_CHARACTERS.get(ch, ch) for ch in item]))
    splitter = re.compile(
        r"(" + r"|".join(keywords) + r"|\s)",
        re.IGNORECASE,
    )
    return splitter


def _fix_special_chars(tokens):
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

    if builder:
        raise ProgrammingError("Unable to parse Query, check for missing quotes.")


def _remove_comments(string):
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


def _clean_statement(string):
    """
    Remove carriage returns and all whitespace to single spaces
    """
    whitespace_cleaner = re.compile(r"\s+")
    return whitespace_cleaner.sub(" ", string).strip()


def tokenize(expression):

    expression = _remove_comments(expression)
    expression = _clean_statement(expression)
    tokens = _build_splitter().split(expression)
    # characters like '*' in literals break the tokenizer, so we need to fix them
    tokens = list(_fix_special_chars(tokens))
    tokens = [t.strip() for t in tokens if t.strip() != ""]

    # ; is used to terminate a SQL Statement
    try:
        end_token = tokens.index(";")
        if end_token:
            tokens = tokens[:end_token]
    except ValueError:
        pass

    return tokens
