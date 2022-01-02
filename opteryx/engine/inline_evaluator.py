# no-maintain-checks
# cython: language_level=3
"""
This module is compiled, any changes to it need the following to be run before they
will be effective:

python setup.py build_ext --inplace


This class performs functions on individual rows. There is a set of functions in
the sql_functions module.

We interpret the evaluation line as a set of values, usually made up of Functions
and Variables, the functions may include constants of different types.

e.g.

Evaluator("LEFT(NAME, 1), AGE").evaluate(dic)

will perform the function LEFT on the NAME field from the dict and return AGE
from the dict
"""
import re
from opteryx.engine.functions.inline_functions import FUNCTIONS
from opteryx.utils.dates import parse_iso
from opteryx.utils.token_labeler import TOKENS, get_token_type


class InvalidEvaluator(Exception):
    """custom error"""

    pass


def get_function_name(token):
    """
    Convert tokens back to a function name for inclusion in results
    """

    def _inner(tokens):
        ret = []
        for token in tokens:
            if token["type"] in (TOKENS.FUNCTION, TOKENS.AGGREGATOR):
                ret.append(get_function_name(token))
            else:
                ret.append(token["value"])
        return ret

    if token["type"] in (TOKENS.FUNCTION, TOKENS.AGGREGATOR):
        params = ",".join(_inner(token["parameters"]))
        return f"{token['value']}({params})"
    else:
        return token["value"]


def get_fields(tokens):
    def inner(tokens):
        for token in tokens:
            if token["type"] in (TOKENS.EVERYTHING,):
                yield "*"
            elif token["type"] in (TOKENS.FUNCTION, TOKENS.AGGREGATOR):
                if token["as"]:
                    yield token["as"]
                else:
                    yield get_function_name(token)
            elif token["type"] in (
                TOKENS.VARIABLE,
                TOKENS.INTEGER,
                TOKENS.LITERAL,
                TOKENS.DATE,
                TOKENS.FLOAT,
                TOKENS.DATE,
            ):
                if token["as"]:
                    yield token["as"]
                else:
                    yield token["value"]

    return list(inner(tokens))


def build(tokens):
    response = []
    if not isinstance(tokens, TokenSet):
        ts = TokenSet(tokens)
    else:
        ts = tokens
    while not ts.finished():
        token = ts.token()
        ts.step()  # move along
        if token["type"] in (TOKENS.FUNCTION, TOKENS.AGGREGATOR):

            token["value"] = token["value"].upper()

            if not ts.token()["type"] == TOKENS.LEFTPARENTHESES:
                raise InvalidEvaluator("Invalid expression, missing expected `(` ")
            ts.step()  # step over the (

            # collect all the tokens between the parentheses and 'build' them
            open_parentheses = 1
            collector = []
            while open_parentheses > 0:
                if ts.finished():
                    break

                if ts.token()["type"] == TOKENS.RIGHTPARENTHESES:
                    open_parentheses -= 1
                    if open_parentheses != 0:
                        collector.append(ts.token())
                elif ts.token()["type"] == TOKENS.LEFTPARENTHESES:
                    open_parentheses += 1
                    collector.append(ts.token())
                else:
                    collector.append(ts.token())
                ts.step()

            if open_parentheses != 0:
                raise InvalidEvaluator("Unbalanced parantheses")

            token["parameters"] = build(collector)

        if not ts.finished() and ts.token()["type"] == TOKENS.AS:
            ts.step()
            if ts.finished():
                raise InvalidEvaluator("Incomplete statement after AS")
            token["as"] = ts.token()["value"]
            ts.step()

        response.append(token)
    return response


def evaluate_field(dict, token):
    """
    Evaluate a single field
    """
    token_type = token["type"]
    if token_type == TOKENS.EVERYTHING:
        return ("*", "*")
    if token_type == TOKENS.VARIABLE:
        variable = token["value"]
        if variable[0] == variable[-1] == "`":
            variable = variable[1:-1]
        return (
            token["value"],
            dict.get(variable),
        )
    if token_type == TOKENS.FUNCTION:
        if not token["as"]:
            label = get_fields([token]).pop()
            token["as"] = label
        else:
            label = token["as"]
        return (
            label,
            FUNCTIONS[str(token["value"]).upper()](
                *[evaluate_field(dict, t)[1] for t in token["parameters"]]
            ),
        )
    if token_type == TOKENS.FLOAT:
        return (
            token["value"],
            float(token["value"]),
        )
    if token_type == TOKENS.INTEGER:
        return (
            token["value"],
            int(token["value"]),
        )
    if token_type == TOKENS.LITERAL:
        return (
            token["value"],
            str(token["value"])[1:-1],
        )
    if token_type == TOKENS.DATE:
        return (
            token["value"],
            parse_iso(token["value"][1:-1]),
        )
    if token_type == TOKENS.BOOLEAN:
        return (
            token["value"],
            str(token["value"]).upper() == "TRUE",
        )
    if token_type == TOKENS.NULL:
        return (
            token["value"],
            None,
        )
    return (
        token["value"],
        None,
    )


class TokenSet(list):
    def __init__(self, tokens):
        self._tokens = tokens
        self._index = 0
        self._max = len(tokens)

    def token(self):
        token = self._tokens[self._index]
        if isinstance(token, dict):
            return token
        return {
            "value": token,
            "type": get_token_type(token),
            "parameters": [],
            "as": None,
        }

    def step(self):
        if self._index < self._max:
            self._index += 1

    def next(self):
        self._index += 1
        ret = {"type": None}
        if self._index < self._max:
            ret = self.token()
        self._index -= 1
        return ret

    def finished(self):
        return self._index == self._max


class Evaluator:
    def __init__(self, proforma):
        reg = re.compile(r"(\(|\)|,|\bAS\b)", re.IGNORECASE)
        tokens = [t.strip() for t in reg.split(proforma) if t.strip() not in ("", ",")]
        self.tokens = build(tokens)
        self._iter = None

    def __call__(self, dic):
        builder = {}
        if any(t["type"] == TOKENS.EVERYTHING for t in self.tokens):
            if hasattr(dic, "as_dict"):
                builder = dic.as_dict()
            else:
                builder = dic.copy()
        for field in self.tokens:
            (k, v) = evaluate_field(dic, field)
            builder[k] = v
        return builder

    def __iter__(self):
        return self

    def __next__(self):
        if not self._iter:
            self._iter = iter(self.tokens)
        return next(self._iter)

    def fields(self):
        return get_fields(self.tokens)
