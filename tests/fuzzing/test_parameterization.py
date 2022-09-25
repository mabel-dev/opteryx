"""
Fuzzing allows us to test a lot more variations than we would if we were to write
all test cases by hand.

Parameterization is the most likely place to introduce security weaknesses, so this
is one of the initial targets for fuzzing.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import hypothesis.strategies as st
from hypothesis import given, settings

from opteryx.managers.planner import QueryPlanner


@settings(deadline=None)
@given(value=st.text(min_size=0))
def test_fuzz_text_parameters(value):

    statement = "SELECT * FROM $planets WHERE name = ? AND id = 0"

    subject = QueryPlanner(statement=statement)
    subject.parse_and_lex()
    subject.bind_ast([value])

    # plant a safe value
    control = QueryPlanner(statement=statement.replace("?", "'exchanged_value'"))
    control.parse_and_lex()
    control.bind_ast([])
    # manually replace the value
    # fmt:off
    control.ast[0]["Query"]["body"]["Select"]["selection"]["BinaryOp"]["left"]["BinaryOp"]["right"]["Value"]["SingleQuotedString"] = value
    # fmt:on
    assert control.ast == subject.ast


@settings(deadline=None)
@given(value=st.integers())
def test_fuzz_int_parameters(value):

    statement = "SELECT * FROM $planets WHERE name = ? AND id = 0"

    subject = QueryPlanner(statement=statement)
    subject.parse_and_lex()
    subject.bind_ast([value])

    # plant a safe value
    control = QueryPlanner(statement=statement.replace("?", "10"))
    control.parse_and_lex()
    control.bind_ast([])
    # manually replace the value
    # fmt:off
    control.ast[0]["Query"]["body"]["Select"]["selection"]["BinaryOp"]["left"]["BinaryOp"]["right"]["Value"]["Number"] = [value, False]
    # fmt:on

    assert control.ast == subject.ast


if __name__ == "__main__":  # pragma: no cover

    test_fuzz_text_parameters()

    print("✅ okay")
