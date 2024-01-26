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

from opteryx.components.query_planner import QueryPlanner

# allows us to run short CI and longer scheduled tests
TEST_ITERATIONS = int(os.environ.get("TEST_ITERATIONS", 100))


@settings(deadline=None, max_examples=TEST_ITERATIONS)
@given(value=st.text(min_size=0))
def test_fuzz_text_parameters(value):
    statement = "SELECT * FROM $planets WHERE name = ? AND id = 0"

    subject_planner = QueryPlanner(statement=statement)
    subject = next(subject_planner.parse_and_lex())
    subject = subject_planner.bind_ast(subject, parameters=[value])

    # plant a safe value
    control_planner = QueryPlanner(statement=statement.replace("?", "'exchanged_value'"))
    control = next(control_planner.parse_and_lex())
    control = control_planner.bind_ast(control, [])
    # manually replace the value
    # fmt:off
    control["Query"]["body"]["Select"]["selection"]["BinaryOp"]["left"]["BinaryOp"]["right"]["Value"]["SingleQuotedString"] = value

    # remove a chunk of the tree that has unique ids inserted during planning
    control["Query"]["body"]["Select"].pop("from")
    subject["Query"]["body"]["Select"].pop("from")
    # fmt:on
    assert control == subject, f"{control}\n\n{subject}"


@settings(deadline=None, max_examples=TEST_ITERATIONS // 10)
@given(value=st.integers())
def test_fuzz_int_parameters(value):
    statement = "SELECT * FROM $planets WHERE name = ? AND id = 0"

    subject_planner = QueryPlanner(statement=statement)
    subject = next(subject_planner.parse_and_lex())
    subject = subject_planner.bind_ast(subject, parameters=[value])

    # plant a safe value
    control_planner = QueryPlanner(statement=statement.replace("?", "10"))
    control = next(control_planner.parse_and_lex())
    control = control_planner.bind_ast(control, [])
    # manually replace the value
    # fmt:off
    control["Query"]["body"]["Select"]["selection"]["BinaryOp"]["left"]["BinaryOp"]["right"]["Value"]["Number"] = [value, False]

    # remove a chunk of the tree that has unique ids inserted during planning
    control["Query"]["body"]["Select"].pop("from")
    subject["Query"]["body"]["Select"].pop("from")
    # fmt:on

    assert control == subject


if __name__ == "__main__":  # pragma: no cover
    test_fuzz_text_parameters("0")
    test_fuzz_int_parameters(10)

    print("✅ okay")
