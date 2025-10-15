"""
Fuzzing allows us to test a lot more variations than we would if we were to write
all test cases by hand.

We're testing string functions (in this particular case, HASH) can accept fuzzed
inputs, HASH was chosen because it's a single parameter function.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import hypothesis.strategies as st
from hypothesis import given, settings

import opteryx

# allows us to run short CI and longer scheduled tests
TEST_ITERATIONS = int(os.environ.get("TEST_ITERATIONS", 100))

literals = st.text(min_size=1)


@settings(deadline=None, max_examples=TEST_ITERATIONS)
@given(literal=literals)
def test_fuzz_literals(literal):
    # single quote is the delimiter, it's not a bug that we think a delimeter
    # mid string indicates the end of the string
    literal = literal.replace("'", "#")

    # these ones are a bug
    literal = literal.replace("\\", "")
    literal = literal.replace("--", "")

    statement = f"SELECT HASH('{literal}');"
    print(statement)

    conn = opteryx.connect()
    curr = conn.cursor()
    curr.execute(statement)
    curr.arrow()


if __name__ == "__main__":  # pragma: no cover
    test_fuzz_literals()

    print("✅ okay")
