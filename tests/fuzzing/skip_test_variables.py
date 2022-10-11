"""
Fuzzing allows us to test a lot more variations than we would if we were to write
all test cases by hand.

Parameterization is the most likely place to introduce security weaknesses, so this
is one of the initial targets for fuzzing.
"""
import os
import sys


sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import string

import hypothesis.strategies as st
from hypothesis import given, settings

import opteryx

# allows us to run short CI and longer scheduled tests
TEST_ITERATIONS = int(os.environ.get("TEST_ITERATIONS", 100))

names = st.text(alphabet=string.ascii_letters, min_size=1)


@settings(deadline=None, max_examples=TEST_ITERATIONS)
@given(name=names, value=st.text(alphabet=string.printable))
def test_fuzz_variables(name, value):

    # we know these fail
    failures = ("'", "\\", "\r", "\n", "\t", "\x0b", "\x0c", "--")
    if any(f in value for f in failures):  # pragma: no cover
        return

    statement = f"SET @{name} = '{value}'; SELECT @{name};"
    #    print(statement.encode())

    conn = opteryx.connect()
    curr = conn.cursor()
    curr.execute(statement)
    #    result = curr.fetchall()
    #    print(result)
    result = curr.arrow().to_pylist()
    #    print(result)

    #    print(name, value, result)

    assert next(iter(result[0].values())) == value, result


if __name__ == "__main__":  # pragma: no cover

    print(string.ascii_letters)

    test_fuzz_variables("a", "b")
    test_fuzz_variables("val", "\x00")

    print("✅ okay")
