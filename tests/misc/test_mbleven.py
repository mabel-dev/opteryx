import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from opteryx.third_party.mbleven import compare

# fmt:off
TESTS = [
    ("abc", "abc", 0),

    ("abc", "xabc", 1),
    ("abc", "axbc", 1),
    ("abc", "abxc", 1),
    ("abc", "abcx", 1),
    ("abc", "xxabc", 2),
    ("abc", "axxbc", 2),
    ("abc", "abxxc", 2),
    ("abc", "abcxx", 2),

    ('abc', 'xbc', 1),
    ('abc', 'axc', 1),
    ('abc', 'abx', 1),
    ('abc', 'xxc', 2),
    ('abc', 'axx', 2),
    ('abc', 'xbx', 2),

    ('abc', 'ab', 1),
    ('abc', 'ac', 1),
    ('abc', 'bc', 1),
    ('a', 'abc', 2),
    ('b', 'abc', 2),
    ('c', 'abc', 2),

    ('abcde', 'eabcd', 2),
    ('abcde', 'acdeb', 2),
    ('abcde', 'abdec', 2),
    ('ababa', 'babab', 2),

    ('', '', 0),
    ('', 'a', 1),
    ('', 'ab', 2),
    ('', 'abc', -1),
    ('abc', '', -1),

    ('abc', 'def', -1)
]
# fmt:on


@pytest.mark.parametrize("str1, str2, score", TESTS)
def test_level_battery(str1, str2, score):
    actual_score = compare(str1, str2)
    assert score == actual_score, f"expected: {score}, got: {actual_score}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} TESTS")
    for str1, str2, score in TESTS:
        test_level_battery(str1, str2, score)

    print("✅ okay")
