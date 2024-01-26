"""
Some tests created with assistance from ChatGTP
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from hypothesis import given
from hypothesis.strategies import text

from opteryx.third_party.mbleven import compare

# fmt:off
TESTS = [

    ("abc", "dbc", 1),  # one replace operation required
    ("abcd", "dabc", 2),  # two operations required
    ("abcde", "fghij", -1),  # strings too different to be transformable
    ("abccde", "abbde", 2),  
    ("hello", "heyyo", 2),  # two operations required to change "l" to "y"

    # empty strings
    ("", "", 0),

    # equal strings
    ("hello", "hello", 0),
    ("world", "world", 0),

    # single char difference
    ("hello", "hallo", 1),
    ("jupiter", "jupter", 1),

    # multiple char differences
    ("hello", "allo", 2),
    ("zebra", "xero", -1),

    # swapped chars
    ("bat", "tab", 2),
    ("fred", "erfd", 2),

    # different lengths
    ("hello", "ello", 1),
    ("bob", "bobcat", -1),

    # difference 2
    ("string", "thing", 2),
    ("blooper", "bloop", 2),

    # upper and lower case
    ("HELLO", "hello", 0),
    ("wORld", "WorLD", 0),

    # special characters
    ("hello", "h@llo", 1),
    ("!@$", "!$@", 2),

    # non-ascii characters
    ("café", "cafe", 1),
    ("über", "uber", 1),

    # string with spaces
    ("hello world", "hello  world", 1),
    ("foo bar", "foo  bar", 1),

    # string with newlines
    ("hello\nworld", "hello\n worl", 2),
    ("foo\nbar\nbaz", "foo\nbaz", -1),

    # string with tabs
    ("hello\tworld", "hello\t worl", 2),
    ("foo\tbar\tbaz", "foo\tbaz", -1),

    # string with line breaks
    ("hello\r\nworld", "hello\r worl", 2),
    ("foo\r\nbar\r\nbaz", "foo\r\nbaz", -1),

    # same-length string with alternating characters
    ("01010101", "10101010", 2),
    ("qwqwqwqw", "wqwqwq", 2),

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

    ('abc', 'def', -1),

    ("kitten", "sittin", 2),
    ("a", "", 1),
    ("mambo", "jumbo", 2),
    ("pizza", "pizzeria", -1),
]
# fmt:on


@pytest.mark.parametrize("str1, str2, score", TESTS)
def test_level_battery(str1, str2, score):
    actual_score = compare(str1, str2)
    assert score == actual_score, f"{str1} -> {str2}, expected: {score}, got: {actual_score}"


@given(text(), text())
def test_compare_symmetric(str1, str2):
    assert compare(str1, str2) == compare(str2, str1)


@given(text())
def test_compare_equal_strings(s):
    assert compare(s, s) == 0


@given(text())
def test_compare_add_char(s):
    assert compare(s, s + "a") == 1


@given(text())
def test_compare_delete_char(s):
    if len(s) > 0:
        assert compare(s, s[:-1]) == 1


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} TESTS")
    for str1, str2, score in TESTS:
        print("\033[38;2;26;185;67m.\033[0m", end="")
        test_level_battery(str1, str2, score)

    print("")
    print("✅ okay")
