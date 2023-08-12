import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.third_party.levenshtein import levenshtein

# fmt:off
TESTS = [
    # equal strings
    ("hello", "hello", 0),
    ("world", "world", 0),

    # single char difference
    ("hello", "hallo", 1),
    ("jupiter", "jupter", 1),

    # completely different strings
    ("abcd", "efgh", 4),
    ("test", "wxyz", 4),

    # one string is empty
    ("", "abcd", 4),
    ("efgh", "", 4),

    # both strings are empty
    ("", "", 0),

    # long strings
    ("thisisaverylongstring", "thisisaverylongstring", 0),
    ("thisisaverylongstring", "thisisadifferentlongstring", 7),

    # single char at different positions
    ("hello", "aello", 1),
    ("hello", "hallo", 1),
    ("hello", "helao", 1),
    ("hello", "hellx", 1),

    # multiple char differences
    ("test", "text", 1),
    ("test", "txst", 1),
    ("test", "txxt", 2),

    # string with spaces
    ("hello world", "hello wrrld", 1),
    ("hello world", "hxllo world", 1),
    ("hello world", "hello w", 4),

    # strings with numbers
    ("12345", "12346", 1),
    ("12345", "1234", 1),
    ("12345", "123", 2),
    ("12345", "12", 3),
    ("12345", "1", 4),

    # strings with punctuation marks
    ("hello, world!", "hello, world?", 1),  # comma replaced with question mark
    ("hello. world!", "hello, world!", 1),  # period replaced with comma
    ("hello! world?", "hello world!", 2),  # exclamation mark removed
    ("hello, world!", "hello, world!!", 1),  # extra exclamation mark
    ("hello# world!", "hello, world!", 1),  # hash replaced with comma

    # strings with spaces at different locations
    ("hello world", "helloworld", 1),  # space removed
    ("hello world", "hello  world", 1),  # extra space
    ("hello world", " hello world", 1),  # space added at the beginning
    ("hello world", "hello world ", 1),  # space added at the end
    ("hello world", "h e l l o world", 4),  # spaces added in between characters
]
# fmt:on


@pytest.mark.parametrize("a, b, distance", TESTS)
def test_levenshtien_battery(a, b, distance):
    calculated_distance = levenshtein(a, b)
    assert (
        calculated_distance == distance
    ), f"for {a}/{b} - expected: '{distance}', got: '{calculated_distance}'"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} TESTS")
    for a, b, distance in TESTS:
        test_levenshtien_battery(a, b, distance)

    print("✅ okay")