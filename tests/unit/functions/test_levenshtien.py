import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import numpy
import pytest

from opteryx.compiled.list_ops import list_levenshtein

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

    # strings with special characters
    ("hello@world", "hello#world", 1),  # at replaced with hash
    ("hello@world", "hello world", 1),  # at replaced with space
    ("hello@world", "hello@wor1d", 1),  # l replaced with 1

    # strings with mixed case
    ("HelloWorld", "helloworld", 2),  # H replaced with h
    ("HelloWorld", "Helloworld", 1),  # W replaced with w
    ("HelloWorld", "helloworld!", 3),  # H replaced with h and exclamation mark added

    # strings with unicode characters
    ("hello", "héllo", 1),  # e replaced with é
    ("hello", "hellö", 1),  # o replaced with ö
    ("你好", "你号", 1),  # 好 replaced with 号
    ("こんにちは", "こんばんは", 2),  # こん replaced with こん and は replaced with ば
    ("hello world", "hello world ", 1),  # space added at the end
    ("hello world", "h e l l o world", 4),  # spaces added in between characters

    # additional edge cases: reversed strings
    ("abc", "cba", 2),  # reverse requires 2 operations
    ("abcd", "dcba", 4),  # reverse of 4-char string

    # additional edge cases: repeated characters
    ("aaa", "aaa", 0),  # identical repeated chars
    ("aaa", "aaaa", 1),  # one extra repeated char
    ("aaaa", "aaa", 1),  # one less repeated char
    ("aaabbb", "bbbaaa", 6),  # reversed repeated chars

    # additional edge cases: single character strings
    ("a", "a", 0),  # same single char
    ("a", "b", 1),  # different single char
    ("a", "", 1),  # single char vs empty

    # additional edge cases: strings with control characters
    ("hello\x00world", "hello\x00world", 0),  # null byte preserved
    ("hello\x00world", "helloworld", 1),  # null byte removed
    ("a\r\nb", "a\nb", 1),  # CRLF to LF
    ("a\tb\tc", "a b c", 2),  # tabs to spaces

    # additional edge cases: strings with emojis
    ("😀😁😂", "😀😁😂", 0),  # same emoji sequence
    ("😀😁😂", "😀😂😁", 2),  # swapped emojis
    ("hello😀", "hello😁", 1),  # different emoji at end
    ("😀", "a", 1),  # emoji to letter

    # additional edge cases: very long strings
    ("a" * 50, "a" * 50, 0),  # identical long strings
    ("a" * 50, "a" * 49, 1),  # one char difference in length
    ("a" * 50, "a" * 49 + "b", 1),  # one char replacement in long string
    ("a" * 50, "b" * 50, 50),  # all chars different in long string

    # additional edge cases: prefix and suffix variations
    ("prefix_suffix", "prefix_suffix", 0),  # same prefix/suffix
    ("prefix_suffix", "suffix", 7),  # remove prefix
    ("prefix_suffix", "prefix", 7),  # remove suffix
    ("test", "testing", 3),  # string is prefix of another
    ("testing", "test", 3),  # string is suffix of another

    # additional edge cases: common typos
    ("receive", "recieve", 2),  # i/e swap
    ("occurred", "occured", 1),  # double to single
    ("definitely", "definately", 1),  # a/i swap
    ("separate", "seperate", 1),  # a/e swap

    # additional edge cases: mixed operations
    ("kitten", "sitting", 3),  # classic example (substitute k->s, substitute e->i, insert g)
    ("saturday", "sunday", 3),  # multiple operations
    ("book", "back", 2),  # middle chars differ

    # additional edge cases: numeric and alphanumeric
    ("abc123", "abc123", 0),  # same alphanumeric
    ("abc123", "abc124", 1),  # one digit different
    ("123abc", "abc123", 6),  # reversed alphanumeric
    ("v1.2.3", "v1.2.4", 1),  # version string difference

    # additional edge cases: case variations
    ("ABC", "abc", 3),  # all uppercase to lowercase
    ("AbC", "aBc", 3),  # mixed case differences
    ("HELLO", "hello", 5),  # all caps to all lowercase
]
# fmt:on


@pytest.mark.parametrize("a, b, distance", TESTS)
def test_levenshtien_battery(a, b, distance):
    calculated_distance = list_levenshtein(numpy.array([a], dtype=object), numpy.array([b], dtype=object))[0]
    assert (
        calculated_distance == distance
    ), f"for {a}/{b} - expected: '{distance}', got: '{calculated_distance}'"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} TESTS")
    for a, b, distance in TESTS:
        test_levenshtien_battery(a, b, distance)
        print("\033[38;2;26;185;67m.\033[0m", end="")

    print()
    print("✅ okay")
