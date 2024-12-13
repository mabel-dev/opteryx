import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.utils import sql


# fmt:off
TEST_CASES = [
    # Basic patterns
    ("a%", r"^a.*?$"),
    ("%a", r"^.*?a$"),
    ("%a%", r"^.*?a.*?$"),
    ("a_b", r"^a.b$"),
    ("a__", r"^a..$"),
    ("_", r"^.$"),
    ("__", r"^..$"),

    # Escaping special regex characters
    ("a.b", r"^a\.b$"),
    ("[abc]", r"^\[abc\]$"),
    ("(test)", r"^\(test\)$"),
    ("a+b", r"^a\+b$"),
    ("a*b", r"^a\*b$"),
    ("a^b", r"^a\^b$"),
    ("a$b", r"^a\$b$"),
    ("a|b", r"^a\|b$"),
    ("a\\b", r"^a\\b$"),
    ("{test}", r"^\{test\}$"),

    # Mixed wildcards and special characters
    ("%a.b%", r"^.*?a\.b.*?$"),
    ("a_b%", r"^a.b.*?$"),
    ("%a_b%", r"^.*?a.b.*?$"),
    ("a%[test]%b", r"^a.*?\[test\].*?b$"),
    ("_%[abc]", r"^..*?\[abc\]$"),
    ("%a+b%", r"^.*?a\+b.*?$"),
    ("a%b_", r"^a.*?b.$"),

    # Patterns with only wildcards
    ("%", r"^.*?$"),
    ("__", r"^..$"),
    ("_%", r"^..*?$"),
    ("_%_", r"^..*?.$"),
    ("%_%", r"^.*?..*?$"),

    # Patterns with no special characters
    ("test", r"^test$"),
    ("abc", r"^abc$"),
    ("hello world", r"^hello world$"),
    ("123", r"^123$"),

    # Empty pattern
    ("", r"^$"),

    # Edge cases with spaces
    (" a%", r"^ a.*?$"),
    ("% a", r"^.*? a$"),
    (" a b ", r"^ a b $"),

    # Multiple wildcards
    ("a%%b", r"^a.*?.*?b$"),
    ("%a_b%", r"^.*?a.b.*?$"),
    ("a%_%b", r"^a.*?..*?b$"),
    ("%_%_%", r"^.*?..*?..*?$"),

    # Patterns with underscores
    ("_", r"^.$"),
    ("__abc", r"^..abc$"),
    ("%__%", r"^.*?...*?$"),
    ("abc_", r"^abc.$"),
    ("_%_", r"^..*?.$"),

    # Patterns with numeric characters
    ("123%", r"^123.*?$"),
    ("12_3", r"^12.3$"),
    ("%1_2%", r"^.*?1.2.*?$"),

    # Patterns with mixed characters
    ("a1_b2%", r"^a1.b2.*?$"),
    ("a_b%c_d%", r"^a.b.*?c.d.*?$"),
    ("%a%1%", r"^.*?a.*?1.*?$"),
    ("_a%_b", r"^.a.*?.b$"),

    # Patterns with special regex characters and wildcards
    ("%a(b)%", r"^.*?a\(b\).*?$"),
    ("[test]_%", r"^\[test\]..*?$"),
    ("a{1,3}_b", r"^a\{1,3\}.b$"),
    ("(a|b)%", r"^\(a\|b\).*?$")
]
# fmt:on


@pytest.mark.parametrize("like_pattern, re_pattern", TEST_CASES)
def test_like_to_regex(like_pattern, re_pattern):
    converted = sql.sql_like_to_regex(like_pattern)
    assert converted == re_pattern, f"{like_pattern} -> {converted} != {re_pattern}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TEST_CASES)} LIKE -> REGEX TESTS")
    import time

    t = time.monotonic_ns()
    for i in range(57):
        for like_pattern, re_pattern in TEST_CASES:
            print(".", end="")
            test_like_to_regex(like_pattern, re_pattern)
    print()
    print("✅ okay")
    print(time.monotonic_ns() - t)
