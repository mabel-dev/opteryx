import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.utils import fuzzy_search

# fmt:off
TESTS = [
        ("apple", ["snapple", "crackle", "pop"], "snapple"),
        ("app_le", ["apple", "crackle", "pop"], "apple"),
        ("apple", ["AppLe", "crackle", "pop"], "AppLe"),
        ("apple", ["car", "plane", "bus"], None),
    ]
# fmt:on


@pytest.mark.parametrize("string, candidates, expected", TESTS)
def test_date_parser(string, candidates, expected):
    assert fuzzy_search(string, candidates) == expected


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} FUZZY TESTS")
    for s, c, e in TESTS:
        test_date_parser(s, c, e)
    print("✅ okay")
