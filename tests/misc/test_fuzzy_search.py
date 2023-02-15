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
        ("dog", ["cat", "doge", "pig", "duck"], "doge"),
        ("crackle", ["cracker", "crack", "snack"], "cracker"),
        ("carrot", ["cabbage", "celery", "cucumber", "carrot", "cantaloupe"], "carrot"),
        ("", ["hello", "world"], None),
        ("apple", ["apple", "crackle", "pop"], "apple"),
        ("appple", ["apple", "crackle", "pop"], "apple"),
        ("aaple", ["apple", "crackle", "pop"], "apple"),
        ("appl", ["apple", "crackle", "pop"], "apple"),
        ("aple", ["apple", "crackle", "pop"], "apple"),
        ("aple", ["aple", "crackle", "pop"], "aple"),
        ("ppl", ["apple", "crackle", "pop"], "apple"),
        ("a", ["apple", "crackle", "pop"], None),
        ("", ["apple", "crackle", "pop"], None),
        ("", ["", "crackle", "pop"], ""),
        ("", [], None),
        ("apple", ["appl", "aple", "aplee", "aplle"], "appl"),  # first best match
        ("a_b_c_d", ["abcd", "a_b_cd", "a_b_c_d_e"], "a_b_cd"),
        ("a_b_c_d_e", ["abcd", "a_b_cd", "a_b_c_d_e"], "a_b_c_d_e"),
        ("a-b+c_d", ["abcd", "a_b+cd", "a-b+c_d-e"], "a_b+cd"),
        ("apple", ["banana", "orange", "pear"], None),
        ("apple", [], None),
        ("apple", ["appl", "aple", "aplee", "aplle", "apple"], "apple"),
        ("123", ["123", "321", "456", "12"], "123"),
        ("123", ["124", "456", "12"], "124"), # first best match
        ("123", ["124", "321", "456", "12"], "124"),  # first best match
        ("1234", ["124", "321", "456", "12"], "124"),
        ("apple", ["appl", "aple", "aplee", "aplle", "aplle"], "appl"), # first best match
        ("apple", ["aple", "appl", "aplee", "aplle", "aplee"], "aple"), # first best match
        ("apple", ["aplee", "applle"], "applle"),
    ]
# fmt:on


@pytest.mark.parametrize("string, candidates, expected", TESTS)
def test_date_parser(string, candidates, expected):
    """
    We're running a string through a set of candidate matches and returning the item
    which is the best match (expected)
    """
    assert (
        fuzzy_search(string, candidates) == expected
    ), f"{string}, {candidates}, {expected} != {fuzzy_search(string, candidates)}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} FUZZY TESTS")
    for s, c, e in TESTS:
        test_date_parser(s, c, e)
    print("✅ okay")
