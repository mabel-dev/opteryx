"""
Test the permissions model is correctly allowing and blocking queries being executed

"""

import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

test_cases = [
    ("SELECT * FROM $planets", {"testdata.planets": [("id", "Eq", 4)]}, (9, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Eq", 4)]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "NotEq", 4)]}, (8, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Gt", 4)]}, (5, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Lt", 4)]}, (3, 20)),
    ("SELECT * FROM $planets", {"$planets": [("name", "Eq", "Earth")]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [("name", "Like", "%a%")]}, (4, 20)),
    ("SELECT * FROM $satellites", {"$planets": [("id", "Eq", 4)]}, (177, 8)),
    ("SELECT * FROM $satellites", {"$planets": [("id", "NotEq", 4)]}, (177, 8)),
    ("SELECT * FROM $satellites", {"$planets": [("id", "Gt", 4)]}, (177, 8)),
    ("SELECT * FROM $satellites", {"$planets": [("id", "Lt", 4)]}, (177, 8)),
    ("SELECT * FROM $satellites", {"$satellites": [("id", "Eq", 4)]}, (1, 8)),
    ("SELECT * FROM $satellites", {"$satellites": [("id", "NotEq", 4)]}, (176, 8)),
    ("SELECT * FROM $satellites", {"$satellites": [("id", "Gt", 4)]}, (173, 8)),
    ("SELECT * FROM $satellites", {"$satellites": [("id", "Lt", 4)]}, (3, 8)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Eq", 4)], "$satellites": [("id", "Gt", 4)]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "NotEq", 4)], "$satellites": [("id", "Gt", 4)]}, (8, 20)),

    ("SELECT * FROM $planets AS planets", {"planets": [("id", "Eq", 4)]}, (9, 20)),
    ("SELECT * FROM $planets AS p", {"$planets": [("id", "Eq", 4)]}, (1, 20)),

    ("SELECT * FROM $planets", {"$planets": [[("id", "Eq", 4), ("name", "Like", "M%")], [("id", "Gt", 7)]]}, (3, 20)),
    ("SELECT * FROM $planets", {"$planets": [[("name", "Eq", "Earth"), ("id", "Eq", 4)], [("id", "Gt", 7)]]}, (2, 20)),
    ("SELECT * FROM $planets", {"$planets": [[("id", "Eq", 4)], [("name", "Like", "M%")]]}, (2, 20)),

    ("SELECT * FROM $planets AS p INNER JOIN $satellites AS s ON p.id = s.planetId", {"$planets": [("id", "Eq", 3)]}, (1, 28)),
    ("SELECT * FROM $planets p LEFT JOIN $satellites s ON p.id = s.planetId", {"$planets": [("id", "Gt", 3)], "$satellites": [("id", "Lt", 10)]}, (12, 28)),
    ("SELECT * FROM $planets p LEFT JOIN $satellites s ON p.id = s.planetId",  {}, (179, 28)),
    ("SELECT * FROM $planets p LEFT JOIN $satellites s ON p.id = s.planetId",  {"$satellites": [("id", "Lt", 4)]}, (10, 28)),

    ("SELECT * FROM $planets p1 JOIN $planets p2 ON p1.id = p2.id", {"$planets": [("id", "Gt", 3)], "p2": [("name", "NotEq", "X")]}, (6, 40)),

    ("SELECT * FROM $planets WHERE id = 4", {"$planets": [("id", "Eq", 4)]}, (1, 20)),
    ("SELECT * FROM $planets WHERE name = 'Mars'", {"$planets": [("name", "Eq", "Mars")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE name LIKE 'M%'", {"$planets": [("name", "Like", "M%")]}, (2, 20)),
    ("SELECT * FROM $planets WHERE id > 3 AND name LIKE 'M%'", {"$planets": [("id", "Gt", 3), ("name", "Like", "M%")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE id < 4 OR name LIKE 'M%'", {"$planets": [("id", "Lt", 4), ("name", "Like", "M%")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE id = 4 AND name = 'Mars'", {"$planets": [("id", "Eq", 4), ("name", "Eq", "Mars")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE id = 4 OR name = 'Mars'", {"$planets": [("id", "Eq", 4), ("name", "Eq", "Mars")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE id = 4 AND name LIKE 'M%'", {"$planets": [("id", "Eq", 4), ("name", "Like", "M%")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE name LIKE 'M%'", {"$planets": [("id", "Eq", 4), ("name", "Like", "M%")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE id = 4", {"$planets": [("id", "Eq", 4), ("name", "NotLike", "M%")]}, (0, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Eq", 4), ("name", "NotLike", "M%")]}, (0, 20)),
]


@pytest.mark.parametrize("sql, filters, shape", test_cases)
def test_visibility_filters(sql, filters, shape):
    """test we can stop users performing some query types"""
    
    cur = opteryx.query(sql, visibility_filters=filters)
    cur.materialize()
    assert cur.shape ==  shape, cur.shape


if __name__ == "__main__":  # pragma: no cover

    import shutil
    import time

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(test_cases)} TESTS")
    for index, (sql, filters, shape) in enumerate(test_cases):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" .",
            end="",
            flush=True,
        )

        try:
            start = time.monotonic_ns()
            test_visibility_filters(sql, filters, shape)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(" \033[0;31m*\033[0m")
            else:
                print()
        except Exception as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(5)}ms ❌ *\033[0m")
            print(">", sql, filters, shape, err)
            failed += 1

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
