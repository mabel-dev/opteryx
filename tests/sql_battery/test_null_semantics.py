"""
The best way to test a SQL engine is to throw queries at it.

This tests the way NULLs are handled in filter conditions.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
import opteryx

# fmt:off
STATEMENTS = [
    (
"""
-- Query 1: SELECT * FROM tristatebooleans WHERE bool;
-- Expected rows: 1 (True)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool;
""", {True}),(
"""
-- Query 2: SELECT * FROM tristatebooleans WHERE NOT bool;
-- Expected rows: 1 (False)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE NOT bool;
""", {False}),(
"""
-- Query 3: SELECT * FROM tristatebooleans WHERE bool IS True;
-- Expected rows: 1 (True)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool IS TRUE;
""", {True}),(
"""
-- Query 4: SELECT * FROM tristatebooleans WHERE bool IS NOT True;
-- Expected rows: 2 (False, NULL)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool IS NOT TRUE;
""", {False, None}),(
"""
-- Query 5: SELECT * FROM tristatebooleans WHERE NOT bool IS True;
-- Expected rows: 1 (False)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE NOT bool IS TRUE;
""", {False, None}),(
"""
-- Query 6: SELECT * FROM tristatebooleans WHERE bool IS NULL;
-- Expected rows: 1 (NULL)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool IS NULL;
""", {None}),(
"""
-- Query 7: SELECT * FROM tristatebooleans WHERE bool IS NOT NULL;
-- Expected rows: 2 (True, False)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool IS NOT NULL;
""", {True, False}),(
"""
-- Query 8: SELECT * FROM tristatebooleans WHERE NOT (bool IS NULL);
-- Expected rows: 2 (True, False)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE NOT (bool IS NULL);
""", {True, False}),(
"""
-- Query 9: SELECT * FROM tristatebooleans WHERE bool = bool;
-- Expected rows: 2 (True, False)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool = bool;
""", {True, False}),(
"""
-- Query 10: SELECT * FROM tristatebooleans WHERE bool <> bool;
-- Expected rows: 0
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool <> bool;
""", {}),(
"""
-- Query 11: SELECT * FROM tristatebooleans WHERE bool OR NOT bool;
-- Expected rows: 2 (True, False)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool OR NOT bool;
""",{True,False}),(
"""
-- Query 12: SELECT * FROM tristatebooleans WHERE bool AND NOT bool;
-- Expected rows: 0
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool AND NOT bool;
""", {}),(
"""
-- Query 13: SELECT * FROM tristatebooleans WHERE bool IS NOT FALSE;
-- Expected rows: 2 (True, NULL)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool IS NOT FALSE;
""", {True, None})
]
# fmt:on


def compare_sets(set1, set2):
    if not set1 and not set2:
        return True
    return set1 == set2


@pytest.mark.parametrize("statement, rows, columns, skip", STATEMENTS)
def test_null_semantics(statement, expected_result):
    """
    Test an battery of statements
    """

    cursor = opteryx.query(statement)
    result = {v[0] for v in cursor.fetchall()}
    assert compare_sets(
        result, expected_result
    ), f"Query returned {result} rows but {expected_result} was expected.\n{statement}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(STATEMENTS)} NULL SEMANTICS TESTS")
    for statement, expected_result in STATEMENTS:
        print(statement)
        test_null_semantics(statement, expected_result)

    print("✅ okay")
