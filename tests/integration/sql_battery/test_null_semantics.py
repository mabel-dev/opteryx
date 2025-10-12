"""
The best way to test a SQL engine is to throw queries at it.

This tests the way NULLs are handled in filter conditions.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

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
""", {True, None}),(
"""
-- Query 14: Expected rows: 1 ("true")
SELECT * FROM (VALUES ('true'), ('false'), (NULL)) AS tristatebooleans(bool) WHERE bool = 'true';
""", {'true'}),(
"""
-- Query 15: Expected rows: 1 (NULL)
SELECT * FROM (VALUES ('true'), ('false'), (NULL)) AS tristatebooleans(bool) WHERE bool IS NULL;
""", {None}),(
"""
-- Query 16: Expected rows: 2 ("true", "false")
SELECT * FROM (VALUES ('true'), ('false'), (NULL)) AS tristatebooleans(bool) WHERE bool IS NOT NULL;
""", {'true', 'false'}),(
"""
-- Query 17: Expected rows: 0
SELECT * FROM (VALUES ('true'), ('false'), (NULL)) AS tristatebooleans(bool) WHERE bool = NULL;
""", {}),(
"""
-- Query 18: Expected rows: 2 ("true", "false")
SELECT * FROM (VALUES ('true'), ('false'), (NULL)) AS tristatebooleans(bool) WHERE NOT bool IS NULL;
""", {'true', 'false'}),(
"""
-- Query 19: Expected rows: 1 ("false")
SELECT * FROM (VALUES ('true'), ('false'), (NULL)) AS tristatebooleans(bool) WHERE NOT bool = "true";
""", {'false'}),(
"""
-- Query 20: Expected rows: 1 (1)
SELECT * FROM (VALUES (1), (-1), (NULL)) AS tristatebooleans(bool) WHERE bool = 1;
""", {1}),(
"""
-- Query 21: Expected rows: 1 (NULL)
SELECT * FROM (VALUES (1), (-1), (NULL)) AS tristatebooleans(bool) WHERE bool IS NULL;
""", {None}),(
"""
-- Query 22: Expected rows: 2 (1, -1)
SELECT * FROM (VALUES (1), (-1), (NULL)) AS tristatebooleans(bool) WHERE bool IS NOT NULL;
""", {1, -1}),(
"""
-- Query 23: Expected rows: 0
SELECT * FROM (VALUES (1), (-1), (NULL)) AS tristatebooleans(bool) WHERE bool = NULL;
""", {}),(
"""
-- Query 24: Expected rows: 3 (1, -1, NULL)
SELECT * FROM (VALUES (1), (-1), (NULL)) AS tristatebooleans(bool) WHERE NOT bool IS NULL;
""", {1, -1}),(
"""
-- Query 25: SELECT * FROM tristatebooleans WHERE bool IS NULL;
-- Expected rows: 1 (NULL)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool IS NULL;
""", {None}),(
"""
-- Query 26: SELECT * FROM tristatebooleans WHERE bool IS NOT TRUE;
-- Expected rows: 2 (False, NULL)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool IS NOT TRUE;
""", {False, None}),(
"""
-- Query 27: SELECT * FROM tristatebooleans WHERE bool IS NOT FALSE;
-- Expected rows: 2 (True, NULL)
SELECT * FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool) WHERE bool IS NOT FALSE;
""", {True, None}),(
"""
-- Query 28: NULL OR TRUE should return TRUE (1 row)
-- This tests the three-valued logic: null OR true = true
SELECT 1 FROM $no_table WHERE NULL OR TRUE;
""", {1}),(
"""
-- Query 29: TRUE OR NULL should return TRUE (1 row)
-- This tests commutativity: true OR null = true
SELECT 1 FROM $no_table WHERE TRUE OR NULL;
""", {1}),(
"""
-- Query 30: NULL AND FALSE should return FALSE (0 rows)
-- This tests the three-valued logic: null AND false = false
SELECT 1 FROM $no_table WHERE NULL AND FALSE;
""", {}),(
"""
-- Query 31: FALSE AND NULL should return FALSE (0 rows)
-- This tests commutativity: false AND null = false
SELECT 1 FROM $no_table WHERE FALSE AND NULL;
""", {}),(
"""
-- Query 32: NULL AND TRUE should return NULL (0 rows, null coerces to false in WHERE)
-- This tests the three-valued logic: null AND true = null
SELECT 1 FROM $no_table WHERE NULL AND TRUE;
""", {}),(
"""
-- Query 33: TRUE AND NULL should return NULL (0 rows, null coerces to false in WHERE)
-- This tests commutativity: true AND null = null
SELECT 1 FROM $no_table WHERE TRUE AND NULL;
""", {}),(
"""
-- Query 34: NULL OR FALSE should return NULL (0 rows, null coerces to false in WHERE)
-- This tests the three-valued logic: null OR false = null
SELECT 1 FROM $no_table WHERE NULL OR FALSE;
""", {}),(
"""
-- Query 35: FALSE OR NULL should return NULL (0 rows, null coerces to false in WHERE)
-- This tests commutativity: false OR null = null
SELECT 1 FROM $no_table WHERE FALSE OR NULL;
""", {}),(
"""
-- Query 36: NULL XOR TRUE should return NULL (0 rows, null coerces to false in WHERE)
-- This tests the three-valued logic: null XOR true = null
SELECT 1 FROM $no_table WHERE NULL XOR TRUE;
""", {}),(
"""
-- Query 37: NULL XOR FALSE should return NULL (0 rows, null coerces to false in WHERE)
-- This tests the three-valued logic: null XOR false = null
SELECT 1 FROM $no_table WHERE NULL XOR FALSE;
""", {}),(
"""
-- Query 38: NOT NULL should return NULL (0 rows, null coerces to false in WHERE)
-- This tests the three-valued logic: NOT null = null
SELECT 1 FROM $no_table WHERE NOT NULL;
""", {}),(
"""
-- Query 39: NULL in aggregate functions - COUNT should ignore NULLs
SELECT COUNT(bool) FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool);
""", {2}),(
"""
-- Query 40: NULL in aggregate functions - COUNT(*) should count NULLs
SELECT COUNT(*) FROM (VALUES (True), (False), (NULL)) AS tristatebooleans(bool);
""", {3}),(
# """
# -- Query 41: NULL in string concatenation
#SELECT 1 FROM $no_table WHERE ('hello' || NULL) IS NULL;
#""", {1}),(
#"""
#-- Query 42: NULL in arithmetic operations - addition
#SELECT 1 FROM $no_table WHERE (5 + NULL) IS NULL;
#""", {1}),(
#"""
#-- Query 43: NULL in arithmetic operations - multiplication
#SELECT 1 FROM $no_table WHERE (5 * NULL) IS NULL;
#""", {1}),(
"""
-- Query 44: NULL in comparison - NULL = NULL is NULL (not TRUE)
SELECT 1 FROM $no_table WHERE NULL = NULL;
""", {}),(
"""
-- Query 45: NULL in comparison - NULL <> NULL is NULL (not TRUE)
SELECT 1 FROM $no_table WHERE NULL <> NULL;
""", {}),(
#"""
#-- Query 46: NULL in comparison - NULL IS DISTINCT FROM NULL is FALSE
#SELECT 1 FROM $no_table WHERE NOT (NULL IS DISTINCT FROM NULL);
#""", {1}),(
"""
-- Query 47: NULL in CASE expression - NULL in condition
SELECT 1 FROM $no_table WHERE CASE WHEN NULL THEN FALSE ELSE TRUE END;
""", {1}),(
"""
-- Query 48: NULL NULLS FIRST in ORDER BY (just checking it doesn't error)
SELECT bool FROM (VALUES (1), (NULL), (2)) AS test(bool) ORDER BY bool NULLS FIRST;
""", {1, None, 2}),(
"""
-- Query 49: NULL NULLS LAST in ORDER BY (just checking it doesn't error)
SELECT bool FROM (VALUES (1), (NULL), (2)) AS test(bool) ORDER BY bool NULLS LAST;
""", {1, None, 2}),(
"""
-- Query 50: NULL with COALESCE - returns first non-NULL
SELECT 1 FROM $no_table WHERE COALESCE(NULL, NULL, 5) = 5;
""", {1}),(
"""
-- Query 51: NULL with COALESCE - all NULLs returns NULL
SELECT 1 FROM $no_table WHERE COALESCE(NULL, NULL, NULL) IS NULL;
""", {1}),(
"""
-- Query 52: NULL in HAVING clause
SELECT bool FROM (VALUES (True), (False), (NULL)) AS test(bool) GROUP BY bool HAVING bool IS NULL;
""", {None}),(
"""
-- Query 53: NULL in MIN aggregate
SELECT MIN(val) FROM (VALUES (1), (2), (NULL)) AS test(val);
""", {1}),(
"""
-- Query 54: NULL in MAX aggregate
SELECT MAX(val) FROM (VALUES (1), (2), (NULL)) AS test(val);
""", {2}),(
"""
-- Query 55: NULL in SUM aggregate
SELECT SUM(val) FROM (VALUES (1), (2), (NULL)) AS test(val);
""", {3}),(
"""
-- Query 56: NULL in AVG aggregate
SELECT AVG(val) FROM (VALUES (1.0), (3.0), (NULL)) AS test(val);
""", {2.0}),(
"""
-- Query 57: NULL with IN operator - NULL IN (values) is always NULL
SELECT 1 FROM $no_table WHERE NULL IN (1, 2, 3);
""", {}),(
#"""
#-- Query 58: NULL with NOT IN operator - value NOT IN (values with NULL) can be NULL
#SELECT 1 FROM $no_table WHERE 5 NOT IN (1, 2, NULL);
#""", {}),(
"""
-- Query 59: Non-NULL with NOT IN operator - value NOT IN (values without the value) is TRUE
SELECT 1 FROM $no_table WHERE 5 NOT IN (1, 2, 3);
""", {1}),(
"""
-- Query 60: NULL in DISTINCT - should be treated as a unique value
SELECT COUNT(DISTINCT val) FROM (VALUES (1), (1), (NULL), (NULL)) AS test(val);
""", {2}),
]
# fmt:on


def process_set(set_with_nan):
    has_nan = any(item != item for item in set_with_nan)  # Check for NaN using NaN's property
    set_without_nan = {
        item for item in set_with_nan if item == item
    }  # Create a new set without NaNs
    return has_nan, set_without_nan


def compare_sets(set1, set2):
    if not set1 and not set2:
        return True

    s1_nan, s1_no_nan = process_set(set1)
    s2_nan, s2_no_nan = process_set(set2)

    return s1_nan == s2_nan and s1_no_nan == s2_no_nan


@pytest.mark.parametrize("statement, expected_result", STATEMENTS)
def test_null_semantics(statement, expected_result):
    """
    Test an battery of statements
    """

    cursor = opteryx.query(statement)
    result = {v[0] for v in cursor.fetchall()}
    assert compare_sets(
        result, expected_result
    ), f"Query returned {result} but {expected_result} was expected.\n{statement}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(STATEMENTS)} NULL SEMANTICS TESTS")
    for statement, expected_result in STATEMENTS:
        print(statement)
        test_null_semantics(statement, expected_result)

    print("✅ okay")
