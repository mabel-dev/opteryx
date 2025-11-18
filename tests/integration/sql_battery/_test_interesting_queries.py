"""
The best way to test a SQL engine is to throw queries at it.

This battery of tests is inspired by the article 
"SQL Queries That Will Surprise You"
by Markus Winand.
https://medium.com/codex/sql-queries-that-will-surprise-you-a5e21cc0ee85
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest
import opteryx

# fmt:off
STATEMENTS = [
    # we don't support subqueries ("/* Find the Second Highest Value */ SELECT MAX(salary) AS second_highest_salary FROM employees WHERE salary < (SELECT MAX(salary) FROM employees);", [(0)]),
    # we don't support functions in joins ("/* Detect Gaps */ SELECT t1.id + 1 AS missing_id FROM $planets t1 LEFT JOIN $planets t2 ON t1.id + 1 = t2.id WHERE t2.id IS NULL;", [(0)]),
    ("/* Find the Nth Highest Value */ SELECT DISTINCT name FROM $planets ORDER BY id DESC LIMIT 1 OFFSET 2;", [('Uranus',)]),
    ("/* Calculating a Running Total */ SELECT employee_id, salary, SUM(salary) OVER (ORDER BY employee_id) AS running_total FROM employees;", [(0)]),
    ("/* Pivoting Data */ SELECT employee_id, MAX(CASE WHEN month = 'January' THEN sales END) AS january_sales, MAX(CASE WHEN month = 'February' THEN sales END) AS february_sales FROM sales GROUP BY employee_id;", [(0)]),
    ("/* Finding Duplicates */ SELECT column_name, COUNT(*) FROM table_name GROUP BY column_name HAVING COUNT(*) > 1;", [(0)]),
    ("/* Overlapping Date Ranges */ SELECT a.*, b.* FROM reservations a JOIN reservations b ON a.start_date < b.end_date AND a.end_date > b.start_date AND a.id <> b.id;", [(0)]),
    ("/* Hierarchical Queries */ WITH RECURSIVE employee_tree AS (SELECT employee_id, manager_id, 1 AS level FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.employee_id, e.manager_id, et.level + 1 FROM employees e JOIN employee_tree et ON e.manager_id = et.employee_id) SELECT * FROM employee_tree;", [(0)]),
    ("/* Generating Random Data */ SELECT FLOOR(1 + (RAND() * 100)) FROM numbers_table LIMIT 10;", [(0)]),
    ("/* Finding the Median */ SELECT AVG(salary) AS median FROM (SELECT salary FROM employees ORDER BY salary LIMIT 2 - (SELECT COUNT(*) FROM employees) % 2 OFFSET (SELECT (COUNT(*) - 1) / 2 FROM employees)) subquery;", [(0)]),
]
# fmt:on

def compare_sets(result, expected_result):
    return set(result) == set(expected_result)

@pytest.mark.parametrize("statement, expected_result", STATEMENTS)
def test_null_semantics(statement, expected_result):
    """
    Test an battery of statements
    """

    cursor = opteryx.query(statement)
    result = [tuple(v) for v in cursor.fetchall()]
    assert compare_sets(
        result, expected_result
    ), f"Query returned {result} but {expected_result} was expected.\n{statement}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(STATEMENTS)} NULL SEMANTICS TESTS")
    for statement, expected_result in STATEMENTS:
        print(statement)
        test_null_semantics(statement, expected_result)

    print("✅ okay")
