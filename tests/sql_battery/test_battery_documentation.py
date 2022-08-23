"""
Test that the queries used in documentation execute without error
"""
import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
import pyarrow

# fmt:off
STATEMENTS = [
    ("SELECT * FROM $planets;"),
    ("SELECT id, name FROM $planets WHERE name = 'Earth';"),
    ("SELECT id, UPPER(name) AS uppercase_name FROM $planets WHERE id = 3;"),
    ("SELECT * FROM $planets WHERE lengthOfDay > 24 AND numberOfMoons < 10;"),
    ("SELECT name, numberOfMoons FROM $planets WHERE numberOfMoons = 0;"),
    ("SELECT name, numberOfMoons FROM $planets WHERE numberOfMoons = 0 ORDER BY name;"),
    ("SELECT DISTINCT planetId FROM $satellites;"),
    ("SELECT DISTINCT planetId FROM $satellites ORDER BY planetId;"),
    ("SELECT * FROM $satellites, $planets WHERE planetId = $planets.id;"),
    ("SELECT * FROM $satellites, $planets WHERE $satellites.planetId = $planets.id;"),
    ("SELECT * FROM $satellites INNER JOIN $planets ON $satellites.planetId = $planets.id;"),
    ("SELECT * FROM $satellites LEFT OUTER JOIN $planets ON $satellites.planetId = $planets.id;"),
    ("SELECT name FROM $planets WHERE id NOT IN (SELECT DISTINCT planetId FROM $satellites);"),
]
# fmt:on


@pytest.mark.parametrize("statement", STATEMENTS)
def test_documentation_examples(statement):

    conn = opteryx.connect()
    cursor = conn.cursor()

    cursor.execute(statement)
    cursor._results = list(cursor._results)
    if cursor._results:
        result = pyarrow.concat_tables(cursor._results, promote=True)
    assert result


if __name__ == "__main__":  # pragma: no cover

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} DOCUMENTATION TESTS")
    for statement in STATEMENTS:
        print(statement)
        test_documentation_examples(statement)

    print("✅ okay")
