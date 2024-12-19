"""
This module tests the ability to read from SQLite using the SQLConnector.

SQLite is used to rigorously test the SQLConnector due to its local file nature,
which avoids contention with remote services. This allows for more intensive
testing without the overhead of network latency or remote service limitations.

Note: DuckDB also includes additional tests beyond the standard battery. However,
due to DuckDB's unstable file format, it only covers a subset of the required use
cases to save time, as loading it with numerous different tables can be time-consuming.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector
from opteryx.utils.formatter import format_sql

# fmt: off
STATEMENTS = [
    ("SELECT * FROM sqlite.planets", 9, 20, None),
    ("SELECT * FROM sqlite.satellites", 177, 8, None),
    ("SELECT * FROM sqlite_tweets.tweets", 100000, 9, None),
    ("SELECT COUNT(*) FROM sqlite.planets;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite.satellites;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite_tweets.tweets", 1, 1, None),
    ("SELECT COUNT(*) FROM (SELECT * FROM sqlite.planets) AS p", 1, 1, None),
    ("SELECT COUNT(*) FROM (SELECT COUNT(*) FROM sqlite.planets) AS p", 1, 1, None),
    ("SELECT COUNT(*) FROM (SELECT * FROM sqlite.planets WHERE id > 4) AS p", 1, 1, None),
    ("SELECT COUNT(*) FROM (SELECT * FROM sqlite.planets) AS p WHERE id > 4", 1, 1, None),
    ("SELECT name FROM sqlite.planets;", 9, 1, None),
    ("SELECT name FROM sqlite.satellites;", 177, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets;", 100000, 1, None),
    ("SELECT * FROM sqlite.planets INNER JOIN $satellites ON sqlite.planets.id = $satellites.planetId;", 177, 28, None),
    ("SELECT * FROM sqlite.planets, $satellites WHERE sqlite.planets.id = $satellites.planetId;", 177, 28, None),
    ("SELECT * FROM sqlite.planets CROSS JOIN $satellites WHERE sqlite.planets.id = $satellites.planetId;", 177, 28, None),
    ("SELECT * FROM sqlite.planets INNER JOIN sqlite.satellites ON sqlite.planets.id = sqlite.satellites.planetId;", 177, 28, None),
    ("SELECT name FROM sqlite.planets WHERE name LIKE 'Earth';", 1, 1, None),
    ("SELECT * FROM sqlite.planets WHERE id > gravity", 2, 20, None),
    ("SELECT * FROM sqlite.planets WHERE surfacePressure IS NULL", 4, 20, None),
    ("SELECT * FROM sqlite.planets WHERE surfacePressure IS NOT NULL", 5, 20, None),
    ("SELECT user_name, user_verified FROM sqlite_tweets.tweets WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2, None),
    ("SELECT * FROM sqlite.planets, sqlite.satellites WHERE sqlite.planets.id = 5 AND sqlite.satellites.planetId = 5;", 67, 28, None),
    ("SELECT * FROM sqlite.planets, sqlite.satellites WHERE sqlite.planets.id - sqlite.satellites.planetId = 0;", 177, 28, None),
    ("SELECT * FROM sqlite.planets, sqlite.satellites WHERE sqlite.planets.id - sqlite.satellites.planetId != 0;", 1416, 28, None),
    ("SELECT * FROM sqlite.planets WHERE sqlite.planets.id - sqlite.planets.numberOfMoons < 0;", 4, 20, None),
    ("SELECT avg(num_moons) FROM (SELECT numberOfMoons as num_moons FROM sqlite.planets) AS subquery;", 1, 1, None),
    ("SELECT p.name, s.name FROM sqlite.planets p LEFT OUTER JOIN sqlite.satellites s ON p.id = s.planetId;", 179, 2, None),
    ("SELECT A.name, B.name FROM sqlite.planets A, sqlite.planets B WHERE A.gravity = B.gravity AND A.id != B.id;", 2, 2, None),
    ("SELECT * FROM sqlite.planets p JOIN sqlite.satellites s ON p.id = s.planetId WHERE p.gravity > 1;", 172, 28, None),
    ("SELECT planetId, COUNT(*) AS num_satellites FROM sqlite.satellites GROUP BY planetId HAVING COUNT(*) > 1;", 6, 2, None),
    ("SELECT * FROM sqlite.planets ORDER BY name;", 9, 20, None),
    ("SELECT DISTINCT name FROM sqlite.planets;", 9, 1, None),
    ("SELECT MAX(gravity) FROM sqlite.planets;", 1, 1, None),
    ("SELECT MIN(gravity) FROM sqlite.planets;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite.planets WHERE surfacePressure > 0;", 1, 1, None),
    ("SELECT AVG(mass) FROM sqlite.planets", 1, 1, None),
    ("SELECT MIN(distanceFromSun) FROM sqlite.planets", 1, 1, None),
    ("SELECT MAX(lengthOfDay) FROM sqlite.planets", 1, 1, None),
    ("SELECT UPPER(name), ROUND(mass, 2) FROM sqlite.planets", 9, 2, None),
    ("SELECT surfacePressure, COUNT(*) FROM sqlite.planets GROUP BY surfacePressure HAVING COUNT(*) > 1", 1, 2, None),
    ("SELECT * FROM sqlite.planets WHERE mass > 0.1 AND distanceFromSun < 500", 4, 20, None),
    ("SELECT name, SIGNUM(mass) AS sin_mass FROM sqlite.planets", 9, 2, None),
    ("SELECT name, CASE WHEN mass > 1 THEN 'heavy' ELSE 'light' END FROM sqlite.planets", 9, 2, None),
    ("SELECT name FROM sqlite.planets WHERE surfacePressure IS NULL", 4, 1, None),
    ("SELECT name FROM sqlite.planets WHERE surfacePressure IS NOT NULL", 5, 1, None),
    ("SELECT name FROM sqlite.planets WHERE numberOfMoons IS NOT TRUE", 2, 1, None),
    ("SELECT name FROM sqlite.planets WHERE numberOfMoons IS TRUE", 7, 1, None),
    ("SELECT name FROM sqlite.planets WHERE name LIKE 'M%';", 2, 1, None),  # Mars, Mercury
    ("SELECT name FROM sqlite.planets WHERE name NOT LIKE 'M%';", 7, 1, None),  # All except Mars and Mercury
    ("SELECT name FROM sqlite.planets WHERE name LIKE '%e%';", 5, 1, None),  # Earth, Jupiter, Neptune, Mercury, Venus
    ("SELECT name FROM sqlite.planets WHERE name NOT LIKE '%e%';", 4, 1, None),  # Mars, Saturn, Uranus, Pluto
    ("SELECT name FROM sqlite.planets WHERE name ILIKE 'p%';", 1, 1, None),  # Pluto
    ("SELECT name FROM sqlite.planets WHERE name NOT ILIKE 'p%';", 8, 1, None),  # All except Pluto
    ("SELECT name FROM sqlite.planets WHERE name ILIKE '%U%';", 7, 1, None),
    ("SELECT name FROM sqlite.planets WHERE name NOT ILIKE '%U%';", 2, 1, None),
    ("SELECT name FROM sqlite.planets WHERE name LIKE '__r%';", 3, 1, None),  # Earth, Uranus
    ("SELECT name FROM sqlite.planets WHERE name NOT LIKE '__r%';", 6, 1, None), 
    ("SELECT name FROM sqlite.planets WHERE name LIKE '%t';", 0, 1, None), 
    ("SELECT name FROM sqlite.planets WHERE name NOT LIKE '%t';", 9, 1, None), 
    ("SELECT name FROM sqlite.planets WHERE name ILIKE '_a%';", 3, 1, None),  # Mars, Saturn, Uranus
    ("SELECT name FROM sqlite.planets WHERE name NOT ILIKE '_a%';", 6, 1, None),  # All except Mars, Saturn, Uranus
    ("SELECT name FROM sqlite.planets WHERE name LIKE '____';", 1, 1, None), 
    ("SELECT name FROM sqlite.planets WHERE name NOT LIKE '____';", 8, 1, None),  # All except Mars, Earth
    ("SELECT name FROM sqlite.planets WHERE name ILIKE '%o';", 1, 1, None),  # Pluto
    ("SELECT name FROM sqlite.planets WHERE name RLIKE '^M';", 2, 1, None),  # Mars, Mercury
    ("SELECT name FROM sqlite.planets WHERE name RLIKE 'e';", 4, 1, None),  # Earth, Jupiter, Neptune, Mercury, Venus
    ("SELECT name FROM sqlite.planets WHERE name RLIKE '^.a';", 3, 1, None),  # Mars, Saturn, Uranus
    ("SELECT name FROM sqlite.planets WHERE name RLIKE '^.{4}$';", 1, 1, None),  # Mars
    ("SELECT name FROM sqlite.planets WHERE name RLIKE 't$';", 0, 1, None), 
    ("SELECT name FROM sqlite.planets WHERE name RLIKE 'o$';", 1, 1, None),  # Pluto
    ("SELECT name FROM sqlite.planets WHERE name NOT RLIKE 'o$';", 8, 1, None),  # All except Pluto
    ("SELECT COUNT(DISTINCT name) FROM sqlite.planets;", 1, 1, None),
    ("SELECT name FROM sqlite.planets WHERE id NOT IN (1, 2, 3);", 6, 1, None),
    ("SELECT COUNT(*) FROM sqlite.satellites WHERE planetId = 1;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite.satellites WHERE planetId = 2;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite.satellites WHERE planetId = 3;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite.satellites WHERE planetId = 4;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite.satellites WHERE planetId = 5;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite.satellites WHERE planetId = 6;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite.satellites WHERE planetId = 7;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite.satellites WHERE planetId = 8;", 1, 1, None),
    ("SELECT COUNT(*) FROM sqlite.satellites WHERE planetId = 9;", 1, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE user_verified is true;", 711, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE user_verified is false;", 99289, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE user_verified = true;", 711, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE user_verified = false;", 99289, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE text LIKE '%happy%';", 1174, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE text LIKE '%sad%';", 697, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE text LIKE '%excited%';", 280, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE text LIKE '%angry%';", 147, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE text LIKE '%bored%';", 102, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE text LIKE '%tired%';", 359, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE text LIKE '%hungry%';", 74, 1, None),
    ("SELECT user_name FROM sqlite_tweets.tweets WHERE text LIKE '%thirsty%';", 10, 1, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 2 AND 5;", 4, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 2 AND 5;", 5, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name IN ('Earth', 'Mars');", 2, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name NOT IN ('Earth', 'Mars');", 7, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name LIKE 'M%';", 2, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name NOT LIKE 'M%';", 7, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name LIKE '%e%';", 5, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name NOT LIKE '%e%';", 4, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name LIKE '_a%';", 3, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name NOT LIKE '_a%';", 6, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name LIKE '____';", 1, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name NOT LIKE '____';", 8, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name ILIKE 'p%';", 1, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name NOT ILIKE 'p%';", 8, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name ILIKE '%U%';", 7, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name NOT ILIKE '%U%';", 2, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name RLIKE '^M';", 2, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name RLIKE 'e';", 4, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name RLIKE '^.a';", 3, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name RLIKE '^.{4}$';", 1, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name RLIKE 't$';", 0, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name RLIKE 'o$';", 1, 20, None),
    ("SELECT * FROM sqlite.planets WHERE name NOT RLIKE 'o$';", 8, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id > 3 AND id < 7;", 3, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id <= 3 OR id >= 7;", 6, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id = 1 OR id = 9;", 2, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id != 1 AND id != 9;", 7, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id > 1 AND id < 9;", 7, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id <= 1 OR id >= 9;", 2, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id IN (1, 3, 5, 7, 9);", 5, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT IN (1, 3, 5, 7, 9);", 4, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 1 AND 3;", 3, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 1 AND 3;", 6, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 7 AND 9;", 3, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 7 AND 9;", 6, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 4 AND 6;", 3, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 4 AND 6;", 6, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 2 AND 8;", 7, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 2 AND 8;", 2, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 1 AND 9;", 9, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 1 AND 9;", 0, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 3 AND 7;", 5, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 3 AND 7;", 4, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 5 AND 9;", 5, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 5 AND 9;", 4, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 1 AND 5;", 5, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 1 AND 5;", 4, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 2 AND 6;", 5, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 2 AND 6;", 4, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 4 AND 8;", 5, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id NOT BETWEEN 4 AND 8;", 4, 20, None),
    ("SELECT * FROM sqlite.planets WHERE id BETWEEN 3 AND 9;", 7, 20, None),
    ("SELECT user_name, name FROM sqlite_tweets.tweets JOIN sqlite.planets ON sqlite_tweets.tweets.followers = sqlite.planets.id;", 3962, 2, None),
]

@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery(statement, rows, columns, exception):
    """
    Test an battery of statements
    """

    opteryx.register_store(
        "sqlite",
        SqlConnector,
        remove_prefix=True,
        connection="sqlite:///testdata/sqlite/database.db",
    )

    opteryx.register_store(
        "sqlite_tweets",
        SqlConnector,
        remove_prefix=True,
        connection="sqlite:///testdata/sqlite/100000-tweets.db",
    )

    try:
        # query to arrow is the fastest way to query
        result = opteryx.query_to_arrow(statement)
        actual_rows, actual_columns = result.shape
        assert (
            rows == actual_rows
        ), f"\n\033[38;5;203mQuery returned {actual_rows} rows but {rows} were expected.\033[0m\n{statement}"
        assert (
            columns == actual_columns
        ), f"\n\033[38;5;203mQuery returned {actual_columns} cols but {columns} were expected.\033[0m\n{statement}"
        assert (
            exception is None
        ), f"Exception {exception} not raised but expected\n{format_sql(statement)}"
    except AssertionError as err:  # pragma: no cover
        raise Exception(err) from err
    except Exception as err:  # pragma: no cover
        print(err)
        if type(err) != exception:
            raise Exception(
                f"{format_sql(statement)}\nQuery failed with error {type(err)} but error {exception} was expected"
            ) from err


if __name__ == "__main__":  # pragma: no cover
    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil
    import time

    from tests.tools import trunc_printable

    start_suite = time.monotonic_ns()

    width = shutil.get_terminal_size((80, 20))[0] - 15

    passed = 0
    failed = 0

    nl = "\n"

    failures = []

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SQLITE TESTS")
    for index, (statement, rows, cols, err) in enumerate(STATEMENTS):
        printable = statement
        if hasattr(printable, "decode"):
            printable = printable.decode()
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(printable), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_sql_battery(statement, rows, cols, err)
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
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ *\033[0m")
            print(">", err)
            failed += 1
            failures.append((statement, err))

    print("- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for statement, err in failures:
            print(err)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
