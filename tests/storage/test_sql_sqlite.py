"""
Test we can read from SQLite.

SQLite is also used to test the SQLConnector harder than the other
SQL sources. We use SQLite for this because the file is local and therefore
we're not going to cause contention with remote services. 

Note: DuckDB also has additional tests to the standard battery but because
DuckDB doesn't have a stable file format, it only covers a subset of
the required use cases (to save time, loading it with a lot of different
tables is time consuming)
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
#    ("SELECT * FROM sqlite.planets p JOIN sqlite.satellites s ON p.id = s.planetId AND p.gravity > 1;", 6, 28, None),
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
    ("SELECT name FROM sqlite.planets WHERE name NOT ILIKE '%o';", 8, 1, None)  # All except Pluto

]
# fmt: on


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
    except AssertionError as err:
        raise Exception(err) from err
    except Exception as err:
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

    print("#- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for statement, err in failures:
            print(err)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
