"""
The best way to test a SQL Engine is to throw queries at it.

This is testing the SQL connector.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from sqlalchemy import create_engine

import opteryx
from opteryx.connectors import SqlConnector

expected_rows = -1
# fmt:off
STATEMENTS = [
        # Are the datasets the shape we expect?
        ("SELECT * FROM sqlite.planets", 9, 20, None),

        # randomly generated tests
        ("SELECT name FROM sqlite.planets WHERE diameter <= 5000", 2, 1, None),
        ("SELECT COUNT(*) FROM sqlite.planets WHERE numberOfMoons > 2", 1, 1, None),
        ("SELECT AVG(diameter) FROM sqlite.planets", 1, 1, None),
        ("SELECT MAX(mass) FROM sqlite.planets", 1, 1, None),
        ("SELECT MIN(orbitalPeriod) FROM sqlite.planets", 1, 1, None),
        ("SELECT * FROM sqlite.planets WHERE surfacePressure > 100", 0, 20, None),
        ("SELECT name FROM sqlite.planets WHERE name LIKE '_a%'", 3, 1, None),
        ("SELECT name, diameter FROM sqlite.planets WHERE diameter BETWEEN 2000 AND 5000", 2, 2, None),
        ("SELECT DISTINCT orbitalEccentricity FROM sqlite.planets", 9, 1, None),
        ("SELECT * FROM sqlite.planets WHERE rotationPeriod > 10 ORDER BY rotationPeriod ASC", 5, 20, None),
        ("SELECT COUNT(DISTINCT numberOfMoons) FROM sqlite.planets", 1, 1, None),
        ("SELECT * FROM sqlite.planets WHERE name LIKE '%s'", 3, 20, None),
        ("SELECT id, name FROM sqlite.planets WHERE mass BETWEEN 100 AND 500", 1, 2, None),
        ("SELECT * FROM sqlite.planets WHERE orbitalVelocity < 15", 5, 20, None),
        ("SELECT name FROM sqlite.planets WHERE escapeVelocity > 15 AND escapeVelocity < 25", 2, 1, None),
        ("SELECT * FROM sqlite.planets WHERE meanTemperature >= -50 AND meanTemperature <= 50", 1, 20, None),
        ("SELECT name FROM sqlite.planets WHERE orbitalInclination > 1", 7, 1, None),
        ("SELECT id FROM sqlite.planets WHERE perihelion > 200", 6, 1, None),
        ("SELECT * FROM sqlite.planets WHERE aphelion < 1500", 5, 20, None),
#        ("SELECT * FROM sqlite.planets WHERE LENGTH(name) BETWEEN 4 AND 6", expected_rows, 20, None),

        ("SELECT * FROM (SELECT name, diameter FROM sqlite.planets) AS sub WHERE sub.diameter > 5000", 7, 2, None),
        ("SELECT sub.name FROM sqlite.planets JOIN (SELECT name, numberOfMoons FROM sqlite.planets AS B WHERE numberOfMoons > 2) AS sub ON sqlite.planets.name = sub.name", 5, 1, None),
        ("SELECT AVG(sub.mass) FROM (SELECT mass FROM sqlite.planets WHERE mass > 5) AS sub", 1, 1, None),
        ("SELECT sub.name, sqlite.planets.mass FROM sqlite.planets JOIN (SELECT name, orbitalPeriod FROM sqlite.planets AS B WHERE orbitalPeriod < 365) AS sub ON sqlite.planets.name = sub.name", 2, 2, None),

        ("SELECT COUNT(*), orbitalEccentricity FROM sqlite.planets GROUP BY orbitalEccentricity", 9, 2, None),
        ("SELECT name, COUNT(*) FROM sqlite.planets GROUP BY name HAVING COUNT(*) > 1", 0, 2, None),
        ("SELECT * FROM sqlite.planets ORDER BY diameter DESC LIMIT 5", 5, 20, None),
        ("SELECT orbitalPeriod, AVG(mass) FROM sqlite.planets GROUP BY orbitalPeriod", 9, 2, None),
        ("SELECT name FROM sqlite.planets WHERE numberOfMoons > 0 ORDER BY numberOfMoons ASC, name DESC", 7, 1, None),

        ("SELECT a.name, b.name FROM sqlite.planets a JOIN sqlite.planets b ON a.numberOfMoons = b.numberOfMoons WHERE a.name <> b.name", 2, 2, None),
        ("SELECT a.name, b.orbitalPeriod FROM sqlite.planets a INNER JOIN sqlite.planets b ON a.id = b.id WHERE a.mass > 5", 5, 2, None),
        ("SELECT a.name FROM sqlite.planets a LEFT JOIN sqlite.planets b ON a.diameter = b.diameter WHERE b.diameter IS NULL", 0, 1, None),
        ("SELECT a.name, b.name FROM sqlite.planets a, sqlite.planets b WHERE a.mass = b.mass", 9, 2, None),
        ("SELECT a.name FROM sqlite.planets a RIGHT JOIN sqlite.planets b ON a.escapeVelocity = b.escapeVelocity WHERE a.name IS NOT NULL", 9, 1, None),

]


@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_connector_battery(statement, rows, columns, exception):
    """
    Test an battery of statements
    """
    connection_string = "sqlite:///testdata/sqlite/database.db"
    engine = create_engine(connection_string)
    # we're passing an engine rather than a connection string
    opteryx.register_store("sqlite", SqlConnector, remove_prefix=True, engine=engine)


    try:
        result = opteryx.query_to_arrow(statement)
        actual_rows, actual_columns = result.shape

        assert (
            rows == actual_rows
        ), f"Query returned {actual_rows} rows but {rows} were expected"
        f" ({actual_columns} vs {columns})\n{statement}"
        assert (
            columns == actual_columns
        ), f"Query returned {actual_columns} cols but {columns} were"
        f" expected\n{statement}"
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

    from opteryx.utils.formatter import format_sql
    from tests.tools import trunc_printable

    start_suite = time.monotonic_ns()

    width = shutil.get_terminal_size((80, 20))[0] - 15

    passed = 0
    failed = 0

    nl = "\n"

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SQL CONNECTOR TESTS")
    for index, (statement, rows, cols, err) in enumerate(STATEMENTS):
        start = time.monotonic_ns()
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
            test_sql_connector_battery(statement, rows, cols, err)
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

    print("--- ✅ \033[0;32mdone\033[0m")
    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
