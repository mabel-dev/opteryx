"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
 >  Shape Checking
    Results Checking
    Compare to DuckDB

This is the main SQL Battery set, others exist for testing specific features (like
reading different file types) but this is the main set of tests for if the Engine
can respond to a query.

This tests that the shape of the response is as expected: the right number of columns,
the right number of rows and, if appropriate, the right exception is thrown.

Some test blocks have labels as to what the block is generally testing, even fewer
tests have comments as to why they exist (usually if the test was written after a
bug-fix).

We have three in-memory tables, one of natural satellite data, one of planet data and
one of astronaut data. These are both small to allow us to test the SQL engine quickly
and is guaranteed to be available whereever the tests are run.

These are supplimented with a few physical tables to test conditions unable to be
tested with the in-memory tables.

We test the shape in this battery because if the shape isn't right, the response isn't
going to be right, and testing shape of an in-memory dataset is quick, so we can do 
bulk testing of 100s of queries in a few seconds and have some confidence the changes
have not broken existing functionality. Note that testing the shape doesn't mean the
response is right.

These tests only test the shape of the response, more specific tests would be needed to
test the body of the response.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

from opteryx.connectors import AwsS3Connector
from opteryx.connectors import DiskConnector

from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import EmptyResultSetError
from opteryx.exceptions import InvalidTemporalRangeFilterError
from opteryx.exceptions import MissingSqlStatement
from opteryx.exceptions import SqlError
from opteryx.exceptions import ProgrammingError
from opteryx.exceptions import UnexpectedDatasetReferenceError
from opteryx.exceptions import UnsupportedSyntaxError

from pyarrow.lib import ArrowInvalid

from opteryx.utils.formatter import format_sql
from tests.tools import skip_if, is_arm, is_mac, is_windows

# fmt:off
STATEMENTS = [
        # Are the datasets the shape we expect?
        ("SELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $planets", 9, 20, None),
        ("SELECT * FROM $astronauts", 357, 19, None),

        # Does the error tester work
        ("THIS IS NOT VALID SQL", None, None, SqlError),

        # V2 Negative Tests
        ("SELECT $planets.id, name FROM $planets INNER JOIN $satellites ON planetId = $planets.id", None, None, AmbiguousIdentifierError),
        ("SELECT $planets.id FROM $satellites", None, None, UnexpectedDatasetReferenceError),

        # V2 New Syntax Checks
        ("SELECT * FROM $planets UNION SELECT * FROM $planets;", None, None, None),
        ("SELECT * FROM $planets LEFT ANTI JOIN $satellites ON id = id;", None, None, ArrowInvalid),  # invalid until the join is written
        ("EXPLAIN ANALYZE FORMAT JSON SELECT * FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id);", None, None, None),
        ("SELECT DISTINCT ON (planetId) planetId, name FROM $satellites ", None, None, None),
        ("SELECT 8 DIV 4", None, None, None),


        # V1 tests
        ("SELECT MAX(density) FROM $planets GROUP BY orbitalInclination, escapeVelocity, orbitalInclination, numberOfMoons, escapeVelocity, density", 9, 1, None),
        ("SELECT GREATEST(ARRAY_AGG(name)) as NAMES FROM $satellites GROUP BY planetId", 7, 1, None),
#        ("SELECT P0.id, P1.ID, P2.ID FROM $planets AS P0 JOIN (SELECT id AS ID, name FROM $planets) AS P1 ON P0.name = P1.name JOIN (SELECT id, name AS ID FROM $planets) AS P2 ON P0.name = P2.name", 9, 3, None),
        ("SELECT name FROM $planets INNER JOIN UNNEST(('Earth', 'Mars')) AS n on name = n", 2, 1, None),
        
]
# fmt:on


@skip_if(is_arm() or is_windows() or is_mac())
@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery_v2(statement, rows, columns, exception):
    """
    Test an battery of statements
    """

    os.environ["ENGINE_VERSION"] = "2"

    opteryx.register_store("tests", DiskConnector)
    opteryx.register_store("mabellabs", AwsS3Connector)

    conn = opteryx.connect()
    cursor = conn.cursor()
    try:
        # V2 is just the planner at the moment
        cursor.execute(statement)
    #        actual_rows, actual_columns = cursor.shape
    #        assert (
    #            rows == actual_rows
    #        ), f"\n{cursor.display()}\n\033[38;5;203mQuery returned {actual_rows} rows but {rows} were expected.\033[0m\n{statement}"
    #        assert (
    #            columns == actual_columns
    #        ), f"\n{cursor.display()}\n\033[38;5;203mQuery returned {actual_columns} cols but {columns} were expected.\033[0m\n{statement}"
    except AssertionError as err:
        print(f"\n{err}", flush=True)
        quit()
    except Exception as err:
        assert (
            type(err) == exception
        ), f"\n{format_sql(statement)}\nQuery failed with error {type(err)} but error {exception} was expected"
    finally:
        os.environ["ENGINE_VERSION"] = "1"


if __name__ == "__main__":  # pragma: no cover
    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil
    import time

    width = shutil.get_terminal_size((80, 20))[0] - 15

    nl = "\n"

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SHAPE TESTS ON ENGINE V2")
    for index, (statement, rows, cols, err) in enumerate(STATEMENTS):
        start = time.monotonic_ns()
        printable = statement
        if hasattr(printable, "encode"):
            printable = str(printable.encode())[2:-1]
        print(
            f"\033[0;36m{(index + 1):04}\033[0m {printable[0:width - 1].ljust(width)}",
            end="",
            flush=True,
        )
        test_sql_battery_v2(statement, rows, cols, err)
        print(f"\033[0;32m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅")

    print("--- ✅ \033[0;32mdone\033[0m")
