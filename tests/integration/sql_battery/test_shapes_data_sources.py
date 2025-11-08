"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
 >  Shape Checking
    Results Checking
    Compare to DuckDB

This file tests: Data source tests (Parquet, Iceberg, multiple sources)

This tests that the shape of the response is as expected: the right number of columns,
the right number of rows and, if appropriate, the right exception is thrown.
"""
import pytest
import os
import sys

#import opteryx

from typing import Optional

sys.path.insert(1, os.path.join(sys.path[0], "../../../draken"))
sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../../rugo"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

# from opteryx.connectors import AwsS3Connector, DiskConnector
from opteryx.exceptions import (
    AmbiguousDatasetError,
    AmbiguousIdentifierError,
    ArrayWithMixedTypesError,
    ColumnNotFoundError,
    ColumnReferencedBeforeEvaluationError,
    DatasetNotFoundError,
    EmptyDatasetError,
    FunctionExecutionError,
    FunctionNotFoundError,
    IncompatibleTypesError,
    InconsistentSchemaError,
    IncorrectTypeError,
    InvalidFunctionParameterError,
    InvalidTemporalRangeFilterError,
    MissingSqlStatement,
    ParameterError,
    PermissionsError,
    SqlError,
    UnexpectedDatasetReferenceError,
    UnnamedColumnError,
    UnsupportedSyntaxError,
    VariableNotFoundError,
)
from opteryx.managers.schemes.mabel_partitions import UnsupportedSegementationError
from opteryx.utils.formatter import format_sql
from opteryx.connectors import IcebergConnector

# fmt:off
# fmt:off
STATEMENTS = [
        # Randomly generated but consistently tested queries (note we have a fuzzer in the full suite)
        ("SELECT * FROM $planets WHERE `name` = 'Earth'", 1, 20, None),
        ("SELECT * FROM $planets WHERE name = 'Mars'", 1, 20, None),
        ("SELECT * FROM $planets WHERE name <> 'Venus'", 8, 20, None),
        ("SELECT * FROM $planets WHERE name = '********'", 0, 20, None),
        ("SELECT id FROM $planets WHERE diameter > 10000", 6, 1, None),
        ("SELECT name, mass FROM $planets WHERE gravity > 5", 6, 2, None),
        ("SELECT * FROM $planets WHERE numberOfMoons >= 5", 5, 20, None),
        ("SELECT * FROM $planets WHERE mass < 10 AND diameter > 1000", 5, 20, None),
        ("SELECT * FROM $planets ORDER BY distanceFromSun DESC", 9, 20, None),
        ("SELECT name FROM $planets WHERE escapeVelocity < 10", 3, 1, None),
        ("SELECT * FROM $planets WHERE obliquityToOrbit > 25", 6, 20, None),
        ("SELECT * FROM $planets WHERE meanTemperature < 0", 6, 20, None),
        ("SELECT * FROM $planets WHERE surfacePressure IS NULL", 4, 20, None),
        ("SELECT name FROM $planets WHERE orbitalEccentricity < 0.1", 7, 1, None),
        ("SELECT * FROM $planets WHERE orbitalPeriod BETWEEN 100 AND 1000", 3, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) <> 5", 6, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) == 5", 3, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) != 5", 6, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM $planets WHERE NOT LENGTH(name) = 5", 6, 20, None),
        ("SELECT * FROM $planets WHERE NOT LENGTH(name) == 5", 6, 20, None),
        ("SELECT * FROM $planets LIMIT 5", 5, 20, None),
        ("SELECT * FROM $planets WHERE numberOfMoons = 0", 2, 20, None),
        ("SELECT id FROM $planets WHERE density > 4000 ORDER BY id ASC", 3, 1, None),
        ("SELECT * FROM $planets WHERE gravity > 10 OR meanTemperature > 100", 4, 20, None),
        ("SELECT * FROM $planets WHERE orbitalInclination <= 5", 7, 20, None),
        ("SELECT * FROM $planets WHERE perihelion >= 150", 6, 20, None),
        ("SELECT name FROM $planets WHERE aphelion < 1000", 5, 1, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) >= 7", 3, 20, None),
        ("SELECT * FROM $planets WHERE id IN (1, 3, 5)", 3, 20, None),
        ("SELECT * FROM $planets WHERE id NOT IN (1, 3, 5)", 6, 20, None),
        ("SELECT * FROM $planets WHERE rotationPeriod < 0", 3, 20, None),
        ("SELECT * FROM $planets WHERE mass > 100 AND mass < 1000", 2, 20, None),
        ("SELECT * FROM $planets WHERE name LIKE 'M%'", 2, 20, None),
        ("SELECT * FROM $planets WHERE orbitalVelocity > 20", 4, 20, None),
        ("SELECT * FROM $planets WHERE escapeVelocity BETWEEN 10 AND 20", 2, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) <= 6", 6, 20, None),
        ("SELECT * FROM $planets ORDER BY id DESC LIMIT 3", 3, 20, None),
        ("SELECT * FROM $planets WHERE gravity IS NOT NULL", 9, 20, None),
        ("SELECT * FROM $planets WHERE surfacePressure IS NOT NULL", 5, 20, None),
        ("SELECT * FROM $planets WHERE meanTemperature <= 100", 7, 20, None),
        ("SELECT * FROM $planets WHERE numberOfMoons > 10", 4, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) > 7", 0, 20, None),
        ("SELECT * FROM $planets WHERE distanceFromSun BETWEEN 100 AND 1000", 4, 20, None),

        # Randomly generated but consistently tested queries
        # the same queries as above, but against parquet, which has a complex reader and
        # is the most used in active deployments
        ("SELECT * FROM testdata.planets WHERE `name` = 'Earth'", 1, 20, None),
        ("SELECT * FROM testdata.planets WHERE name = 'Mars'", 1, 20, None),
        ("SELECT * FROM testdata.planets WHERE name <> 'Venus'", 8, 20, None),
        ("SELECT * FROM testdata.planets WHERE name = '********'", 0, 20, None),
        ("SELECT id FROM testdata.planets WHERE diameter > 10000", 6, 1, None),
        ("SELECT name, mass FROM testdata.planets WHERE gravity > 5", 6, 2, None),
        ("SELECT * FROM testdata.planets WHERE numberOfMoons >= 5", 5, 20, None),
        ("SELECT * FROM testdata.planets WHERE mass < 10 AND diameter > 1000", 5, 20, None),
        ("SELECT * FROM testdata.planets ORDER BY distanceFromSun DESC", 9, 20, None),
        ("SELECT name FROM testdata.planets WHERE escapeVelocity < 10", 3, 1, None),
        ("SELECT * FROM testdata.planets WHERE obliquityToOrbit > 25", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE meanTemperature < 0", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE surfacePressure IS NULL", 4, 20, None),
        ("SELECT name FROM testdata.planets WHERE orbitalEccentricity < 0.1", 7, 1, None),
        ("SELECT * FROM testdata.planets WHERE orbitalPeriod BETWEEN 100 AND 1000", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) <> 5", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) == 5", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) != 5", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE NOT LENGTH(name) = 5", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE NOT LENGTH(name) == 5", 6, 20, None),
        ("SELECT * FROM testdata.planets LIMIT 5", 5, 20, None),
        ("SELECT * FROM testdata.planets WHERE numberOfMoons = 0", 2, 20, None),
        ("SELECT id FROM testdata.planets WHERE density > 4000 ORDER BY id ASC", 3, 1, None),
        ("SELECT * FROM testdata.planets WHERE gravity > 10 OR meanTemperature > 100", 4, 20, None),
        ("SELECT * FROM testdata.planets WHERE orbitalInclination <= 5", 7, 20, None),
        ("SELECT * FROM testdata.planets WHERE perihelion >= 150", 6, 20, None),
        ("SELECT name FROM testdata.planets WHERE aphelion < 1000", 5, 1, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) >= 7", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE id IN (1, 3, 5)", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE id NOT IN (1, 3, 5)", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE rotationPeriod < 0", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE mass > 100 AND mass < 1000", 2, 20, None),
        ("SELECT * FROM testdata.planets WHERE name LIKE 'M%'", 2, 20, None),
        ("SELECT * FROM testdata.planets WHERE orbitalVelocity > 20", 4, 20, None),
        ("SELECT * FROM testdata.planets WHERE escapeVelocity BETWEEN 10 AND 20", 2, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) <= 6", 6, 20, None),
        ("SELECT * FROM testdata.planets ORDER BY id DESC LIMIT 3", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE gravity IS NOT NULL", 9, 20, None),
        ("SELECT * FROM testdata.planets WHERE surfacePressure IS NOT NULL", 5, 20, None),
        ("SELECT * FROM testdata.planets WHERE meanTemperature <= 100", 7, 20, None),
        ("SELECT * FROM testdata.planets WHERE numberOfMoons > 10", 4, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) > 7", 0, 20, None),
        ("SELECT * FROM testdata.planets WHERE distanceFromSun BETWEEN 100 AND 1000", 4, 20, None),

        # Randomly generated but consistently tested queries
        # the same queries as above, but against iceberg, which we anticipate will be our 
        # most utilized data source
        ("SELECT * FROM iceberg.planets WHERE `name` = 'Earth'", 1, 20, None),
        ("SELECT * FROM iceberg.planets WHERE name = 'Mars'", 1, 20, None),
        ("SELECT * FROM iceberg.planets WHERE name <> 'Venus'", 8, 20, None),
        ("SELECT * FROM iceberg.planets WHERE name = '********'", 0, 20, None),
        ("SELECT id FROM iceberg.planets WHERE diameter > 10000", 6, 1, None),
        ("SELECT name, mass FROM iceberg.planets WHERE gravity > 5", 6, 2, None),
        ("SELECT * FROM iceberg.planets WHERE numberOfMoons >= 5", 5, 20, None),
        ("SELECT * FROM iceberg.planets WHERE mass < 10 AND diameter > 1000", 5, 20, None),
        ("SELECT * FROM iceberg.planets ORDER BY distanceFromSun DESC", 9, 20, None),
        ("SELECT name FROM iceberg.planets WHERE escapeVelocity < 10", 3, 1, None),
        ("SELECT * FROM iceberg.planets WHERE obliquityToOrbit > 25", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE meanTemperature < 0", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE surfacePressure IS NULL", 4, 20, None),
        ("SELECT name FROM iceberg.planets WHERE orbitalEccentricity < 0.1", 7, 1, None),
        ("SELECT * FROM iceberg.planets WHERE orbitalPeriod BETWEEN 100 AND 1000", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) <> 5", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) == 5", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) != 5", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE NOT LENGTH(name) = 5", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE NOT LENGTH(name) == 5", 6, 20, None),
        ("SELECT * FROM iceberg.planets LIMIT 5", 5, 20, None),
        ("SELECT * FROM iceberg.planets WHERE numberOfMoons = 0", 2, 20, None),
        ("SELECT id FROM iceberg.planets WHERE density > 4000 ORDER BY id ASC", 3, 1, None),
        ("SELECT * FROM iceberg.planets WHERE gravity > 10 OR meanTemperature > 100", 4, 20, None),
        ("SELECT * FROM iceberg.planets WHERE orbitalInclination <= 5", 7, 20, None),
        ("SELECT * FROM iceberg.planets WHERE perihelion >= 150", 6, 20, None),
        ("SELECT name FROM iceberg.planets WHERE aphelion < 1000", 5, 1, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) >= 7", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE id IN (1, 3, 5)", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE id NOT IN (1, 3, 5)", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE rotationPeriod < 0", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE mass > 100 AND mass < 1000", 2, 20, None),
        ("SELECT * FROM iceberg.planets WHERE name LIKE 'M%'", 2, 20, None),
        ("SELECT * FROM iceberg.planets WHERE orbitalVelocity > 20", 4, 20, None),
        ("SELECT * FROM iceberg.planets WHERE escapeVelocity BETWEEN 10 AND 20", 2, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) <= 6", 6, 20, None),
        ("SELECT * FROM iceberg.planets ORDER BY id DESC LIMIT 3", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE gravity IS NOT NULL", 9, 20, None),
        ("SELECT * FROM iceberg.planets WHERE surfacePressure IS NOT NULL", 5, 20, None),
        ("SELECT * FROM iceberg.planets WHERE meanTemperature <= 100", 7, 20, None),
        ("SELECT * FROM iceberg.planets WHERE numberOfMoons > 10", 4, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) > 7", 0, 20, None),
        ("SELECT * FROM iceberg.planets WHERE distanceFromSun BETWEEN 100 AND 1000", 4, 20, None),

        # Some tests of the same query in different formats
        ("SELECT * FROM $satellites;", 177, 8, None),
        ("SELECT * FROM $satellites\n;", 177, 8, None),
        ("select * from $satellites", 177, 8, None),
        ("Select * From $satellites", 177, 8, None),
        ("SELECT   *   FROM   $satellites", 177, 8, None),
        ("SELECT\n\t*\n  FROM\n\t$satellites", 177, 8, None),
        ("\n\n\n\tSELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites\t;", 177, 8, None),
        ("SELECT *\tFROM $satellites", 177, 8, None),
        ("  SELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites  ", 177, 8, None),
        ("SELECT * \n\n\n FROM $satellites", 177, 8, None),
        ("SeLeCt * FrOm $satellites", 177, 8, None),
        ("SeLeCt * fRoM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites;  ", 177, 8, None),
        ("\t\tSELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites  ;", 177, 8, None),
        ("SELECT * FROM $satellites\n\t;", 177, 8, None),
        ("SeLeCt * FrOm $satellites ;", 177, 8, None),
        ("SELECT\t*\tFROM\t$satellites\t;", 177, 8, None),
        ("  SELECT  *  FROM  $satellites  ;  ", 177, 8, None),
        ("SELECT*\nFROM$satellites", 177, 8, None),
        ("SELECT*FROM$satellites", 177, 8, None),
        ("SELECT *\r\nFROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites\r\n", 177, 8, None),
        ("SELECT * FROM $satellites\r\n;", 177, 8, None),
        ("SeLEcT * fROm $satellites", 177, 8, None),
        ("sElEcT * FrOm $satellites;", 177, 8, None),
        ("\n\r\nSELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites\n\r\n;", 177, 8, None),
        ("\r\nSELECT * FROM $satellites\r\n", 177, 8, None),
        ("SELECT * FROM $satellites\t\r\n;", 177, 8, None),
        ("  \t  SELECT  *  FROM  $satellites  \t  ;  \t  ", 177, 8, None),
        ("SELECT * \n/* comment */\n FROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites\n-- comment\n;", 177, 8, None),
        ("/* comment */SELECT * FROM $satellites--comment", 177, 8, None),
        ("SELECT * FROM $satellites;--comment", 177, 8, None),
        ("SELECT * --comment\nFROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites --comment\n;", 177, 8, None),
        ("""
/* This is a comment */
SELECT *
/* This is a multiline
comment
*/
FROM --
$planets
WHERE /* FALSE AND */
/* FALSE -- */
id > /* 0 */ 1
-- AND name = 'Earth')
         """, 8, 20, None),

        # basic test of the operators
        ("SELECT $satellites.* FROM $satellites", 177, 8, None),
        ("SELECT s.* FROM $satellites AS s", 177, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (name = 'Calypso')", 1, 8, None),
        ("SELECT * FROM $satellites WHERE NOT name = 'Calypso'", 176, 8, None),
        ("SELECT * FROM $satellites WHERE NOT (name = 'Calypso')", 176, 8, None),
        ("SELECT * FROM $satellites WHERE `name` = 'Calypso'", 1, 8, None),
        ("select * from $satellites where name = 'Calypso'", 1, 8, None),
        ("select * from $satellites where name == 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name <> 'Calypso'", 176, 8, None),
        ("SELECT * FROM $satellites WHERE name < 'Calypso'", 21, 8, None),
        ("SELECT * FROM $satellites WHERE name <= 'Calypso'", 22, 8, None),
        ("SELECT * FROM $satellites WHERE name > 'Calypso'", 155, 8, None),
        ("SELECT * FROM $satellites WHERE name >= 'Calypso'", 156, 8, None),
        ("SELECT * FROM $satellites WHERE name is null", 0, 8, None),
        ("SELECT * FROM $satellites WHERE not name is null", 177, 8, None),
        ("SELECT * FROM $satellites WHERE name is not null", 177, 8, None),
        ("SELECT * FROM $satellites WHERE name is true", 0, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE not name is true", 177, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE name is not true", 177, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE name is false", 0, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE not name is false", 177, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE name is not false", 177, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE name != 'Calypso'", 176, 8, None),
        ("SELECT * FROM $satellites WHERE name = '********'", 0, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE '_a_y_s_'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE 'Cal%%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE 'Cal%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name like 'Cal%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name ILIKE '_a_y_s_'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name ILIKE 'Cal%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name ilike 'Cal%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name RLIKE '.a.y.s.'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name RLIKE '^Cal.*'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name rlike '^Cal.*'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name RLIKE '(?i)cal.*'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE TRUE", 177, 8, None),
        ("SELECT * FROM $satellites WHERE FALSE", 0, 8, None),
        ("SELECT * FROM $satellites WHERE NOT TRUE", 0, 8, None),
        ("SELECT * FROM $satellites WHERE NOT FALSE", 177, 8, None),
        ("SELECT * FROM $satellites WHERE `name` = 'Calypso'", 1, 8, None),
        ("SELECT * FROM `$satellites` WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM `$satellites` WHERE `name` = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WITH (NO_CACHE)", 177, 8, None),

]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery(statement:str, rows:int, columns:int, exception: Optional[Exception]):
    """
    Test a battery of statements
    """
    from tests import set_up_iceberg
    from opteryx.connectors import IcebergConnector
    iceberg = set_up_iceberg()
    opteryx.register_store("iceberg", connector=IcebergConnector, catalog=iceberg)

    from opteryx.connectors import DiskConnector, SqlConnector
    from opteryx.managers.schemes import MabelPartitionScheme

    opteryx.register_store(
        "testdata.partitioned", DiskConnector, partition_scheme=MabelPartitionScheme
    )
    opteryx.register_store(
        "sqlite",
        SqlConnector,
        remove_prefix=True,
        connection="sqlite:///testdata/sqlite/database.db",
    )

    try:
        # query to arrow is the fastest way to query
        result = opteryx.query_to_arrow(statement, memberships=["Apollo 11", "opteryx"])
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
    except AssertionError as error:
        raise error
    except Exception as error:
        if not type(error) == exception:
            raise ValueError(
                f"{format_sql(statement)}\nQuery failed with error {type(error)} but error {exception} was expected"
            ) from error


if __name__ == "__main__":  # pragma: no cover
    import shutil
    import time
    from tests import trunc_printable

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 15
    passed:int = 0
    failed:int = 0
    nl:str = "\n"
    failures = []

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} DATA_SOURCES SHAPE TESTS")
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
                print(f" \033[0;31m{failed}\033[0m")
            else:
                print()
        except Exception as err:
            failed += 1
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ {failed}\033[0m")
            print(">", err)
            failures.append((statement, err))

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for statement, err in failures:
            print(err)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )

    # Exit with appropriate code to signal success/failure to parent process
    if failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)
