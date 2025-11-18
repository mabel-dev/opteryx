"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
 >  Shape Checking
    Results Checking
    Compare to DuckDB

This file tests: Basic queries and dataset shape validation

This tests that the shape of the response is as expected: the right number of columns,
the right number of rows and, if appropriate, the right exception is thrown.
"""
import pytest
import os
import sys

#import opteryx

from typing import Optional

sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
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
        # Are the datasets the shape we expect?
        ("SELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $planets", 9, 20, None),
        ("SELECT * FROM $astronauts", 357, 19, None),
        ("SELECT * FROM $no_table", 1, 1, None),
        ("SELECT * FROM sqlite.planets", 9, 20, None),
        ("SELECT * FROM $variables", 42, 5, None),
        ("SELECT * FROM $missions", 4630, 8, None),
        ("SELECT * FROM $statistics", 17, 2, None),
        ("SELECT * FROM $stop_words", 305, 1, None),
        (b"SELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM testdata.missions", 4630, 8, None),
        ("SELECT * FROM testdata.satellites", 177, 8, None),
        ("SELECT * FROM testdata.planets", 9, 20, None),

        ("SELECT COUNT(*) FROM testdata.missions", 1, 1, None),
        ("SELECT COUNT(*) FROM testdata.satellites", 1, 1, None),
        ("SELECT COUNT(*) FROM testdata.planets", 1, 1, None),

        # Does the error tester work
        ("THIS IS NOT VALID SQL", None, None, SqlError),

        # PAGING OF DATASETS AFTER A GROUP BY [#179]
        ("SELECT * FROM (SELECT COUNT(*), column_1 FROM FAKE(5000,2) AS FK GROUP BY column_1 ORDER BY COUNT(*)) AS SQ LIMIT 5", 5, 2, None),
        # FILTER CREATION FOR 3 OR MORE ANDED PREDICATES FAILS [#182]
        ("SELECT * FROM $astronauts WHERE name LIKE '%o%' AND `year` > 1900 AND gender ILIKE '%ale%' AND group IN (1,2,3,4,5,6)", 41, 19, None),

        # Additional basic query patterns - LIMIT and OFFSET
        ("SELECT * FROM $planets LIMIT 5", 5, 20, None),
        ("SELECT * FROM $planets LIMIT 0", 0, 20, None),
        ("SELECT * FROM $planets LIMIT 1", 1, 20, None),
        ("SELECT * FROM $planets OFFSET 5", 4, 20, None),
        ("SELECT * FROM $planets LIMIT 3 OFFSET 2", 3, 20, None),
        ("SELECT * FROM $planets LIMIT 100", 9, 20, None),

        # ORDER BY variations
        ("SELECT * FROM $planets ORDER BY id", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY id DESC", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY name ASC", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY id, name", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY id DESC, name ASC", 9, 20, None),

        # DISTINCT variations
        ("SELECT DISTINCT id FROM $planets", 9, 1, None),
        ("SELECT DISTINCT name FROM $planets", 9, 1, None),
        ("SELECT DISTINCT id, name FROM $planets", 9, 2, None),

        # Basic aggregations
        ("SELECT COUNT(*) FROM $planets", 1, 1, None),
        ("SELECT COUNT(id) FROM $planets", 1, 1, None),
        ("SELECT COUNT(DISTINCT id) FROM $planets", 1, 1, None),
        ("SELECT SUM(id) FROM $planets", 1, 1, None),
        ("SELECT AVG(id) FROM $planets", 1, 1, None),
        ("SELECT MIN(id) FROM $planets", 1, 1, None),
        ("SELECT MAX(id) FROM $planets", 1, 1, None),

        # GROUP BY with aggregations
        ("SELECT COUNT(*) FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT planetId, COUNT(*) FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT planetId, COUNT(*), MAX(id) FROM $satellites GROUP BY planetId", 7, 3, None),

        # WHERE clause variations
        ("SELECT * FROM $planets WHERE id = 1", 1, 20, None),
        ("SELECT * FROM $planets WHERE id != 1", 8, 20, None),
        ("SELECT * FROM $planets WHERE id > 5", 4, 20, None),
        ("SELECT * FROM $planets WHERE id >= 5", 5, 20, None),
        ("SELECT * FROM $planets WHERE id < 5", 4, 20, None),
        ("SELECT * FROM $planets WHERE id <= 5", 5, 20, None),
        ("SELECT * FROM $planets WHERE id BETWEEN 3 AND 6", 4, 20, None),
        ("SELECT * FROM $planets WHERE name LIKE 'M%'", 2, 20, None),
        ("SELECT * FROM $planets WHERE name ILIKE 'm%'", 2, 20, None),
        ("SELECT * FROM $planets WHERE id IN (1, 3, 5)", 3, 20, None),
        ("SELECT * FROM $planets WHERE id NOT IN (1, 3, 5)", 6, 20, None),

        # NULL handling
        ("SELECT * FROM $planets WHERE name IS NULL", 0, 20, None),
        ("SELECT * FROM $planets WHERE name IS NOT NULL", 9, 20, None),

        # Combining conditions
        ("SELECT * FROM $planets WHERE id > 3 AND id < 7", 3, 20, None),
        ("SELECT * FROM $planets WHERE id < 3 OR id > 7", 4, 20, None),
        ("SELECT * FROM $planets WHERE (id > 3 AND id < 7) OR id = 1", 4, 20, None),

        # Column selection variations
        ("SELECT id FROM $planets", 9, 1, None),
        ("SELECT id, name FROM $planets", 9, 2, None),
        ("SELECT name, id FROM $planets", 9, 2, None),
        ("SELECT id, name, id FROM $planets", 9, 3, AmbiguousIdentifierError),

        # Expressions in SELECT
        ("SELECT id * 2 FROM $planets", 9, 1, None),
        ("SELECT id + 1 FROM $planets", 9, 1, None),
        ("SELECT id - 1, id + 1 FROM $planets", 9, 2, None),

        # Subqueries
        ("SELECT * FROM (SELECT * FROM $planets) AS subquery", 9, 20, None),
        ("SELECT * FROM (SELECT id, name FROM $planets) AS subquery", 9, 2, None),
        ("SELECT COUNT(*) FROM (SELECT * FROM $planets WHERE id > 5) AS subquery", 1, 1, None),

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
    opteryx.register_store(
        "iceberg",
        connector=IcebergConnector,
        catalog=iceberg,
        remove_prefix=True,
    )

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

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} BASIC SHAPE TESTS")
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
