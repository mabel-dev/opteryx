from collections import namedtuple
import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import GcpCloudStorageConnector
from opteryx.utils.formatter import format_sql

# Define the test case structure
TestCase = namedtuple("TestCase", "query expected_rowcount expected_columncount stats")

# Example parameterization data using named tuples for clarity
BUCKET_NAME = "opteryx"
test_cases = [
    TestCase(
        query=f"SELECT * FROM {BUCKET_NAME}.space_missions;",
        expected_rowcount=4630,
        expected_columncount=8,
        stats={},
    ),
    TestCase(
        query=f"SELECT * FROM {BUCKET_NAME}.space_missions LIMIT 10;",
        expected_rowcount=10,
        expected_columncount=8,
        stats={},
    ),
    TestCase(
        query=f"SELECT kepid, kepoi_name FROM {BUCKET_NAME}.exoplanets ORDER BY kepid DESC LIMIT 5;",
        expected_rowcount=5,
        expected_columncount=2,
        stats={},
    ),
    TestCase(
        query=f"SELECT COUNT(*) AS Missions, Company FROM {BUCKET_NAME}.space_missions GROUP BY Company;",
        expected_rowcount=62,
        expected_columncount=2,
        stats={},
    ),
    TestCase(
        query=f"SELECT Company FROM {BUCKET_NAME}.space_missions WHERE Rocket_Status = 'Active';",
        expected_rowcount=1010,
        expected_columncount=1,
        stats={"columns_read": 1, "rows_read": 1010},
    ),
    TestCase(
        query=f"SELECT name, kepler_name FROM {BUCKET_NAME}.exoplanets AS exoplanets INNER JOIN $planets AS planets ON rowid = id",
        expected_rowcount=9,
        expected_columncount=2,
        stats={},
    ),
    TestCase(
        query=f"SELECT name, kepler_name FROM {BUCKET_NAME}.exoplanets AS exoplanets INNER JOIN $planets AS planets ON rowid = id LIMIT 5",
        expected_rowcount=5,
        expected_columncount=2,
        stats={"columns_read": 2},
    ),
]


@pytest.mark.parametrize("test_case", test_cases)
def test_gcs_storage(test_case):
    opteryx.register_store(BUCKET_NAME, GcpCloudStorageConnector)

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute(test_case.query)

    # Assertions for rowcount and columncount
    assert (
        cur.rowcount == test_case.expected_rowcount
    ), f"Expected rowcount {test_case.expected_rowcount}, got {cur.rowcount}"
    assert (
        cur.columncount == test_case.expected_columncount
    ), f"Expected columncount {test_case.expected_columncount}, got {cur.columncount}"

    # Assertions for statistics
    for key, expected_value in test_case.stats.items():
        actual_value = cur.stats.get(key, None)
        assert (
            actual_value == expected_value
        ), f"Stats check failed for {key}: expected {expected_value}, got {actual_value}"

    conn.close()


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

    print(f"RUNNING BATTERY OF {len(test_cases)} Google Cloud Storage TESTS")
    for index, test_case in enumerate(test_cases):
        (statement, rows, cols, stats) = test_case

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
            test_gcs_storage(test_case)
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
