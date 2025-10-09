import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import time
import pytest

import opteryx
from opteryx.utils.formatter import format_sql

# Environment setup
os.environ["GCP_PROJECT_ID"] = "mabeldev"

# Define test cases
test_cases = [
    (
        "SELECT user_name FROM testdata.flat.formats.parquet WHERE user_verified = TRUE;",
        711,
        711
    ),
    (
        "SELECT user_name FROM testdata.flat.formats.parquet WHERE user_verified = TRUE and following < 1000;",
        266,
        266
    ),
    (
        "SELECT user_name FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified = TRUE and user_name LIKE '%b%';",
        86,
        711
    ),
    (
        "SELECT * FROM testdata.flat.space_missions WHERE Lauched_at < '2000-01-01';",
        3014,
        3014
    ),
    (
        "SELECT * FROM 'testdata/planets/planets.parquet' WHERE rotationPeriod = lengthOfDay;",
        3,
        9
    ),
]

@pytest.mark.parametrize("query, expected_rowcount, expected_rows_read", test_cases)
def test_predicate_pushdowns_blobs_parquet(query, expected_rowcount, expected_rows_read):
    conn = opteryx.connect()
    cur = conn.cursor()
    
    cur.execute(query)
    cur.materialize()
    
    assert cur.rowcount == expected_rowcount, f"Expected rowcount: {expected_rowcount}, got: {cur.rowcount}"
    assert cur.stats.get("rows_read", 0) == expected_rows_read, f"Expected rows_read: {expected_rows_read}, got: {cur.stats.get('rows_read', 0)}"
    
    conn.close()



if __name__ == "__main__":  # pragma: no cover
    import shutil

    from tests.tools import trunc_printable

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(test_cases)} TESTS")
    for index, (statement, returned_rows, read_rows) in enumerate(test_cases):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(statement), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_predicate_pushdowns_blobs_parquet(statement, returned_rows, read_rows)
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

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
