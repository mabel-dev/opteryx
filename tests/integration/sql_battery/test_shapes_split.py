"""
Basic shape tests for `testdata.split` dataset

This mirrors the style and setup in `test_shapes_basic.py` with a small set of
queries to validate the shapes (row/column counts) returned by the queries.
"""
import pytest
import os
import sys

from typing import Optional

sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

from opteryx.exceptions import (
    SqlError,
    UnsupportedSyntaxError,
)
from opteryx.utils.formatter import format_sql

# fmt:off
STATEMENTS = [
    ("SELECT * FROM testdata.split", 10000, 7, None),
    ("SELECT COUNT(*) FROM testdata.split", 1, 1, None),
    ("SELECT id, server FROM testdata.split LIMIT 25", 25, 2, None),
    ("SELECT * FROM testdata.split WHERE id = 10000", 1, 7, None),

    # FILTER and GROUP checks
    ("SELECT * FROM testdata.split WHERE times_found = 0", 827, 7, None),
    ("SELECT * FROM testdata.split WHERE risk_score > 9.5", 672, 7, None),
    ("SELECT * FROM testdata.split WHERE server LIKE 'alpha%'", 1038, 7, None),
    ("SELECT * FROM testdata.split WHERE cves[1] IS NOT NULL", 6672, 7, None),
    ("SELECT * FROM testdata.split WHERE cves[2] IS NOT NULL", 3373, 7, None),
    ("SELECT server, COUNT(*) FROM testdata.split GROUP BY server", 1000, 2, None),
    ("SELECT times_found, COUNT(*) FROM testdata.split GROUP BY times_found", 11, 2, None),
    ("SELECT EXTRACT(year FROM first_found) AS first_year, COUNT(*) FROM testdata.split GROUP BY EXTRACT(year FROM first_found)", 4, 2, None),
    ("SELECT server, COUNT(*) FROM testdata.split GROUP BY server HAVING COUNT(*) > 5", 950, 2, None),
    ("SELECT server, COUNT(*) FROM testdata.split GROUP BY server ORDER BY COUNT(*) DESC LIMIT 10", 10, 2, None),

    # Combined DISTINCT + FILTER + GROUP checks
    ("SELECT DISTINCT server FROM testdata.split WHERE times_found = 0", 577, 1, None),
    ("SELECT COUNT(DISTINCT server) FROM testdata.split WHERE risk_score > 9.5", 1, 1, None),
    ("SELECT DISTINCT SUBSTR(server, 1, 5) FROM testdata.split WHERE server LIKE 'alpha%'", 1, 1, None),
    ("SELECT DISTINCT cves FROM testdata.split WHERE times_found = 0", 827, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE cves[1] IS NOT NULL", 1000, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE cves[2] IS NOT NULL", 954, 1, None),
    ("SELECT server, COUNT(DISTINCT times_found) FROM testdata.split GROUP BY server", 1000, 2, None),
    ("SELECT server, COUNT(DISTINCT patch_id) FROM testdata.split GROUP BY server ORDER BY server LIMIT 5", 5, 2, None),
    ("SELECT DISTINCT times_found FROM testdata.split WHERE cves[1] IS NOT NULL", 11, 1, None),
    ("SELECT DISTINCT EXTRACT(year FROM first_found) FROM testdata.split WHERE risk_score > 5", 4, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE risk_score > 5", 997, 1, None),
    ("SELECT COUNT(DISTINCT patch_id) FROM testdata.split WHERE server LIKE 'alpha%'", 1, 1, None),
    ("SELECT DISTINCT patch_id FROM testdata.split WHERE risk_score > 9.5", 672, 1, None),
    ("SELECT DISTINCT EXTRACT(year FROM first_found) FROM testdata.split WHERE cves[1] IS NOT NULL", 4, 1, None),
    ("SELECT server, COUNT(DISTINCT patch_id) FROM testdata.split GROUP BY server HAVING COUNT(DISTINCT patch_id) > 5", 950, 2, None),
    ("SELECT server, COUNT(DISTINCT risk_score) FROM testdata.split GROUP BY server LIMIT 10", 10, 2, None),
    ("SELECT DISTINCT SUBSTR(server, 1, 3) FROM testdata.split", 10, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE times_found > 0 AND cves[2] IS NOT NULL", 949, 1, None),
    ("SELECT COUNT(DISTINCT cves) FROM testdata.split WHERE EXTRACT(year FROM first_found) >= 2024", 1, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE risk_score BETWEEN 5 AND 7", 911, 1, None),
    ("SELECT DISTINCT server FROM (SELECT * FROM testdata.split WHERE times_found = 0) AS s", 577, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE times_found = 0 AND risk_score > 7", 283, 1, None),
    ("SELECT DISTINCT server FROM (SELECT * FROM testdata.split WHERE times_found = 0 AND risk_score > 7) s", 283, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE cves[1] IS NOT NULL AND times_found = 0", 437, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE cves[2] IS NOT NULL AND times_found = 0", 256, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE SUBSTR(server,1,3) = 'alp' AND times_found = 0", 64, 1, None),
    ("SELECT DISTINCT CONCAT(server, '-', CAST(times_found AS VARCHAR)) FROM testdata.split WHERE times_found = 0", 577, 1, None),
    ("SELECT DISTINCT CONCAT(server, '-', CAST(times_found AS VARCHAR)) FROM (SELECT * FROM testdata.split WHERE times_found = 0) s", 577, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE server IN (SELECT server FROM testdata.split WHERE times_found = 0)", None, None, UnsupportedSyntaxError),
    ("SELECT DISTINCT server FROM testdata.split WHERE risk_score = (SELECT MAX(risk_score) FROM testdata.split)", None, None, SqlError),
    ("SELECT server, COUNT(DISTINCT patch_id) FROM testdata.split WHERE times_found = 0 GROUP BY server", 577, 2, None),
    ("SELECT DISTINCT server FROM testdata.split ORDER BY server LIMIT 10", 10, 1, None),
    ("SELECT DISTINCT server FROM testdata.split ORDER BY server OFFSET 5 LIMIT 10", 10, 1, None),
    ("SELECT DISTINCT server FROM (SELECT server FROM testdata.split WHERE times_found = 0 ORDER BY server LIMIT 50) s", 28, 1, None),
    ("SELECT DISTINCT server FROM (SELECT server, risk_score FROM testdata.split WHERE risk_score > 7) s", 981, 1, None),
    ("SELECT DISTINCT SUBSTR(server, 5, 3) FROM testdata.split", 3, 1, None),
    ("SELECT DISTINCT CAST(times_found AS VARCHAR) FROM testdata.split", 11, 1, None),
    ("SELECT DISTINCT DATE_TRUNC('day', first_found) FROM testdata.split", 1097, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE times_found BETWEEN 1 AND 3", 998, 1, None),
    ("SELECT DISTINCT cves[1] FROM testdata.split WHERE risk_score > 9", 961, 1, None),
    ("SELECT DISTINCT CONCAT(server, '-', CAST(times_found AS VARCHAR)) FROM (SELECT * FROM testdata.split WHERE times_found = 0) s", 577, 1, None),
    ("SELECT DISTINCT server FROM (SELECT DISTINCT server, risk_score FROM testdata.split) s WHERE risk_score > 9.5", 491, 1, None),

    # DISTINCT checks
    ("SELECT DISTINCT server FROM testdata.split", 1000, 1, None),
    ("SELECT DISTINCT patch_id FROM testdata.split", 10000, 1, None),
    ("SELECT DISTINCT times_found FROM testdata.split", 11, 1, None),
    ("SELECT DISTINCT cves FROM testdata.split", 9999, 1, None),
    ("SELECT DISTINCT risk_score FROM testdata.split", 6086, 1, None),
    ("SELECT DISTINCT EXTRACT(year FROM first_found) AS first_year FROM testdata.split", 4, 1, None),
    ("SELECT DISTINCT server, times_found FROM testdata.split", 4850, 2, None),
    ("SELECT DISTINCT server, patch_id FROM testdata.split", 10000, 2, None),
    ("SELECT DISTINCT server, risk_score FROM testdata.split", 9995, 2, None),
    ("SELECT DISTINCT SUBSTR(server, 1, 5) FROM testdata.split", 10, 1, None),

    # More DISTINCT tests to reproduce edge cases/bugs
    ("SELECT DISTINCT CAST(risk_score AS INTEGER) FROM testdata.split", 11, 1, None),
    ("SELECT DISTINCT CONCAT(server, '-', CAST(times_found AS VARCHAR)) FROM testdata.split", 4850, 1, None),
    ("SELECT DISTINCT ROUND(risk_score, 1) FROM testdata.split", 101, 1, None),
    ("SELECT DISTINCT SUBSTR(server, 1, 2) FROM testdata.split", 10, 1, None),
    ("SELECT DISTINCT SUBSTR(patch_id, 3, 2) FROM testdata.split", 90, 1, None),
    ("SELECT DISTINCT SUBSTR(cves[1],1,9) FROM testdata.split WHERE cves[1] IS NOT NULL", 9, 1, None),
    ("SELECT DISTINCT COUNT(DISTINCT server) FROM testdata.split WHERE risk_score > 8", 1, 1, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE EXTRACT(year FROM first_found) = 2024", 959, 1, None),
    ("SELECT DISTINCT patch_id FROM testdata.split WHERE risk_score > 8", 2804, 1, None),
    ("SELECT DISTINCT LOWER(server) FROM testdata.split", 1000, 1, None),
    ("SELECT DISTINCT server, times_found FROM testdata.split WHERE times_found > 0", 4273, 2, None),
    ("SELECT DISTINCT server, times_found FROM (SELECT server, times_found FROM testdata.split WHERE times_found > 0) s", 4273, 2, None),
    ("SELECT DISTINCT server, times_found FROM testdata.split WHERE times_found > 0 ORDER BY times_found DESC LIMIT 10", 10, 2, None),
    ("SELECT DISTINCT server FROM testdata.split WHERE server LIKE 'alpha%' ORDER BY server LIMIT 5 OFFSET 2", 5, 1, None),
    ("SELECT DISTINCT id FROM testdata.split WHERE id % 10 = 0", 1000, 1, None),
]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery(statement:str, rows:int, columns:int, exception: Optional[Exception]):
    """
    Test a battery of statements
    """
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

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SPLIT PARQUET SHAPE TESTS")
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

