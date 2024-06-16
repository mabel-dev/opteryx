import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import DiskConnector

STATEMENTS = [
    ("SELECT name FROM testdata.planets;", 1),
    ("SELECT MAX(gravity), MAX(name) FROM testdata.planets;", 2),
    ("SELECT * FROM testdata.planets;", 20),
    ("SELECT id, name, mass FROM testdata.planets;", 3),
    ("SELECT MIN(diameter), AVG(density) FROM testdata.planets;", 2),
    ("SELECT gravity, escapeVelocity, rotationPeriod FROM testdata.planets;", 3),
    ("SELECT distanceFromSun, perihelion, aphelion FROM testdata.planets;", 3),
    ("SELECT orbitalPeriod, orbitalVelocity, orbitalInclination, orbitalEccentricity FROM testdata.planets;", 4),
    ("SELECT obliquityToOrbit, meanTemperature, surfacePressure FROM testdata.planets;", 3),
    ("SELECT numberOfMoons, name FROM testdata.planets;", 2),

]

@pytest.mark.parametrize("query, expected_columns", STATEMENTS)
def test_parquet_projection_pushdown(query, expected_columns):

    cur = opteryx.query(query)
    assert cur.stats["columns_read"] == expected_columns, cur.stats



if __name__ == "__main__":  # pragma: no cover
    import shutil
    import time

    from tests.tools import trunc_printable
    from opteryx.utils.formatter import format_sql

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} TESTS")
    for index, (statement, read_columns) in enumerate(STATEMENTS):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(statement), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_parquet_projection_pushdown(statement, read_columns)
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
