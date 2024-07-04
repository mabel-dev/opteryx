"""
Test we can read from Cassandra (DataStax)

This is our only Cassandra Test.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import CqlConnector
from opteryx.utils.formatter import format_sql
from tests.tools import is_arm, is_mac, is_windows, skip_if, is_version

# fmt:off
test_cases = [
    ("SELECT * FROM datastax.opteryx.planets", 9, 20, None),
    ("SELECT COUNT(*) FROM datastax.opteryx.planets", 1, 1, None),
    ("SELECT name FROM datastax.opteryx.planets", 9, 1, None),
    ("SELECT * FROM datastax.opteryx.planets AS P INNER JOIN $planets ON P.gravity = $planets.gravity", 11, 40, None),
    ("SELECT name FROM datastax.opteryx.planets WHERE name LIKE 'Earth'", 1, 1, {"rows_read": 1, "columns_read": 1}),
    ("SELECT * FROM datastax.opteryx.planets WHERE distanceFromSun < lengthOfDay", 2, None, {"rows_read": 9}),
    ("SELECT * FROM datastax.opteryx.planets WHERE mass > 0.5", 7, 20, None),
    ("SELECT * FROM datastax.opteryx.planets WHERE name = 'Earth'", 1, 20, None),
    ("SELECT * FROM datastax.opteryx.planets WHERE name NOT LIKE 'Mars'", 8, 20, None),
    ("SELECT AVG(mass) FROM datastax.opteryx.planets", 1, 1, None),
    ("SELECT MIN(distanceFromSun) FROM datastax.opteryx.planets", 1, 1, None),
    ("SELECT MAX(lengthOfDay) FROM datastax.opteryx.planets", 1, 1, None),
    ("SELECT UPPER(name), ROUND(mass, 2) FROM datastax.opteryx.planets", 9, 2, None),
    ("SELECT surfacePressure, COUNT(*) FROM datastax.opteryx.planets GROUP BY surfacePressure HAVING COUNT(*) > 1", 1, 2, None),
    ("SELECT * FROM datastax.opteryx.planets WHERE mass > 0.1 AND distanceFromSun < 500", 4, 20, None),
    ("SELECT name, SIGNUM(mass) AS sin_mass FROM datastax.opteryx.planets", 9, 2, None),
    ("SELECT name, CASE WHEN mass > 1 THEN 'heavy' ELSE 'light' END FROM datastax.opteryx.planets", 9, 2, None),
    ("SELECT name FROM datastax.opteryx.planets WHERE surfacePressure IS NULL", 4, 1, None),
    ("SELECT name FROM datastax.opteryx.planets WHERE surfacePressure IS NOT NULL", 5, 1, None),
    ("SELECT name FROM datastax.opteryx.planets WHERE numberOfMoons IS NOT TRUE", 8, 1, None),
]
# fmt:on


# skip to reduce billing
@skip_if(is_arm() or is_windows() or is_mac() or is_version("3.10"))
@pytest.mark.parametrize(
    "query, expected_rowcount, expected_columncount, expected_stats", test_cases
)
def test_datastax_storage(query, expected_rowcount, expected_columncount, expected_stats):
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    # We're connecting to DataStax
    cloud_config = {"secure_connect_bundle": "secure-connect.zip"}

    CLIENT_ID = os.environ["DATASTAX_CLIENT_ID"]
    CLIENT_SECRET = os.environ["DATASTAX_CLIENT_SECRET"]

    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)

    opteryx.register_store(
        "datastax",
        CqlConnector,
        remove_prefix=True,
        cluster=cluster,
    )

    results = opteryx.query(query)

    assert (
        results.rowcount == expected_rowcount
    ), f"Expected row count {expected_rowcount}, got {results.rowcount}"

    if expected_columncount is not None:
        assert (
            results.columncount == expected_columncount
        ), f"Expected column count {expected_columncount}, got {results.columncount}"

    if expected_stats:
        for key, expected_value in expected_stats.items():
            actual_value = results.stats.get(key, None)
            assert (
                actual_value == expected_value
            ), f"Expected {key} {expected_value}, got {actual_value}"


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

    print(f"RUNNING BATTERY OF {len(test_cases)} DATASTAX TESTS")
    for index, (statement, rows, cols, expected_stats) in enumerate(test_cases):
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
            test_datastax_storage(statement, rows, cols, expected_stats)
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
