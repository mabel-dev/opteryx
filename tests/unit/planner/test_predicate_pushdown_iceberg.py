import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import time
import pytest

import opteryx
from opteryx.connectors import IcebergConnector
from opteryx.utils.formatter import format_sql
from tests import set_up_iceberg

# Define test cases
#fmt:off
test_cases = [
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_verified = TRUE;", 711, 711),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_verified = TRUE and following < 1000;", 266, 266),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_verified = FALSE and followers > 1000;", 28522, 28522),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE followers >= 5000;", 7123, 7123),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE following <= 1;", 831, 831),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_verified = TRUE and user_name = 'Dakota News Now';", 1, 1),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_name = 'Dakota News Now';", 1, 1),
    ("SELECT user_name FROM iceberg.opteryx.tweets WITH(NO_PARTITION) WHERE user_verified = TRUE and user_name LIKE '%b%';", 86, 711),
    ("SELECT * FROM iceberg.opteryx.missions WHERE Lauched_at < '2000-01-01';", 3014, 3014),
    ("SELECT * FROM iceberg.opteryx.planets WHERE rotationPeriod = lengthOfDay;", 3, 9),
    ("SELECT * FROM iceberg.opteryx.planets WHERE gravity > 10;", 2, 2),
    ("SELECT * FROM iceberg.opteryx.planets WHERE gravity > 10.0;", 2, 2),
    ("SELECT * FROM iceberg.opteryx.planets WHERE mass = 0;", 0, 0),
    ("SELECT * FROM iceberg.opteryx.planets WHERE name != 'Earth';", 8, 8),
    ("SELECT * FROM iceberg.opteryx.planets WHERE name != 'Earth' AND mass < 10;", 4, 4),
    ("SELECT * FROM iceberg.opteryx.planets WHERE name NOT LIKE '%Earth%' AND mass < 10;", 4, 5),
    ("SELECT * FROM iceberg.opteryx.planets WHERE id >= 2.5;", 7, 7),
    ("SELECT * FROM iceberg.opteryx.planets WHERE id >= pi();", 6, 6),
    ("SELECT * FROM iceberg.opteryx.planets WHERE mass <= pi();", 3, 3),

    # Boolean predicates
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_verified IS NULL;", 0, 100000),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE NOT user_verified;", 99289, 100000),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_verified = FALSE OR followers < 500;", 99291, 100000),

    # String predicates
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_name LIKE 'A%';", 4700, 100000),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_name ILIKE 'a%';", 6800, 100000),  # Case-insensitive LIKE
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_name NOT LIKE '%news%';", 99980, 100000),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE LENGTH(user_name) > 10;", 52972, 100000),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_name IN ('Elon Musk', 'Barack Obama', 'Opteryx Bot');", 1, 100000),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE user_name NOT IN ('Elon Musk', 'Barack Obama');", 99999, 100000),

    # Integer predicates
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE followers BETWEEN 500 AND 1000;", 14596, 14596),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE following + followers > 2000;", 31134, 100000),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE following - followers < 0;", 37319, 100000),
    ("SELECT user_name FROM iceberg.opteryx.tweets WHERE tweet_id % 2 = 0;", 62270, 100000),  # Even IDs

    # Floating-point predicates
    ("SELECT * FROM iceberg.opteryx.planets WHERE gravity BETWEEN 9.5 AND 10.5;", 1, 1),
    ("SELECT * FROM iceberg.opteryx.planets WHERE gravity * 2 > 20;", 2, 9),
    ("SELECT * FROM iceberg.opteryx.planets WHERE gravity / 2 < 5;", 7, 9),
    ("SELECT * FROM iceberg.opteryx.planets WHERE mass = CAST(5 AS DOUBLE);", 0, 0),

    # Date & timestamp predicates
    ("SELECT * FROM iceberg.opteryx.missions WHERE Lauched_at >= DATE '2020-01-01';", 368, 368),
    ("SELECT * FROM iceberg.opteryx.missions WHERE EXTRACT(YEAR FROM Lauched_at) = 1999;", 53, 4630),
    ("SELECT * FROM iceberg.opteryx.missions WHERE Lauched_at BETWEEN '1969-01-01' AND '1970-12-31';", 207, 207),

    # NULL handling
    ("SELECT * FROM iceberg.opteryx.planets WHERE COALESCE(mass, 0) = 0;", 0, 9),

    # Edge cases
#    ("SELECT * FROM iceberg.opteryx.planets WHERE gravity IN (1.5, 3.2, 5.7, 9.8);", 0, 9),
    ("SELECT * FROM iceberg.opteryx.planets WHERE id IN (1.5, 3.2, 5.7, 9.8);", 0, 9),
    ("SELECT * FROM iceberg.opteryx.planets WHERE id BETWEEN 0 AND 5;", 5, 5),
    ("SELECT * FROM iceberg.opteryx.planets WHERE gravity > PI();", 8, 8),
    ("SELECT * FROM iceberg.opteryx.planets WHERE mass < PI() * 2;", 5, 5),
    ("SELECT * FROM iceberg.opteryx.planets WHERE id * 2 >= 10;", 5, 9),
]
#fmt:on


@pytest.mark.parametrize("query, expected_rowcount, expected_rows_read", test_cases)
def test_predicate_pushdowns_blobs_parquet(query, expected_rowcount, expected_rows_read):
    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    cur = opteryx.query(query)
    cur.materialize()

    assert cur.rowcount == expected_rowcount, (
        f"Expected rowcount: {expected_rowcount}, got: {cur.rowcount}"
    )
    assert cur.stats.get("rows_read", 0) == expected_rows_read, (
        f"Expected rows_read: {expected_rows_read}, got: {cur.stats.get('rows_read', 0)}"
    )


if __name__ == "__main__":  # pragma: no cover
    import shutil

    from tests import trunc_printable

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
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start) / 1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(" \033[0;31m*\033[0m")
            else:
                print()
        except Exception as err:
            print(
                f"\033[0;31m{str(int((time.monotonic_ns() - start) / 1e6)).rjust(4)}ms ❌ *\033[0m"
            )
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
