"""
Test predicate pushdown using the blob reader
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx import config


def test_predicate_pushdowns_blobs_parquet():
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()

    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute(
        "SET enable_optimizer = false; SELECT user_name FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified = TRUE;"
    )
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 711, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 100000, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT user_name FROM testdata.flat.formats.parquet WITH(NO_PARTITION, NO_PUSH_SELECTION) WHERE user_verified = TRUE;"
    )
    # if we disable pushdown, we read all the rows from the source and we do the filter
    assert cur.rowcount == 711, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 100000, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT user_name FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified = TRUE;"
    )
    # when pushdown is enabled, we only read the matching rows from the source
    assert cur.rowcount == 711, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 711, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT user_name FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified = TRUE and following < 1000;"
    )
    # test with a more complex filter
    assert cur.rowcount == 266, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 266, cur.stats

    cur = conn.cursor()
    cur.execute(
        "SELECT user_name FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified = TRUE and user_name LIKE '%b%';"
    )
    # we don't push all predicates down,
    assert cur.rowcount == 86, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 711, cur.stats

    config.ONLY_PUSH_EQUALS_PREDICATES = True
    cur = conn.cursor()
    cur.execute(
        "SELECT user_name FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified = TRUE and following < 1000;"
    )
    # test only push equals
    assert cur.rowcount == 266, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 711, cur.stats
    config.ONLY_PUSH_EQUALS_PREDICATES = False

    # identifier = identifier isn't pushed - file reader
    config.ONLY_PUSH_EQUALS_PREDICATES = True
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM 'testdata/flat/planets/parquet/planets.parquet' WITH(NO_PARTITION) WHERE rotationPeriod = lengthOfDay;"
    )
    assert cur.rowcount == 3, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 9, cur.stats
    config.ONLY_PUSH_EQUALS_PREDICATES = False

    # push the >, not the eq
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM 'testdata/flat/planets/parquet/planets.parquet' WITH(NO_PARTITION) WHERE rotationPeriod = lengthOfDay AND id > 5;"
    )
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 4, cur.stats

    # identifier = identifier isn't pushed - blob reader
    config.ONLY_PUSH_EQUALS_PREDICATES = True
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM 'testdata.flat.planets.parquet' WITH(NO_PARTITION) WHERE rotationPeriod = lengthOfDay;"
    )
    assert cur.rowcount == 3, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 9, cur.stats
    config.ONLY_PUSH_EQUALS_PREDICATES = False

    # identifier = identifier isn't pushed - blob reader
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM 'testdata.flat.planets.parquet' WITH(NO_PARTITION) WHERE rotationPeriod = lengthOfDay AND id > 5;"
    )
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats.get("rows_read", 0) == 4, cur.stats

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
