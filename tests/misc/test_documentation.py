"""
Test the connection example from the documentation
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.tools import download_file


def test_documentation_connect_example():
    import opteryx

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM $planets")
    rows = cur.fetchall()

    # below here is not in the documentation
    rows = list(rows)
    assert len(rows) == 9
    conn.close()


def test_readme_1():
    import opteryx

    result = opteryx.query("SELECT 4 * 7;")
    result.head()


def test_readme_2():
    import opteryx
    import pandas

    pandas_df = pandas.read_csv(
        "https://storage.googleapis.com/opteryx/exoplanets/exoplanets.csv"
    )
    opteryx.register_df("exoplanets", pandas_df)
    aggregated_df = opteryx.query(
        "SELECT koi_disposition, COUNT(*) FROM exoplanets GROUP BY koi_disposition;"
    ).to_df()
    aggregated_df.head()


def test_readme_3():
    import opteryx

    # this line isn't in the README
    download_file(
        "https://storage.googleapis.com/opteryx/space_missions/space_missions.parquet",
        "space_missions.parquet",
    )

    result = opteryx.query("SELECT * FROM 'space_missions.parquet' LIMIT 5;")
    result.head()


def test_readme_4():
    import opteryx
    from opteryx.connectors import GcpCloudStorageConnector

    # Register the store, so we know queries for this store should be handled by
    # the GCS connector
    opteryx.register_store("opteryx", GcpCloudStorageConnector)
    result = opteryx.query(
        "SELECT * FROM opteryx.space_missions WITH(NO_PARTITION) LIMIT 5;"
    )
    result.head()


def test_get_started():
    import opteryx

    result = opteryx.query("SELECT * FROM $planets;").arrow()


if __name__ == "__main__":  # pragma: no cover
    test_documentation_connect_example()
    test_readme_1()
    test_readme_2()
    test_readme_3()
    test_readme_4()
    test_get_started()

    print("✅ okay")
