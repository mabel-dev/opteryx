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
    import pandas

    import opteryx

    pandas_df = pandas.read_csv("https://storage.googleapis.com/opteryx/exoplanets/exoplanets.csv")
    opteryx.register_df("exoplanets", pandas_df)
    aggregated_df = opteryx.query(
        "SELECT koi_disposition, COUNT(*) FROM exoplanets GROUP BY koi_disposition;"
    ).pandas()
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
    result = opteryx.query("SELECT * FROM opteryx.space_missions WITH(NO_PARTITION) LIMIT 5;")
    result.head()


def test_readme_5():
    import opteryx
    from opteryx.connectors import SqlConnector

    # this line isn't in the README
    download_file(
        "https://storage.googleapis.com/opteryx/planets/database.db",
        "database.db",
    )

    # Register the store, so we know queries for this store should be handled by
    # the SQL Connector
    opteryx.register_store(
        prefix="sql",
        connector=SqlConnector,
        remove_prefix=True,  # the prefix isn't part of the SQLite table name
        connection="sqlite:///database.db",  # SQLAlchemy connection string
    )
    result = opteryx.query("SELECT * FROM sql.planets LIMIT 5;")
    result.head()


def test_get_started():
    import opteryx

    result = opteryx.query("SELECT * FROM $planets;").arrow()


def test_pandas_integration_input():
    import pandas

    import opteryx

    # Create the DataFrame
    data = {
        "Name": ["Huey", "Dewey", "Louie"],
        "Age": [12, 12, 12],
        "Favorite Color": ["Red", "Blue", "Green"],
    }
    df = pandas.DataFrame(data)

    # Register as a data source
    opteryx.register_df("nephews", df)

    results = opteryx.query("SELECT * FROM nephews").arrow()


def test_pandas_integration_output():
    import opteryx

    dataframe = opteryx.query("SELECT * FROM $planets").pandas()


def test_polars_integration_input():
    import polars

    import opteryx

    # Create the DataFrame
    data = {
        "Name": ["Huey", "Dewey", "Louie"],
        "Age": [12, 12, 12],
        "Favorite Color": ["Red", "Blue", "Green"],
    }
    df = polars.DataFrame(data)

    # Register as a data source
    opteryx.register_df("nephews", df)

    results = opteryx.query("SELECT * FROM nephews").arrow()


def test_polars_integration_output():
    import opteryx

    dataframe = opteryx.query("SELECT * FROM $planets").polars()


def test_permissions_example():
    import opteryx

    conn = opteryx.connect(permissions={"Query"})
    curr = conn.cursor()
    # The user does not have permissions to execute a SHOW COLUMNS statement
    # and this will return a oPermissionsError
    try:
        curr.execute("SHOW COLUMNS FROM $planets")
        print(curr.head())
    except opteryx.exceptions.PermissionsError:
        print("User does not have permission to execute this query")


def test_role_based_permissions():
    import opteryx

    role_permissions = {
        "admin": opteryx.constants.PERMISSIONS,
        "user": {"Query"},
        "agent": {"Execute", "Analyze"},
    }

    def get_user_permissions(user_roles):
        permissions = set()
        for role in user_roles:
            if role in role_permissions:
                permissions |= role_permissions[role]
        return permissions

    perms = get_user_permissions(["admin"])
    assert perms == opteryx.constants.PERMISSIONS
    perms = get_user_permissions(["user"])
    assert perms == {"Query"}
    perms = get_user_permissions(["admin", "user"])
    assert perms == opteryx.constants.PERMISSIONS
    perms = get_user_permissions(["user", "agent"])
    assert perms == {"Query", "Execute", "Analyze"}


if __name__ == "__main__":  # pragma: no cover
    test_documentation_connect_example()
    test_readme_1()  # execute-a-simple-query-in-python
    test_readme_2()  # execute-sql-on-a-pandas-dataframe
    test_readme_3()  # query-data-on-local-disk
    test_readme_4()  # query-data-on-gcs
    test_readme_5()  # query-data-in-sqlite
    test_get_started()
    test_pandas_integration_input()
    test_pandas_integration_output()
    test_polars_integration_input()
    test_polars_integration_output()
    test_permissions_example()
    test_role_based_permissions()

    print("✅ okay")
