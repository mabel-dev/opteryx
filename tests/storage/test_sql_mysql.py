"""
Test we can read from Sqlite - this is a basic exercise of the SQL Connector
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

from opteryx.connectors import SqlConnector

MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
MYSQL_USER = os.environ.get("MYSQL_USER")


def test_mysql_storage():
    opteryx.register_store(
        "mysql",
        SqlConnector,
        remove_prefix=True,
        connection=f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@mysql.ci.opteryx.info/opteryx?charset=utf8mb4",
    )

    results = opteryx.query("SELECT * FROM mysql.planets")
    assert results.rowcount == 9, results.rowcount

    # PROCESS THE DATA IN SOME WAY
    results = opteryx.query("SELECT COUNT(*) FROM mysql.planets;")
    assert results.rowcount == 1, results.rowcount


if __name__ == "__main__":  # pragma: no cover
    test_mysql_storage()
    print("✅ okay")
