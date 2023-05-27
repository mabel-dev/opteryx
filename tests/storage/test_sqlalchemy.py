"""
Test we can read from Sqlite - this is a basic exercise of the SQL Connector
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

from opteryx.connectors import SqlConnector


def test_sqlalchemy():
    from sqlalchemy import create_engine

    connection_string = "sqlite:///testdata/sqlite/database.db"
    engine = create_engine(connection_string)

    opteryx.register_store("sqlite", SqlConnector, remove_prefix=True, engine=engine)

    results = opteryx.query("SELECT * FROM sqlite.planets")
    assert results.rowcount == 9, results.rowcount

    # PROCESS THE DATA IN SOME WAY
    results = opteryx.query("SELECT COUNT(*) FROM sqlite.planets;")
    assert results.rowcount == 1, results.rowcount


if __name__ == "__main__":  # pragma: no cover
    test_sqlalchemy()
    print("✅ okay")
