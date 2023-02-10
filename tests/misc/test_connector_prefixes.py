"""
Test the connection example from the documentation
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import register_store, SqlConnector, GcpFireStoreConnector

register_store(
    "sqlite",
    SqlConnector,
    remove_prefix=True,
    connection="sqlite:///testdata/sqlite/database.db",
)
register_store(
    "planets",
    SqlConnector,
    remove_prefix=False,
    connection="sqlite:///testdata/sqlite/database.db",
)

register_store("dwarves", GcpFireStoreConnector, remove_prefix=False)
register_store("fs", GcpFireStoreConnector, remove_prefix=True)

os.environ["GCP_PROJECT_ID"] = "mabeldev"


def test_connector_prefixes():
    # don't remove the prefix
    cur = opteryx.query("SELECT * FROM planets")
    assert cur.rowcount == 9

    cur = opteryx.query("SELECT * FROM dwarves")
    assert cur.rowcount == 7

    # remove the prefix
    cur = opteryx.query("SELECT * FROM sqlite.planets")
    assert cur.rowcount == 9

    cur = opteryx.query("SELECT * FROM fs.dwarves")
    assert cur.rowcount == 7, cur.rowcount


if __name__ == "__main__":  # pragma: no cover
    test_connector_prefixes()

    print("✅ okay")
