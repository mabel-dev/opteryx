"""
Test the connection example from the documentation
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from sqlalchemy.exc import NoSuchTableError, OperationalError

import opteryx
from opteryx.connectors import GcpFireStoreConnector, SqlConnector, register_store
from opteryx.exceptions import DatasetNotFoundError

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


def test_connector_prefixes_negative_tests():
    with pytest.raises((NoSuchTableError, OperationalError)):
        # this should be the SQLAlchemy error
        opteryx.query("SELECT * from planets.planets")

    with pytest.raises(DatasetNotFoundError):
        # this should NOT be the SQLAlchemy error
        opteryx.query("SELECT * FROM planetsplanets.planets")

    with pytest.raises(DatasetNotFoundError):
        # this should NOT be the SQLAlchemy error
        opteryx.query("SELECT * FROM planets_planets.planets")

    with pytest.raises(DatasetNotFoundError):
        opteryx.query("SELECT * FROM fsu.til")


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    test_connector_prefixes_negative_tests()
    run_tests()
