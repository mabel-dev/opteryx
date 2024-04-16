"""
Test we can read from Cassandra (DataStax)

This is our only Cassandra Test
"""

import os
import sys

os.environ["OPTERYX_DEBUG"] = "1"

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import CqlConnector
from tests.tools import is_arm, is_mac, is_windows, skip_if


# skip to reduce billing
@skip_if(is_arm() or is_windows() or is_mac())
def test_datastax_storage():

    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

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

    results = opteryx.query("SELECT * FROM datastax.opteryx.planets")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 20

    # PROCESS THE DATA IN SOME WAY
    results = opteryx.query("SELECT COUNT(*) FROM datastax.opteryx.planets;")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1

    # PUSH A PROJECTION
    results = opteryx.query("SELECT name FROM datastax.opteryx.planets;")
    assert results.rowcount == 9, results.rowcount
    assert results.columncount == 1

    # JOIN ON A NON SQL TABLE
    results = opteryx.query(
        "SELECT * FROM datastax.opteryx.planets AS P INNER JOIN $planets ON P.gravity = $planets.gravity;"
    )
    assert results.rowcount == 11, results.rowcount
    assert results.columncount == 40, results.columncount

    # PUSH - CHECK STATS THE PUSHES WORKED
    results = opteryx.query("SELECT name FROM datastax.opteryx.planets WHERE name LIKE 'Earth';")
    assert results.rowcount == 1, results.rowcount
    assert results.columncount == 1
    assert results.stats["rows_read"] == 1
    assert results.stats["columns_read"] == 1

    results = opteryx.query(
        "SELECT * FROM datastax.opteryx.planets WHERE distanceFromSun < lengthOfDay"
    )
    assert results.rowcount == 2, results.rowcount
    assert results.stats.get("rows_read", 0) == 9, results.stats


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
