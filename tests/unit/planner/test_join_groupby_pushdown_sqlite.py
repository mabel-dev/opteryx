import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector


opteryx.register_store(
    "sqlite",
    SqlConnector,
    remove_prefix=True,
    connection="sqlite:///testdata/sqlite/database.db",
)


def test_join_groupby_sqlite():
    # simple join
    q = "SELECT p.name AS planet, s.name AS satellite FROM sqlite.planets AS p INNER JOIN sqlite.satellites AS s ON p.id = s.planetId WHERE p.id = 1"
    res = opteryx.query(q)
    res.materialize()
    assert res.rowcount >= 0

    # simple group by
    q2 = "SELECT name, COUNT(*) AS cnt FROM sqlite.satellites GROUP BY name LIMIT 5"
    res2 = opteryx.query(q2)
    res2.materialize()
    assert res2.rowcount >= 0
