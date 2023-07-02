"""
Test we can read from DuckDB - this is a basic exercise of the SQL Connector
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import SqlConnector

CREATE_DB = """
CREATE TABLE planets (
  id INTEGER PRIMARY KEY,
  name VARCHAR(20),
  mass DECIMAL(5, 1),
  diameter INTEGER,
  density DECIMAL(5, 1),
  gravity DECIMAL(5, 1),
  escapeVelocity DECIMAL(5, 1),
  rotationPeriod DECIMAL(5, 1),
  lengthOfDay DECIMAL(5, 1),
  distanceFromSun DECIMAL(5, 1),
  perihelion DECIMAL(5, 1),
  aphelion DECIMAL(5, 1),
  orbitalPeriod DECIMAL(7, 1),
  orbitalVelocity DECIMAL(5, 1),
  orbitalInclination DECIMAL(5, 1),
  orbitalEccentricity DECIMAL(5, 3),
  obliquityToOrbit DECIMAL(5, 1),
  meanTemperature INTEGER,
  surfacePressure DECIMAL(7, 5),
  numberOfMoons INTEGER
);

INSERT INTO planets (id, name, mass, diameter, density, gravity, escapeVelocity, rotationPeriod, lengthOfDay, distanceFromSun, perihelion, aphelion, orbitalPeriod, orbitalVelocity, orbitalInclination, orbitalEccentricity, obliquityToOrbit, meanTemperature, surfacePressure, numberOfMoons)
VALUES 
  (1, 'Mercury', 0.33, 4879, 5427, 3.7, 4.3, 1407.6, 4222.6, 57.9, 46, 69.8, 88, 47.4, 7, 0.205, 0.034, 167, 0, 0),
  (2, 'Venus', 4.87, 12104, 5243, 8.9, 10.4, -5832.5, 2802, 108.2, 107.5, 108.9, 224.7, 35, 3.4, 0.007, 177.4, 464, 92, 0),
  (3, 'Earth', 5.97, 12756, 5514, 9.8, 11.2, 23.9, 24, 149.6, 147.1, 152.1, 365.2, 29.8, 0, 0.017, 23.4, 15, 1, 1),
  (4, 'Mars', 0.642, 6792, 3933, 3.7, 5, 24.6, 24.7, 227.9, 206.6, 249.2, 687, 24.1, 1.9, 0.094, 25.2, -65, 0.01, 2),
  (5, 'Jupiter', 1898, 142984, 1326, 23.1, 59.5, 9.9, 9.9, 778.6, 740.5, 816.6, 4331, 13.1, 1.3, 0.049, 3.1, -110, CAST(NULL AS INTEGER), 79),
  (6, 'Saturn', 568, 120536, 687, 9, 35.5, 10.7, 10.7, 1433.5, 1352.6, 1514.5, 10747, 9.7, 2.5, 0.057, 26.7, -140, NULL, 62),
  (7, 'Uranus', 86.8, 51118, 1271, 8.7, 21.3, -17.2, 17.2, 2872.5, 2741.3, 3003.6, 30589, 6.8, 0.8, 0.046, 97.8, -195, NULL, 27),
  (8, 'Neptune', 102, 49528, 1638, 11, 23.5, 16.1, 16.1, 4495.1, 4444.5, 4545.7, 59800, 5.4, 1.8, 0.011, 28.3, -200, NULL, 14),
  (9, 'Pluto', 0.0146, 2370, 2095, 0.7, 1.3, -153.3, 153.3, 5906.4, 4436.8, 7375.9, 90560, 4.7, 17.2, 0.244, 122.5, -225, 0.00001, 5)
"""


def create_duck_db():
    """
    The DuckDB file format isn't stable, so ust create it anew each time and
    bypass the need to track versions.
    """
    import duckdb

    try:
        os.remove("planets.duckdb")
    except:
        # we expect to fail when running in GitHub Actions, but not fail
        # when running locally - just ignore failures here, it's not a
        # meaningful part of the script
        pass
    conn = duckdb.connect(database="planets.duckdb")
    cur = conn.cursor()
    cur.execute(CREATE_DB)


def test_duckdb_storage():
    create_duck_db()

    opteryx.register_store(
        "duckdb",
        SqlConnector,
        remove_prefix=True,
        connection="duckdb:///planets.duckdb",
    )

    results = opteryx.query("SELECT * FROM duckdb.planets")
    assert results.rowcount == 9, results.rowcount

    # PROCESS THE DATA IN SOME WAY
    results = opteryx.query("SELECT COUNT(*) FROM duckdb.planets;")
    assert results.rowcount == 1, results.rowcount


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
