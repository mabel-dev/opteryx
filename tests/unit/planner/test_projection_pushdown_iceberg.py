import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import IcebergConnector
from opteryx.utils.formatter import format_sql
from tests import set_up_iceberg


STATEMENTS = [
    # Simple column selection cases
    ("SELECT * FROM iceberg.opteryx.planets;", 20),
    ("SELECT name FROM iceberg.opteryx.planets;", 1),
    ("SELECT id, name FROM iceberg.opteryx.planets;", 2),
    ("SELECT id, name, mass FROM iceberg.opteryx.planets;", 3),
    ("SELECT gravity, escapeVelocity, rotationPeriod FROM iceberg.opteryx.planets;", 3),
    ("SELECT distanceFromSun, perihelion, aphelion FROM iceberg.opteryx.planets;", 3),
    ("SELECT orbitalPeriod, orbitalVelocity, orbitalInclination, orbitalEccentricity FROM iceberg.opteryx.planets;", 4),
    ("SELECT obliquityToOrbit, meanTemperature, surfacePressure FROM iceberg.opteryx.planets;", 3),
    ("SELECT numberOfMoons, name FROM iceberg.opteryx.planets;", 2),

    # Function applied to column selections
    ("SELECT UPPER(name) FROM iceberg.opteryx.planets;", 1),
    ("SELECT LENGTH(name), gravity FROM iceberg.opteryx.planets;", 2),

    # WHERE clause filters with projections
    ("SELECT name FROM iceberg.opteryx.planets WHERE IFNULL(gravity, 1.0) > 9.8;", 2),  # we can't push this filter
    ("SELECT id, mass FROM iceberg.opteryx.planets WHERE density < 5;", 2),  # we can push this filter
    ("SELECT escapeVelocity FROM iceberg.opteryx.planets WHERE name LIKE 'M%';", 2),

    # DISTINCT column selection
    ("SELECT DISTINCT name FROM iceberg.opteryx.planets;", 1),
    ("SELECT DISTINCT id, name FROM iceberg.opteryx.planets;", 2),

    # Projection pushdown with GROUP BY
    ("SELECT name, COUNT(*) FROM iceberg.opteryx.planets GROUP BY name;", 1),
    ("SELECT gravity, AVG(mass) FROM iceberg.opteryx.planets GROUP BY gravity;", 2),

    # ORDER BY clauses should not prevent projection pushdown
    ("SELECT name FROM iceberg.opteryx.planets ORDER BY gravity;", 2),
    ("SELECT id FROM iceberg.opteryx.planets ORDER BY name, mass;", 3),

    # CASE statements in the SELECT clause
    ("SELECT CASE WHEN gravity > 9.8 THEN 'High' ELSE 'Low' END FROM iceberg.opteryx.planets;", 1),
    ("SELECT name, CASE WHEN mass > 5 THEN 'Massive' ELSE 'Light' END FROM iceberg.opteryx.planets;", 2),

    # Queries involving mathematical expressions
    ("SELECT DOUBLE(gravity) * 9.81 FROM iceberg.opteryx.planets;", 1),
    ("SELECT mass / density FROM iceberg.opteryx.planets;", 2),

    # Column aliasing in the main query
    ("SELECT name AS planet_name FROM iceberg.opteryx.planets;", 1),
    ("SELECT id AS planet_id, mass AS planet_mass FROM iceberg.opteryx.planets;", 2),

    # Limit queries to verify projection pushdown still occurs
    ("SELECT name FROM iceberg.opteryx.planets LIMIT 10;", 1),
    ("SELECT id, escapeVelocity FROM iceberg.opteryx.planets LIMIT 5;", 2),

    # Aggregation cases
    ("SELECT MAX(gravity), MIN(name) FROM iceberg.opteryx.planets;", 2),
    ("SELECT SUM(diameter), AVG(density) FROM iceberg.opteryx.planets;", 2),
    ("SELECT COUNT(*), MAX(numberOfMoons) FROM iceberg.opteryx.planets;", 1),

    # Pushing past subqueries 
    ("SELECT * FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches", 3),
    ("SELECT DISTINCT Mission FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches", 1),
    ("SELECT LL FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches", 1),

    # Basic projection pushdown cases
    ("SELECT Company FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),
    ("SELECT Mission FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),
    ("SELECT LL FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),
    ("SELECT * FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 3),

    # Projection pushdown with DISTINCT and ORDER BY
    ("SELECT DISTINCT Company FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),
    ("SELECT DISTINCT Company FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches ORDER BY Company;", 1),
    ("SELECT DISTINCT Mission FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),

    # Testing functions on the projected columns
    ("SELECT LOG2(LL) FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),
    ("SELECT LENGTH(Company) > LL FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 2),
    ("SELECT LENGTH(Mission) FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),

    # Test with WHERE clause that filters using different columns
    ("SELECT LL FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches WHERE LENGTH(Company) < LL;", 2),
    ("SELECT Mission FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches WHERE Company = 'SpaceX';", 1),

    # Combining DISTINCT with functions and subqueries
    ("SELECT DISTINCT LENGTH(Company) FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),

    # Projection with multiple levels of subqueries
    ("SELECT Company FROM (SELECT * FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS inner_query) AS outer_query;", 1),
    ("SELECT LL FROM (SELECT * FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS inner_query) AS outer_query;", 1),

    # Testing aggregation functions
    ("SELECT MAX(LL) FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),
    ("SELECT COUNT(*), MAX(LL) FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),
    ("SELECT AVG(LL) FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches;", 1),

    # Case with ORDER BY clause but only selected columns should be read
    ("SELECT LL FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches ORDER BY Mission;", 2),
    ("SELECT Mission FROM (SELECT Company, Mission, LENGTH(Location) AS LL FROM iceberg.opteryx.missions) AS launches ORDER BY LL;", 2),

]

@pytest.mark.parametrize("query, expected_columns", STATEMENTS)
def test_parquet_projection_pushdown(query, expected_columns):

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    cur = opteryx.query(query)
    cur.materialize()
    assert cur.stats.get("columns_read") == expected_columns, cur.stats



if __name__ == "__main__":  # pragma: no cover
    import shutil
    import time

    from tests import trunc_printable
    from opteryx.utils.formatter import format_sql

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} TESTS")
    for index, (statement, read_columns) in enumerate(STATEMENTS):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(statement), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_parquet_projection_pushdown(statement, read_columns)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(" \033[0;31m*\033[0m")
            else:
                print()
        except Exception as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ *\033[0m")
            print(">", err)
            failed += 1

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
