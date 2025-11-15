#!/usr/bin/env python3
"""
DBAPI Quickstart Example

This script demonstrates how to use ``opteryx.connect()`` and cursors in a minimal
workflow:
    * establish a connection
    * run SELECT statements
    * fetch results using DBAPI cursor methods
    * register in-memory Arrow tables and query them like regular datasets

Run it directly:
    python examples/basic/dbapi_quickstart.py
"""

from __future__ import annotations

import pyarrow as pa

import opteryx

PLANETS_DATASET = "testdata/planets/planets.parquet"


def register_sample_table() -> None:
    """Register an in-memory Arrow table so we can query it via SQL."""

    crew_manifest = pa.Table.from_pylist(
        [
            {"mission": "Cassini", "astronauts": 3, "destination": "Saturn"},
            {"mission": "Voyager", "astronauts": 0, "destination": "Interstellar"},
            {"mission": "Apollo 11", "astronauts": 3, "destination": "Moon"},
        ]
    )
    opteryx.register_arrow("crew_manifest", crew_manifest)


def select_top_planets(cursor) -> None:
    """Query a Parquet file directly using DBAPI-style fetch helpers."""

    cursor.execute(
        f"""
        SELECT name, gravity, distanceFromSun
        FROM '{PLANETS_DATASET}'
        ORDER BY gravity DESC
        LIMIT 3;
        """
    )
    print("Top three planets by gravity:")
    for row in cursor.fetchall():
        row_map = dict(row.as_map)
        print(
            f"  {row_map['name']}: gravity={row_map['gravity']}, distance={row_map['distanceFromSun']}"
        )
    print()


def select_registered_table(cursor) -> None:
    """Show how to query the registered Arrow table as if it were persistent data."""

    cursor.execute(
        """
        SELECT mission, destination, astronauts
        FROM crew_manifest
        ORDER BY mission;
        """
    )
    print("Crew manifest (registered Arrow table):")
    for row in cursor.fetchall():
        row_map = dict(row.as_map)
        print(
            f"  {row_map['mission']} -> {row_map['destination']} ({row_map['astronauts']} astronauts)"
        )
    print()


def pandas_example() -> None:
    """Demonstrate using the built-in pandas converter on a query result."""

    planets_df = opteryx.query(
        """
        SELECT name, gravity, distanceFromSun
        FROM $planets
        ORDER BY gravity DESC
        LIMIT 5
        """
    ).pandas()

    print("Planets DataFrame powered by cursor.pandas():")
    print(planets_df)
    print()


def main() -> None:
    register_sample_table()

    with opteryx.connect(user="example_user") as connection, connection.cursor() as cursor:
        select_top_planets(cursor)
        select_registered_table(cursor)
        print("Cursor description metadata:")
        for column in cursor.description or []:
            print(f"  {column[0]} (type={column[1]})")

    pandas_example()


if __name__ == "__main__":
    main()
