#!/usr/bin/env python3
"""
DBAPI + Pandas Example

Showcase how to combine ``opteryx.connect()`` with familiar analytics tools.
Steps:
    1. Run a SQL query through the cursor
    2. Convert the result set to a Pandas DataFrame via ``cursor.pandas()``
    3. Perform further analysis using Pandas

Run it directly:
    python examples/advanced/dbapi_pandas_integration.py
"""

from __future__ import annotations

import pandas as pd

import opteryx


def main() -> None:
    with opteryx.connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                name,
                gravity,
                distanceFromSun,
                atmosphere
            FROM $planets
            WHERE gravity > 5
            ORDER BY gravity DESC
            LIMIT 5;
            """
        )

        df: pd.DataFrame = cursor.pandas()
        print("Planets with gravity > 5 m/s²:")
        print(df)
        print()

        print("Aggregate by atmosphere composition count:")
        print(df.groupby("atmosphere")["name"].count())


if __name__ == "__main__":
    main()
