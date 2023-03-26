"""
Generate random SQL statements

These are pretty basic statements but this has still found bugs.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import random

import opteryx
from opteryx.utils.formatter import format_sql


def generate_random_sql_select(columns, table):
    # Generate a list of column names to select
    column_list = random.choices(columns, k=random.randint(1, len(columns)))
    agg_column = None
    # Add DISTINCT keyword with 20% chance
    if random.random() < 0.2:
        select_clause = "SELECT DISTINCT " + ", ".join(column_list)
    elif random.random() < 0.3:
        agg_func = random.choice(["AVG", "SUM", "MIN", "MAX"])
        agg_column = random.choice(columns[2:])
        select_clause = "SELECT " + agg_func + "(" + agg_column + ")"
    else:
        select_clause = "SELECT " + ", ".join(column_list)
    # Add table name
    select_clause = select_clause + " FROM " + table
    # Generate a WHERE clause with 70% chance
    if random.random() < 0.7:
        where_column = random.choice(columns[2:])
        where_operator = random.choice(["=", "!=", "<", "<=", ">", ">="])
        where_value = str(random.randint(1, 100))
        where_clause = f"{where_column} {where_operator} {where_value}"
        select_clause = f"{select_clause} WHERE {where_clause}"
        # add an abitrary number of additional conditions
        while random.random() < 0.3:
            linking_condition = random.choice(["AND", "OR", "AND NOT"])
            where_column = random.choice(columns[2:])
            where_operator = random.choice(["=", "!=", "<", "<=", ">", ">="])
            where_value = str(random.randint(1, 100))
            where_clause = f"{where_column} {where_operator} {where_value}"
            select_clause = f"{select_clause} {linking_condition} {where_clause}"
    # Add GROUP BY clause with 40% chance
    if agg_column and random.random() < 0.4:
        select_clause = select_clause + " GROUP BY " + ", ".join(column_list + [agg_column])
    # Add ORDER BY clause with 60% chance
    if not agg_column and random.random() < 0.6:
        order_column = random.choice(column_list)
        order_direction = random.choice(["ASC", "DESC"])
        select_clause = select_clause + " ORDER BY " + order_column + " " + order_direction

    return select_clause


TABLES = [
    {
        "name": "$planets",
        "fields": [
            "id",
            "name",
            "mass",
            "diameter",
            "density",
            "gravity",
            "escapeVelocity",
            "rotationPeriod",
            "lengthOfDay",
            "distanceFromSun",
            "perihelion",
            "aphelion",
            "orbitalPeriod",
            "orbitalVelocity",
            "orbitalInclination",
            "orbitalEccentricity",
            "obliquityToOrbit",
            "meanTemperature",
            "surfacePressure",
            "numberOfMoons",
        ],
    },
    {
        "name": "$satellites",
        "fields": ["id", "name", "planetId", "gm", "radius", "density", "magnitude", "albedo"],
    },
]

TEST_CYCLES: int = 250


def test_sql_fuzzing():
    for i in range(TEST_CYCLES):
        table = TABLES[random.choice(range(len(TABLES)))]
        statement = generate_random_sql_select(table["fields"], table["name"])
        print(format_sql(statement))
        res = opteryx.query(statement)
        try:
            print(res.shape)
        except Exception as e:
            print()
            print(e)
            raise e
    print("")


if __name__ == "__main__":  # pragma: no cover
    test_sql_fuzzing()

    print("✅ okay")
