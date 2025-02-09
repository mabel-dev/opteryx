"""
This fuzzer tests connectors perform the same by issuing the same SQL statement
to multiple connectors and comparing the results.

The test generates random SQL SELECT statements to test various aspects of the
engine such as predicate and projection pushdowns.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime
import random

import pytest
from orso.tools import random_int, random_string
from orso.types import OrsoTypes

import opteryx
from opteryx.utils.formatter import format_sql
from opteryx import virtual_datasets

from tests.tools import create_duck_db
from tests.tools import is_arm, is_mac, is_windows, skip_if, is_version

TEST_CYCLES: int = 100


TABLES = {
    "planets": {
        "opteryx_name": virtual_datasets.planets.schema().name,
        "duckdb_name": "planets",
        "fields": virtual_datasets.planets.schema().columns,
    },
    "satellites": {
        "opteryx_name": virtual_datasets.satellites.schema().name,
        "duckdb_name": "satellites",
        "fields": virtual_datasets.satellites.schema().columns,
    },
}


def random_value(t):
    if t == OrsoTypes.VARCHAR:
        return f"'{random_string(4)}'"
    if t in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP):
        if random.random() < 0.5:
            return f"'{datetime.datetime.now() + datetime.timedelta(seconds=random_int())}'"
        return f"'{(datetime.datetime.now() + datetime.timedelta(seconds=random_int())).date()}'"
    if random.random() < 0.5:
        return random_int()
    return random_int() / 1000


def generate_condition(columns):
    where_column = columns[random.choice(range(len(columns)))]
    while where_column.type in (OrsoTypes.ARRAY, OrsoTypes.STRUCT):
        where_column = columns[random.choice(range(len(columns)))]
    if random.random() < 0.1:
        where_operator = random.choice(["IS", "IS NOT"])
        if where_column.type == OrsoTypes.BOOLEAN:
            where_value = random.choice(["TRUE", "FALSE", "NULL"])
        else:
            where_value = "NULL"
    elif where_column.type == OrsoTypes.VARCHAR and random.random() < 0.5:
        where_operator = random.choice(
            ["LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE"]
        )
        where_value = (
            "'" + random_string(8).replace("1", "%").replace("A", "%").replace("6", "_") + "'"
        )
    elif random.random() < 0.8:
        where_operator = random.choice(["==", "<>", "=", "!=", "<", "<=", ">", ">="])
        where_value = f"{str(random_value(where_column.type))}"
    else:
        return f"{where_column.name} BETWEEN {str(random_value(where_column.type))} AND {str(random_value(where_column.type))}"
    return f"{where_column.name} {where_operator} {where_value}"


def generate_random_sql_select(columns, table):
    # Generate a list of column names to select
    column_list = list(
        set(random.choices(range(len(columns)), k=max(int(random.random() * len(columns)), 1)))
    )
    column_list = [columns[i] for i in column_list]
    agg_column = None
    is_count_star = False
    # Add DISTINCT keyword with 20% chance
#    if random.random() < 0.2:
#        select_clause = "SELECT DISTINCT " + ", ".join(c.name for c in column_list)
    if random.random() < 0.3:
        distinct = "DISTINCT " if random.random() < 0.1 else ""
        agg_func = random.choice(["MIN", "MAX", "SUM", "AVG", "COUNT"])
        agg_column = columns[random.choice(range(len(columns)))]
        while agg_func in ("SUM", "AVG") and agg_column.type in (
            OrsoTypes.ARRAY,
            OrsoTypes.STRUCT,
            OrsoTypes.VARCHAR,
            OrsoTypes.TIMESTAMP,
            OrsoTypes.DATE,
        ):
            agg_column = columns[random.choice(range(len(columns)))]
        while agg_func in ("MIN", "MAX", "COUNT") and agg_column.type in (
            OrsoTypes.ARRAY,
            OrsoTypes.STRUCT,
        ):
            agg_column = columns[random.choice(range(len(columns)))]
        select_clause = "SELECT " + distinct + agg_func + "(" + agg_column.name + ")"

        column_list = [c for c in column_list if c.type not in (OrsoTypes.ARRAY, OrsoTypes.STRUCT)]
    elif random.random() < 0.8:
        select_clause = "SELECT " + ", ".join(c.name for c in column_list)
    elif random.random() < 0.5:
        select_clause = "SELECT COUNT(*) "
        is_count_star = True
    else:
        select_clause = "SELECT *"
    # Add table name
    select_clause = select_clause + " FROM " + table
    # Generate a WHERE clause with 70% chance
    if random.random() < 0.7:
        where_clause = generate_condition(columns)
        # add an abitrary number of additional conditions
        while random.random() < 0.3:
            linking_condition = random.choice(["AND", "OR", "AND NOT"])
            where_clause += f" {linking_condition} {generate_condition(columns)} "
        select_clause = f"{select_clause} WHERE {where_clause}"
    # Add GROUP BY clause with 40% chance
    if agg_column and random.random() < 0.4:
        column_list = [c.name for c in column_list]
        select_clause = select_clause + " GROUP BY " + ", ".join(column_list + [agg_column.name])
    # Add ORDER BY clause with 60% chance
    if not agg_column and not is_count_star and random.random() < 0.6:
        order_column = columns[random.choice(range(len(columns)))]
        if order_column.type not in (OrsoTypes.ARRAY, OrsoTypes.STRUCT):
            order_direction = random.choice(["ASC", "DESC", ""])
            select_clause = select_clause + " ORDER BY " + order_column.name + " " + order_direction
    if random.random() < 0.2:
        select_clause = select_clause + " LIMIT " + str(int(random.random() * 10))
    return select_clause


@skip_if(is_arm() or is_windows() or is_mac() or not is_version("3.10"))
@pytest.mark.parametrize("i", range(TEST_CYCLES))
def test_sql_fuzzing_connector_comparisons(i):

    create_duck_db()

    import duckdb
    conn = duckdb.connect(database="planets.duckdb")

    seed = random_int()
    random.seed(seed)

    table_name = random.choice(list(TABLES.keys()))
    table = TABLES[table_name]
    statement = generate_random_sql_select(table["fields"], table_name)
    formatted_statement = format_sql(statement)

    print(formatted_statement)

    print(f"Seed: {seed}, Cycle: {i}")

    try:
        duck_statement = statement.replace(table_name, table["duckdb_name"])
        duck_result = conn.query(duck_statement).arrow()
        opteryx_statement = statement.replace(table_name, table["opteryx_name"])
        opteryx_result = opteryx.query(opteryx_statement).arrow()

        assert duck_result.shape[0] == opteryx_result.shape[0]
        assert duck_result.shape[1] == opteryx_result.shape[1]

    except Exception as e:
        import traceback

        print(
            f"\033[0;31mError in Test Cycle {i + 1} \033[0m: {type(e)}"
        )
        print("Duck", duck_result.shape)
        print("Opteryx", opteryx_result.shape)

        print(traceback.print_exc())
        # Log failing statement and error for analysis
        assert False, (
            f"Error in Test Cycle {i + 1}: {type(e)} \n {format_sql(statement)}"
        )
    print()


if __name__ == "__main__":  # pragma: no cover
    for i in range(TEST_CYCLES):
        test_sql_fuzzing_connector_comparisons(i)

    print("✅ okay\n")
