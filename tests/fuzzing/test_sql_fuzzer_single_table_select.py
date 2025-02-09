"""
Generate random SQL statements

These are pretty basic statements but this has still found bugs.

We test virtual datasets and parquet file datasets.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime
import random
import time

import pytest
from orso.tools import random_int, random_string
from orso.types import OrsoTypes

import opteryx
from opteryx.utils.formatter import format_sql
from opteryx.connectors import IcebergConnector

def random_value(t):
    if t == OrsoTypes.VARCHAR:
        return f"'{random_string(4)}'"
    if t == OrsoTypes.BLOB:
        return f"b'{random_string(8)}'"
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
    elif where_column.type in (OrsoTypes.VARCHAR, OrsoTypes.BLOB) and random.random() < 0.5:
        where_operator = random.choice(
            ["LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE", "RLIKE", "NOT RLIKE"]
        )
        where_value = (
            random_value(where_column.type).replace("1", "%").replace("A", "%").replace("6", "_")
        )
    elif random.random() < 0.8:
        where_operator = random.choice(["==", "<>", "=", "!=", "<", "<=", ">", ">="])
        where_value = f"{str(random_value(where_column.type))}"
    else:
        return f"{where_column.name} BETWEEN {str(random_value(where_column.type))} AND {str(random_value(where_column.type))}" 
    return f"{where_column.name} {where_operator} {where_value}"



def generate_random_sql_select(columns, table):
    # Generate a list of column names to select
    column_list = list(set(random.choices(range(len(columns)), k=max(int(random.random() * len(columns)), 1))))
    column_list = [columns[i] for i in column_list]
    agg_column = None
    is_count_star = False
    # Add DISTINCT keyword with 20% chance
    if random.random() < 0.2:
        select_clause = "SELECT DISTINCT " + ", ".join(c.name for c in column_list)
    elif random.random() < 0.3:
        distinct = "DISTINCT " if random.random() < 0.1 else ""
        agg_func = random.choice(["MIN", "MAX", "SUM", "AVG", "COUNT", "COUNT_DISTINCT"])
        agg_column = columns[random.choice(range(len(columns)))]
        while agg_func in ("SUM", "AVG") and agg_column.type in (OrsoTypes.ARRAY, OrsoTypes.STRUCT, OrsoTypes.VARCHAR, OrsoTypes.BLOB, OrsoTypes.TIMESTAMP, OrsoTypes.DATE):
            agg_column = columns[random.choice(range(len(columns)))]
        while agg_func in ("MIN", "MAX", "COUNT_DISTINCT", "COUNT") and agg_column.type in (OrsoTypes.ARRAY, OrsoTypes.STRUCT):
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
    if random.random() < 0.1:
        return f"SELECT * FROM ({generate_random_sql_select(columns, table)}) as table_{random_string(4)}"
    else:
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

from opteryx import virtual_datasets
from tests.tools import set_up_iceberg

catalog = set_up_iceberg()

TABLES = [
    {
        "name": "iceberg.planets",
        "fields": IcebergConnector(dataset="iceberg.planets", statistics=None, catalog=catalog).get_dataset_schema().columns,
    },
    {
        "name": "iceberg.satellites",
        "fields": IcebergConnector(dataset="iceberg.satellites", statistics=None, catalog=catalog).get_dataset_schema().columns,
    },
    {
        "name": "iceberg.astronauts",
        "fields": IcebergConnector(dataset="iceberg.astronauts", statistics=None, catalog=catalog).get_dataset_schema().columns,
    },
    {
        "name": "iceberg.missions",
        "fields": IcebergConnector(dataset="iceberg.missions", statistics=None, catalog=catalog).get_dataset_schema().columns,
    },
    {
        "name": virtual_datasets.planets.schema().name,
        "fields": virtual_datasets.planets.schema().columns,
    },
    {
        "name": virtual_datasets.satellites.schema().name,
        "fields": virtual_datasets.satellites.schema().columns,
    },
    {
        "name": virtual_datasets.astronauts.schema().name,
        "fields": virtual_datasets.astronauts.schema().columns,
    },
    {
        "name": virtual_datasets.missions.schema().name,
        "fields": virtual_datasets.missions.schema().columns,
    },
    {
        "name": "testdata.planets",
        "fields": virtual_datasets.planets.schema().columns,
    },
    {
        "name": "testdata.satellites",
        "fields": virtual_datasets.satellites.schema().columns,
    },
    {
        "name": "testdata.missions",
        "fields": virtual_datasets.missions.schema().columns,
    },
    {
        "name": "'testdata/planets/planets.parquet'",
        "fields": virtual_datasets.planets.schema().columns,
    },
    {
        "name": "'testdata/satellites/satellites.parquet'",
        "fields": virtual_datasets.satellites.schema().columns,
    },
    {
        "name": "'testdata/missions/space_missions.parquet'",
        "fields": virtual_datasets.missions.schema().columns,
    },
]

TEST_CYCLES: int = 500


@pytest.mark.parametrize("i", range(TEST_CYCLES))
def test_sql_fuzzing_single_table(i):

    from tests.tools import set_up_iceberg
    from opteryx.connectors import IcebergConnector
    iceberg = set_up_iceberg()
    opteryx.register_store("iceberg", connector=IcebergConnector, catalog=iceberg)

    seed = random_int()
    random.seed(seed)

    table = TABLES[random.choice(range(len(TABLES)))]
    statement = generate_random_sql_select(table["fields"], table["name"])
    formatted_statement = format_sql(statement)

    print(formatted_statement)

    print(f"Seed: {seed}, Cycle: {i}, ", end="")

    start_time = time.time()  # Start timing the query execution
    try:
        res = opteryx.query(statement)
        execution_time = time.time() - start_time  # Measure execution time
        print(f"Shape: {res.shape}, Execution Time: {execution_time:.2f} seconds")
        # Additional success criteria checks can be added here
    except Exception as e:
        import traceback

        print(f"\033[0;31mError in Test Cycle {i+1}\033[0m: {e}")
        print(traceback.print_exc())
        # Log failing statement and error for analysis
        raise e
    print()

if __name__ == "__main__":  # pragma: no cover

    for i in range(TEST_CYCLES):
        test_sql_fuzzing_single_table(i)

    print("✅ okay\n")
