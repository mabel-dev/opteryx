"""
Generate random SQL JOINs

These are pretty basic joins but this approach still finds bugs.
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

from opteryx.connectors import FileConnector, IcebergConnector
from tests.tools import set_up_iceberg

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


def generate_condition(table, columns):
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
        return f"{table}.{where_column.name} BETWEEN {str(random_value(where_column.type))} AND {str(random_value(where_column.type))}" 
    return f"{table}.{where_column.name} {where_operator} {where_value}"

def generate_random_sql_join(columns1, table1, columns2, table2) -> str:
    join_type = random.choice(["JOIN", "INNER JOIN", "LEFT JOIN", "LEFT OUTER JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "LEFT ANTI JOIN", "LEFT SEMI JOIN"])

    last_value = -1
    this_value = random.random()
    conditions = []
    # we add multiple conditions by cycling over ever increasing random values until we get a lower one
    while this_value > last_value:
        last_value = this_value
        this_value = random.random()

        left_column = columns1[random.choice(range(len(columns1)))]
        right_column = columns2[random.choice(range(len(columns2)))]
        while left_column.type != right_column.type or left_column.type in (OrsoTypes.ARRAY, OrsoTypes.STRUCT):
            left_column = columns1[random.choice(range(len(columns1)))]
            right_column = columns2[random.choice(range(len(columns2)))]

        condition = f"{table1}.{left_column.name} = {table2}.{right_column.name}"
        conditions.append(condition)

    join_condition = " AND ".join(conditions)

    if join_type in ("LEFT ANTI JOIN", "LEFT SEMI JOIN"):
        selected_columns = [f"{table1}.{col.name}" for col in columns1 if random.random() < 0.2]
    else:
        selected_columns = [f"{table1}.{col.name}" for col in columns1 if random.random() < 0.2] + [f"{table2}.{col.name}" for col in columns2 if random.random() < 0.2]
    if len(selected_columns) == 0:
        selected_columns = ["*"]
    select_clause = "SELECT " + ", ".join(selected_columns)
    
    where_clause = "--"
    # Generate a WHERE clause with 70% chance
    if random.random() < 0.3:
        if where_clause == "--":
            where_clause = " WHERE "
        where_clause += generate_condition(table1, columns1)
        # add an abitrary number of additional conditions
        while random.random() < 0.1:
            linking_condition = random.choice(["AND", "OR", "AND NOT"])
            where_clause += f" {linking_condition} {generate_condition(table1, columns1)}"
    
    if join_type not in ("LEFT ANTI JOIN", "LEFT SEMI JOIN") and random.random() < 0.3:
        if where_clause == "--":
            where_clause = " WHERE "
        else:
            where_clause += f' {random.choice(["AND", "OR", "AND NOT"])} '
        where_clause += generate_condition(table2, columns2)
        # add an abitrary number of additional conditions
        while random.random() < 0.1:
            linking_condition = random.choice(["AND", "OR", "AND NOT"])
            where_clause += f" {linking_condition} {generate_condition(table2, columns2)}"

    query = f"{select_clause} FROM {table1} {join_type} {table2} ON {join_condition} {where_clause}"
    
    return query

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
        "fields": FileConnector(dataset="testdata/planets/planets.parquet", statistics=None).get_dataset_schema().columns,
    },
    {
        "name": "testdata.satellites",
        "fields": FileConnector(dataset="testdata/satellites/satellites.parquet", statistics=None).get_dataset_schema().columns,
    },
    {
        "name": "testdata.missions",
        "fields": FileConnector(dataset="testdata/missions/space_missions.parquet", statistics=None).get_dataset_schema().columns,
    },
]

TEST_CYCLES: int = 250


@pytest.mark.parametrize("i", range(TEST_CYCLES))
def test_sql_fuzzing_join(i):
    seed = random_int()
    random.seed(seed)

    from tests.tools import set_up_iceberg
    from opteryx.connectors import IcebergConnector
    iceberg = set_up_iceberg()
    opteryx.register_store("iceberg", connector=IcebergConnector, catalog=iceberg)

    table1 = TABLES[random.choice(range(len(TABLES)))]
    table2 = TABLES[random.choice(range(len(TABLES)))]
    while table1 == table2:
        table2 = TABLES[random.choice(range(len(TABLES)))]
    statement = generate_random_sql_join(table1["fields"], table1["name"], table2["fields"], table2["name"])
    formatted_statement = format_sql(statement)

    print(formatted_statement)

    start_time = time.time()  # Start timing the query execution
    try:
        res = opteryx.query(statement)
        execution_time = time.time() - start_time  # Measure execution time
        print(f"Shape: {res.shape}, Execution Time: {execution_time:.2f} seconds, Seed: {seed} ({i})")
        # Additional success criteria checks can be added here
    except Exception as e:
        import traceback

        print(f"\033[0;31mError in Test Cycle {i+1} Seed: {seed} \033[0m: {e}")
        print(traceback.print_exc())
        # Log failing statement and error for analysis
        raise e
    print()


if __name__ == "__main__":  # pragma: no cover

    for i in range(TEST_CYCLES):
        test_sql_fuzzing_join(i)

    print("✅ okay")
