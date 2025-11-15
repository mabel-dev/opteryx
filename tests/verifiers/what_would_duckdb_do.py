
import os
import sys
import duckdb
from orso import DataFrame

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
sys.path.insert(1, os.path.join(sys.path[0], ".."))

import opteryx
from opteryx.utils.formatter import format_sql
from tests import create_duck_db


TABLES = ("planets","satellites")


def wwdd(statement: str):

    create_duck_db()
    conn = duckdb.connect(database="tmp/planets-gw0.duckdb")

    formatted_statement = format_sql(statement)

    print(formatted_statement)

    opteryx_statement = statement

    for table_name in TABLES:
        opteryx_statement = opteryx_statement.replace(table_name, f"${table_name}")

    duck_result = DataFrame.from_arrow(conn.query(statement).arrow())
    opteryx_result = opteryx.query(opteryx_statement)

    print("Duck")
    print(duck_result)
    print("Opteryx")
    print(opteryx_result)

    print()


wwdd("SELECT AVG(mass), COUNT(*), name FROM (SELECT name, mass FROM planets GROUP BY name, mass) AS A group by name")