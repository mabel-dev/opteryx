import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from orso.types import OrsoTypes

from opteryx.managers.expression import format_expression
from opteryx.managers.expression import NodeType
from opteryx.models.node import Node

from tests.tools import create_duck_db
import duckdb
import opteryx


def test_format_nodes():
    import duckdb

    create_duck_db()

    SQL = "SELECT 1+1*1"

    result = duckdb.query(SQL)

    print(result)

    result = opteryx.query(SQL)
    print(result)


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
