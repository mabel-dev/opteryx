import os
import sys
from decimal import Decimal

import pyarrow
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import InvalidCursorStateError, MissingSqlStatement, UnsupportedSyntaxError
from opteryx.constants import ResultType


def setup_function():
    # Setup for each test, create a new connection for example
    conn = opteryx.Connection()
    cursor = conn.cursor()
    return cursor


def test_execute():
    conn = opteryx.Connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM $planets")
    assert conn.history[-1][0] == "SELECT * FROM $planets", conn.history
    # cursor can now be re-used for additional queries
    cursor.execute("SELECT name FROM $planets LIMIT 1")
    result = cursor.fetchone()
    assert result[0] == "Mercury"


def test_rowcount():
    cursor = opteryx.query("SELECT * FROM $planets")
    assert cursor.rowcount == 9


def test_shape():
    cursor = opteryx.query("SELECT * FROM $planets")
    assert cursor.shape == (9, 20), cursor.shape


def test_fetchone():
    cursor = opteryx.query("SELECT * FROM $planets")
    one = cursor.fetchone()
    assert one == (
        1,
        "Mercury",
        0.33,
        4879,
        5427,
        Decimal("3.7"),
        4.3,
        1407.6,
        4222.6,
        57.9,
        46.0,
        69.8,
        88.0,
        47.4,
        7.0,
        0.205,
        0.03,
        167,
        0.0,
        0,
    ), one


def test_fetchmany():
    cursor = opteryx.query("SELECT * FROM $planets")
    dual = cursor.fetchmany(2)
    assert dual == [
        (
            1,
            "Mercury",
            0.33,
            4879,
            5427,
            Decimal("3.7"),
            4.3,
            1407.6,
            4222.6,
            57.9,
            46.0,
            69.8,
            88.0,
            47.4,
            7.0,
            0.205,
            0.03,
            167,
            0.0,
            0,
        ),
        (
            2,
            "Venus",
            4.87,
            12104,
            5243,
            Decimal("8.9"),
            10.4,
            -5832.5,
            2802.0,
            108.2,
            107.5,
            108.9,
            224.7,
            35.0,
            3.4,
            0.007,
            177.4,
            464,
            92.0,
            0,
        ),
    ], dual


def test_fetchall():
    cursor = opteryx.query("SELECT * FROM $planets")
    all_rows = cursor.fetchall()
    assert len(all_rows) == 9, len(all_rows)


def test_execute_error():
    # Test that an error is raised when executing an invalid SQL statement
    conn = opteryx.Connection()
    cursor = conn.cursor()
    with pytest.raises(Exception):
        cursor.execute("SELECT * FROM non_existent_table")


def test_cursor_init():
    cursor = setup_function()
    assert not cursor  # __bool__ should be False before execution


def test_execute_to_arrow():
    cursor = setup_function()
    results = cursor.execute_to_arrow("SELECT * FROM $planets")
    assert results.shape == (9, 20)
    assert isinstance(results, pyarrow.Table)


def test_query_to_arrow():
    results = opteryx.query_to_arrow("SELECT * FROM $planets")
    assert results.shape == (9, 20)
    assert isinstance(results, pyarrow.Table)


def test_execute_to_arrow_batches():
    cursor = setup_function()
    batches = list(cursor.execute_to_arrow_batches("SELECT * FROM $planets", batch_size=3))
    # Ensure we received record batches
    assert all(isinstance(b, pyarrow.RecordBatch) for b in batches)
    # Verify total rows match
    assert sum(b.num_rows for b in batches) == 9


def test_execute_to_arrow_batches_limit():
    cursor = setup_function()
    batches = list(cursor.execute_to_arrow_batches("SELECT * FROM $planets", batch_size=2, limit=3))
    assert sum(b.num_rows for b in batches) == 3


def test_query_to_arrow_batches():
    batches = list(opteryx.query_to_arrow_batches("SELECT * FROM $planets", batch_size=4))
    assert all(isinstance(b, pyarrow.RecordBatch) for b in batches)


def test_execute_to_arrow_batches_consolidate():
    cursor = setup_function()
    # create two morsels 50 and 100 rows
    t1 = pyarrow.Table.from_pydict({"a": [1] * 50})
    t2 = pyarrow.Table.from_pydict({"a": [2] * 100})

    def fake_execute_statements(operation, params, visibility_filters):
        return (iter([t1, t2]), ResultType.TABULAR)

    # patch the cursor's execute statements to return our fake morsels
    cursor._execute_statements = fake_execute_statements

    # target batch 150: should emit a single batch of 150
    batches = list(cursor.execute_to_arrow_batches("SELECT fakes", batch_size=150))
    assert len(batches) == 1
    assert batches[0].num_rows == 150

    # target batch 100: should emit one batch of 100 and a second of 50
    cursor = setup_function()
    cursor._execute_statements = fake_execute_statements
    batches = list(cursor.execute_to_arrow_batches("SELECT fakes", batch_size=100))
    assert [b.num_rows for b in batches] == [100, 50]


def test_execute_to_arrow_batches_sets_description():
    cursor = setup_function()
    batches = cursor.execute_to_arrow_batches("SELECT * FROM $planets", batch_size=3)
    next(batches)
    assert cursor.description is not None


def test_execute_missing_sql_statement():
    cursor = setup_function()
    with pytest.raises(MissingSqlStatement):
        cursor.execute("")


def test_execute_unsupported_syntax_error():
    cursor = setup_function()
    with pytest.raises(UnsupportedSyntaxError):
        cursor.execute("SELECT * FROM table; SELECT * FROM table2", params=[1])

def test_non_tabular_result():
    cursor = setup_function()
    cursor.execute("SET @name = 'tim'")
    cursor.fetchall()

def test_limit():
    cursor = setup_function()
    dataset = cursor.execute_to_arrow("SELECT * FROM $planets", limit=3)
    assert dataset.num_rows == 3


def test_cursor_close_blocks_further_commands():
    cursor = setup_function()
    cursor.close()
    with pytest.raises(InvalidCursorStateError):
        cursor.execute("SELECT * FROM $planets")


def test_execute_to_arrow_can_repeat():
    cursor = setup_function()
    result_first = cursor.execute_to_arrow("SELECT * FROM $planets")
    assert result_first.shape == (9, 20)
    result_second = cursor.execute_to_arrow("SELECT name FROM $planets LIMIT 2")
    assert result_second.num_rows == 2


def test_cursor_truthiness_after_close():
    cursor = setup_function()
    cursor.execute("SELECT * FROM $planets")
    assert cursor
    cursor.close()
    assert not cursor

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
