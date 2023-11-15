import os
import sys
from decimal import Decimal
import pytest
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import InvalidCursorStateError, MissingSqlStatement, UnsupportedSyntaxError
from opteryx.cursor import CursorState


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
    with pytest.raises(InvalidCursorStateError):
        cursor.execute("SELECT * FROM $planets")


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
    assert cursor._state == CursorState.INITIALIZED


def test_execute_transition_state():
    cursor = setup_function()
    cursor.execute("SELECT * FROM $planets")
    assert cursor._state == CursorState.EXECUTED


def test_execute_with_invalid_state():
    cursor = setup_function()
    cursor._state = CursorState.EXECUTED  # Manually setting to test
    with pytest.raises(InvalidCursorStateError):
        cursor.execute("SELECT * FROM $planets")


def test_close_with_invalid_state():
    cursor = setup_function()
    with pytest.raises(InvalidCursorStateError):
        cursor.close()


def test_execute_to_arrow_with_invalid_state():
    cursor = setup_function()
    cursor.execute_to_arrow("SELECT * FROM $planets")
    with pytest.raises(InvalidCursorStateError):
        cursor.execute_to_arrow("SELECT * FROM $planets")


def test_execute_to_arrow():
    cursor = setup_function()
    results = cursor.execute_to_arrow("SELECT * FROM $planets")
    assert results.shape == (9, 20)
    assert isinstance(results, pyarrow.Table)


def test_query_to_arrow():
    results = opteryx.query_to_arrow("SELECT * FROM $planets")
    assert results.shape == (9, 20)
    assert isinstance(results, pyarrow.Table)


def test_execute_missing_sql_statement():
    cursor = setup_function()
    with pytest.raises(MissingSqlStatement):
        cursor.execute("")


def test_execute_unsupported_syntax_error():
    cursor = setup_function()
    with pytest.raises(UnsupportedSyntaxError):
        cursor.execute("SELECT * FROM table; SELECT * FROM table2", params=[1])


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
