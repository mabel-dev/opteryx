import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import CursorInvalidStateError


def test_execute():
    cursor = opteryx.query("SELECT * FROM $planets")
    assert cursor.query == "SELECT * FROM $planets", cursor.query
    with pytest.raises(CursorInvalidStateError):
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
        3.7,
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
    dual = list(cursor.fetchmany(2))
    assert dual == [
        (
            1,
            "Mercury",
            0.33,
            4879,
            5427,
            3.7,
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
            8.9,
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


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
