import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def test_question_mark():
    res = opteryx.query("SELECT * FROM $planets WHERE id = ?", params=[1])
    assert res.shape == (1, 20)

    res = opteryx.query("SELECT * FROM $planets WHERE id = ? or name = ?", params=[1, "Earth"])
    assert res.shape == (2, 20)

    res = opteryx.query(
        "SELECT * FROM (SELECT * FROM $planets WHERE id = ? or name = ?) AS sub",
        params=[1, "Earth"],
    )
    assert res.shape == (2, 20)


def test_named_parameter():
    res = opteryx.query("SELECT * FROM $planets WHERE id = :pid", params={"pid": 1})
    assert res.shape == (1, 20)

    res = opteryx.query(
        "SELECT * FROM $planets WHERE id = :pid or name = :name", params={"pid": 1, "name": "Earth"}
    )
    assert res.shape == (2, 20)

    # if we've given named params, provide a dict
    with pytest.raises(opteryx.exceptions.ParameterError):
        res = opteryx.query(
            "SELECT * FROM (SELECT * FROM $planets WHERE id = :pid or name = :name) AS sub",
            params=[1, "Earth"],
        )

    # we can't use param lists with batched queries
    with pytest.raises(opteryx.exceptions.UnsupportedSyntaxError):
        res = opteryx.query(
            "SET @apple = ?; SELECT * FROM $planets WHERE id = ? or name = :name",
            params=[1, "Earth"],
        )

    # we can used named parameters though
    res = opteryx.query(
        "SET @apple = :apple; SELECT * FROM $planets WHERE id = :apple or name = :name",
        params={"apple": 1, "name": "Earth"},
    )
    assert res.shape == (2, 20)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
