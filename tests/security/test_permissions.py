"""
Test the permissions model is correctly allowing and blocking queries being executed

"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def test_security_permissions_cursor():
    """test we can stop users performing some query types"""
    conn = opteryx.connect()

    # shouldn't have any issues
    curr = conn.cursor()
    curr.execute("EXPLAIN SELECT * FROM $planets")
    curr.arrow()
    # shouldn't have any issues
    curr = conn.cursor()
    curr.execute("SELECT * FROM $planets")
    curr.arrow()

    conn = opteryx.connect(permissions={"Query"})
    # shouldn't have any issues
    curr = conn.cursor()
    curr.execute("SELECT * FROM $planets")
    curr.arrow()
    # should fail
    with pytest.raises(opteryx.exceptions.PermissionsError):
        curr = conn.cursor()
        curr.execute("EXPLAIN SELECT * FROM $planets")
        curr.arrow()


def test_security_permissions_query():
    """test we can stop users performing some query types"""
    # shouldn't have any issues
    opteryx.query("EXPLAIN SELECT * FROM $planets").arrow()
    # shouldn't have any issues
    opteryx.query("SELECT * FROM $planets").arrow()

    # shouldn't have any issues
    opteryx.query("SELECT * FROM $planets", permissions={"Query"}).arrow()
    # should fail
    with pytest.raises(opteryx.exceptions.PermissionsError):
        opteryx.query("EXPLAIN SELECT * FROM $planets", permissions={"Query"}).arrow()


def test_security_permissions_validation():
    # shouldn't have any issues
    opteryx.query("SELECT * FROM $planets", permissions=opteryx.constants.PERMISSIONS).arrow()
    opteryx.query("SELECT * FROM $planets", permissions=None).arrow()
    opteryx.query("SELECT * FROM $planets", permissions={"Analyze", "Execute", "Query"}).arrow()
    opteryx.query("SELECT * FROM $planets", permissions=["Analyze", "Execute", "Query"]).arrow()
    opteryx.query("SELECT * FROM $planets", permissions=("Analyze", "Execute", "Query")).arrow()
    # should fail
    with pytest.raises(opteryx.exceptions.ProgrammingError):
        # invalid permission
        opteryx.query("SELECT * FROM $planets", permissions={"Select"}).arrow()
    with pytest.raises(opteryx.exceptions.ProgrammingError):
        # no permissions
        opteryx.query("SELECT * FROM $planets", permissions={}).arrow()
    with pytest.raises(opteryx.exceptions.ProgrammingError):
        # invalid permission
        opteryx.query("SELECT * FROM $planets", permissions={"Query", "Select"}).arrow()


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
