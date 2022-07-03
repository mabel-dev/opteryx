"""
Tests different no data scenarios
"""
import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

from opteryx.exceptions import DatabaseError
from opteryx.storage.adapters import DiskStorage

# fmt:off
STATEMENTS = [
    # virtual dataset doesn't exist
    ("SELECT * FROM $RomanGods", DatabaseError),
    # disk dataset doesn't exist
    ("SELECT * FROM non.existent", DatabaseError),
]
# fmt:on

@pytest.mark.parametrize("statement, error", STATEMENTS)
def test_no_data_errors(statement, error):

    conn = opteryx.connect(reader=DiskStorage(), partition_scheme=None)
    cursor = conn.cursor()

    ex = None
    value = None
    try:
        cursor.execute(statement)   # some errors thrown in planning
        value = cursor.fetchone()   # some errors thrown in execution
    except Exception as exception:
        ex = exception
    finally:
        assert isinstance(ex, error), f"{error} expected but {ex} thrown."


if __name__ == "__main__":  # pragma: no cover

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} DATE MISSING TESTS")
    for statement, error in STATEMENTS:
        print(statement)
        test_no_data_errors(statement, error)
    print("okay")
