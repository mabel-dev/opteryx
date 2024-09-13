"""
Test we can read from GCS
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import GcpFireStoreConnector


def test_firestore_storage():
    opteryx.register_store("dwarves", GcpFireStoreConnector)
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()

    # SELECT EVERYTHING
    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves;")
    assert cur.rowcount == 7

    # PROCESS THE DATA IN SOME WAY
    cur = conn.cursor()
    cur.execute("SELECT actor, COUNT(*) FROM dwarves GROUP BY actor;")
    assert cur.rowcount == 6, cur.rowcount

    conn.close()

def test_predicate_pushdown():
    opteryx.register_store("dwarves", GcpFireStoreConnector)
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()

    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor = 'Pinto Colvig';")
    # when pushdown is enabled, we only read the matching rows from the source
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats["rows_read"] == 2, cur.stats

def test_predicate_pushdown_not_equals():
    """we don't push these, we get 5 records by Opteryx does the filtering not the source"""
    opteryx.register_store("dwarves", GcpFireStoreConnector)
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()
    
    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor != 'Pinto Colvig';")
    assert cur.rowcount == 5, cur.rowcount
    assert cur.stats["rows_read"] == 5, cur.stats


def test_predicate_pushdown_multiple_not_equals():
    """we don't push these, we get 5 records by Opteryx does the filtering not the source"""
    opteryx.register_store("dwarves", GcpFireStoreConnector)
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()
    
    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor != 'Pinto Colvig' and actor != 'Sleepy';")
    assert cur.rowcount == 5, cur.rowcount
    assert cur.stats["rows_read"] == 5, cur.stats

    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor != 'Pinto Colvig' and name != 'Sneezy';")
    assert cur.rowcount == 4, cur.rowcount
    assert cur.stats["rows_read"] == 4, cur.stats

def test_predicate_pushdown_multiple_equals():
    """we don't push these, we get 5 records by Opteryx does the filtering not the source"""
    opteryx.register_store("dwarves", GcpFireStoreConnector)
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()
    
    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor == 'Pinto Colvig' and actor == 'Sleepy';")
    assert cur.rowcount == 0, cur.rowcount
    assert cur.stats["rows_read"] == 0, cur.stats

def test_predicate_pushdown_multiple_mixed():
    """we don't push these, we get 5 records by Opteryx does the filtering not the source"""
    opteryx.register_store("dwarves", GcpFireStoreConnector)
    os.environ["GCP_PROJECT_ID"] = "mabeldev"

    conn = opteryx.connect()
    
    # TEST PREDICATE PUSHDOWN
    cur = conn.cursor()
    cur.execute("SELECT * FROM dwarves WHERE actor == 'Pinto Colvig' and actor != 'Sleepy';")
    assert cur.rowcount == 2, cur.rowcount
    assert cur.stats["rows_read"] == 2, cur.stats



if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
