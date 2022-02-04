"""
The best way to test a SQL engine is to throw queries at it.

We have two in-memory tables, one of natural satellite data and one of planet data.
These are both small to allow us to test the SQL engine quickly.

These tests only test the shape of the response, more specific tests wil test values,
the point of these tests is that we can throw many variations of queries, such as
different whitespace and capitalization and ensure we get a sane response. 
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
from opteryx import OpteryxQuery
import pytest

@pytest.mark.parametrize(
    "statement, rows, columns",
    # fmt:off
    [
        ("SELECT * FROM $satellites", 65499, 8),
        ("SELECT * FROM $satellites  WHERE user_name = 'Verizon Support'", 2, 8),
        ("SELECT * FROM $satellites  WHERE user_name == 'Verizon Support'", 2, 8),
        ("SELECT * FROM $satellites  WHERE `user_name` = 'Verizon Support'", 2, 8),
        ("SELECT * FROM $satellites  WHERE user_name = \"Verizon Support\"", 2, 8),
        ("select * from $satellites  where user_name = 'Verizon Support'", 2, 8),
        ("SELECT * FROM tests.data.index.not WHERE user_name = 'Verizon Support'", 2, 8),
        ("SELECT * FROM $satellites  WHERE user_name = '********'", 0, 8),
        ("SELECT * FROM $satellites  WHERE user_name LIKE '_erizon _upport'", 2, 8),
        ("SELECT * FROM $satellites  WHERE user_name LIKE '%Support%'", 31, 8),
        ("SELECT * FROM $satellites  WHERE user_name = 'Verizon Support'", 2, 8),
        ("SELECT * FROM $satellites  WHERE tweet_id = 1346604539923853313", 1, 8), 
        ("SELECT * FROM $satellites  WHERE tweet_id = 1346604539923853313 AND user_id = 4832862820", 1, 8),
        ("SELECT * FROM $satellites  WHERE tweet_id IN (1346604539923853313, 1346604544134885378)", 2, 8),
        ("SELECT * FROM $satellites  WHERE tweet_id = 1346604539923853313 OR user_id = 2147860407", 2, 8),
        ("SELECT * FROM $satellites  WHERE tweet_id = 1346604539923853313 OR user_verified = True", 453, 8),
        ("SELECT * FROM $satellites  WHERE user_name = 'Dave Jamieson' AND user_verified = True", 1, 8),
        ("SELECT COUNT(*) FROM $satellites  WHERE user_name = 'Dave Jamieson' AND user_verified = True", 1, 8),
        ("SELECT count(*) FROM $satellites GROUP BY user_verified", 2, 8),
        ("SELECT COUNT (*) FROM $satellites GROUP BY user_verified", 2, 8),
        ("SELECT Count(*) FROM $satellites GROUP BY user_verified", 2, 8),
        ("SELECT COUNT(*), user_verified FROM $satellites GROUP BY user_verified", 2, 8),
        ("SELECT * FROM $satellites WHERE hash_tags contains 'Georgia'", 50, 8),
        ("SELECT COUNT(*) FROM (SELECT user_name FROM $satellites GROUP BY user_name)", 1, 8),
        ("SELECT MAX(user_name) FROM $satellites", 1, 8),
        ("SELECT AVG(followers) FROM $satellites", 1, 8),
        ("SELECT * FROM $satellites ORDER BY user_name", 10000, 8), # ORDER BY is 10000 record limited
        ("SELECT * FROM $satellites ORDER BY user_name ASC", 10000, 8), # ORDER BY is 10000 record limited
        ("SELECT * FROM $satellites ORDER BY user_name DESC", 10000, 8), # ORDER BY is 10000 record limited
        ("SELECT COUNT(user_id) FROM $satellites", 1, 8),
        ("SELECT * FROM $satellites WHERE user_id > 1000000", 65475, 8),
        ("SELECT * FROM $satellites WHERE followers > 100.0", 49601, 8),
        ("SELECT COUNT(*), user_verified, user_id FROM $satellites GROUP BY user_verified, user_id", 60724, 8),
        ("SELECT * FROM $satellites WHERE user_name IN ('Steve Strong', 'noel')", 3, 8),
        ("SELECT `followers` FROM $satellites", 65499, 8),
        ("SELECT `user name` FROM tests.data.gaps", 25, 8),
        ("SELECT `user name` FROM tests.data.gaps WHERE `user name` = 'NBCNews'", 21, 8),
    ]
    # fmt:on
)
def test_sql_battery(statement, rows, columns):
    """
    Test an assortment of statements
    """
    res = OpteryxQuery(connection=None, operation=statement)
    res.materialize()
    actual_rows, actual_columns = res.shape

    assert (
        rows == actual_rows
    ), f"Query returned {actual_rows} rows but {rows} were expected, {statement}"
    assert (
        columns == actual_columns
    ), f"Query returned {actual_columns} cols but {columns} were expected, {statement}"
