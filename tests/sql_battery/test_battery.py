"""
The best way to test a SQL engine is to throw queries at it.

We have two in-memory tables, one of natural satellite data and one of planet data.
These are both small to allow us to test the SQL engine quickly and is guaranteed to
be available whereever the tests are run.

These tests only test the shape of the response, more specific tests wil test values.
The point of these tests is that we can throw many variations of queries, such as
different whitespace and capitalization and ensure we get a sensible looking response.

We test the shape in this battery because if the shape isn't right, the response isn't
going to be right, and testing shape of an in-memory dataset is quick, we can test 100s
of queries in a few seconds.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import opteryx
import pytest
from opteryx.storage.adapters import DiskStorage
from opteryx.third_party.pyarrow_ops import head
import pyarrow

# fmt:off
STATEMENTS = [
        ("SELECT * FROM $satellites", 177, 8),
        ("select * from $satellites", 177, 8),
        ("Select * From $satellites", 177, 8),
        ("SELECT   *   FROM   $satellites", 177, 8),
        ("SELECT\n\t*\n  FROM\n\t$satellites", 177, 8),
        ("\n\n\n\tSELECT * FROM $satellites", 177, 8),
        ("SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8),
        ("SELECT * FROM $satellites WHERE `name` = 'Calypso'", 1, 8),
        #("SELECT * FROM $satellites WHERE \"name\" = 'Calypso'", 1, 8),
        ("select * from $satellites where name = 'Calypso'", 1, 8),
        #("SELECT * FROM \"$satellites\" WHERE name = 'Calypso'", 1, 8),
        #("SELECT * FROM \"$satellites\" WHERE \"name\" = 'Calypso'", 1, 8),        
        ("SELECT * FROM $satellites WHERE name <> 'Calypso'", 176, 8),
        ("SELECT * FROM $satellites WHERE name = '********'", 0, 8),
        ("SELECT * FROM $satellites WHERE name LIKE '_a_y_s_'", 1, 8),
        ("SELECT * FROM $satellites WHERE name LIKE 'Cal%'", 4, 8),
        ("SELECT * FROM $satellites WHERE name like 'Cal%'", 4, 8),

        ("SELECT * FROM $satellites WHERE id = 5", 1, 8), 
        #("SELECT * FROM $satellites WHERE id = 5 AND magnitude = 5.29", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 AND magnitude = 1", 0, 8),
        ("SELECT * FROM $satellites WHERE id = 5 AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 OR name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 OR name = 'Moon'", 2, 8),
        ("SELECT * FROM $satellites WHERE id BETWEEN 5 AND 8", 4, 8),
        #("SELECT * FROM $satellites WHERE id BETWEEN 5 AND 8 AND name = 'Eurpoa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id BETWEEN 5 AND 8 OR name = 'Moon'", 5, 8),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8)", 4, 8),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8) AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8) OR name = 'Moon'", 5, 8),

        ("SELECT COUNT(*) FROM $satellites", 1, 1),
        ("SELECT count(*) FROM $satellites", 1, 1),
        ("SELECT COUNT (*) FROM $satellites", 1, 1),
        ("SELECT Count(*) FROM $satellites", 1, 1),
        ("SELECT COUNT(name) FROM $satellites", 1, 1),
        ("SELECT COUNT(*) FROM $satellites GROUP BY name", 177, 1),
        ("SELECT COUNT(*) FROM $satellites GROUP BY planetId", 7, 1),
        ("SELECT COUNT(*) FROM $satellites GROUP\nBY planetId", 7, 1),
        ("SELECT COUNT(*) FROM $satellites GROUP     BY planetId", 7, 1),
        ("SELECT COUNT(*), planetId FROM $satellites GROUP BY planetId", 7, 2),
                
        
        
#        ("SELECT COUNT(*) FROM $satellites  WHERE user_name = 'Dave Jamieson' AND user_verified = True", 1, 8),
#        ("SELECT count(*) FROM $satellites GROUP BY user_verified", 2, 8),
#        ("SELECT COUNT (*) FROM $satellites GROUP BY user_verified", 2, 8),
#        ("SELECT Count(*) FROM $satellites GROUP BY user_verified", 2, 8),
#        ("SELECT COUNT(*), user_verified FROM $satellites GROUP BY user_verified", 2, 8),
#        ("SELECT * FROM $satellites WHERE hash_tags contains 'Georgia'", 50, 8),
#        ("SELECT COUNT(*) FROM (SELECT user_name FROM $satellites GROUP BY user_name)", 1, 8),
#        ("SELECT MAX(user_name) FROM $satellites", 1, 8),
#        ("SELECT AVG(followers) FROM $satellites", 1, 8),
#        ("SELECT * FROM $satellites ORDER BY user_name", 10000, 8), # ORDER BY is 10000 record limited
#        ("SELECT * FROM $satellites ORDER BY user_name ASC", 10000, 8), # ORDER BY is 10000 record limited
#        ("SELECT * FROM $satellites ORDER BY user_name DESC", 10000, 8), # ORDER BY is 10000 record limited
#        ("SELECT * FROM $satellites WHERE user_id > 1000000", 65475, 8),
#        ("SELECT * FROM $satellites WHERE followers > 100.0", 49601, 8),
#        ("SELECT COUNT(*), user_verified, user_id FROM $satellites GROUP BY user_verified, user_id", 60724, 8),
#        ("SELECT * FROM $satellites WHERE user_name IN ('Steve Strong', 'noel')", 3, 8),
#        ("SELECT `followers` FROM $satellites", 65499, 8),
#        ("SELECT `user name` FROM tests.data.gaps", 25, 8),
#        ("SELECT `user name` FROM tests.data.gaps WHERE `user name` = 'NBCNews'", 21, 8),
    ]
    # fmt:on

@pytest.mark.parametrize(
    "statement, rows, columns", STATEMENTS    
)
def test_sql_battery(statement, rows, columns):
    """
    Test an assortment of statements
    """
    conn = opteryx.connect(reader=DiskStorage(), partition_scheme=None)
    cursor = conn.cursor()
    cursor.execute(statement)

    result = pyarrow.concat_tables(cursor._results)
    actual_rows, actual_columns = result.shape

    assert (
        rows == actual_rows
    ), f"Query returned {actual_rows} rows but {rows} were expected, {statement}, {head(result)}"
    assert (
        columns == actual_columns
    ), f"Query returned {actual_columns} cols but {columns} were expected, {statement}"


if __name__ == "__main__":

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} TESTS")
    for statement, rows, cols in STATEMENTS:
        print(statement)
        test_sql_battery(statement, rows, cols)