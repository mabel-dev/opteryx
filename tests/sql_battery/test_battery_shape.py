"""
The best way to test a SQL engine is to throw queries at it.

We have three in-memory tables, one of natural satellite data, one of planet data and
one of astronaut data. These are both small to allow us to test the SQL engine quickly
and is guaranteed to be available whereever the tests are run.

These tests only test the shape of the response, more specific tests wil test values.
The point of these tests is that we can throw many variations of queries, such as
different whitespace and capitalization and ensure we get a sensible looking response.

We test the shape in this battery because if the shape isn't right, the response isn't
going to be right, and testing shape of an in-memory dataset is quick, we can test 100s
of queries in a few seconds.

Testing the shape doesn't mean the response is right though, so another battery will be
required to test values.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import opteryx
import pytest
from opteryx.storage.adapters import DiskStorage
from opteryx.utils.arrow import fetchmany
from opteryx.utils.display import ascii_table
import pyarrow

# fmt:off
STATEMENTS = [
        ("SELECT * FROM $satellites", 177, 8),
        ("SELECT * FROM $satellites;", 177, 8),
        ("SELECT * FROM $satellites\n;", 177, 8),
        ("select * from $satellites", 177, 8),
        ("Select * From $satellites", 177, 8),
        ("SELECT   *   FROM   $satellites", 177, 8),
        ("SELECT\n\t*\n  FROM\n\t$satellites", 177, 8),
        ("\n\n\n\tSELECT * FROM $satellites", 177, 8),
        ("SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8),
        ("SELECT * FROM $satellites WHERE `name` = 'Calypso'", 1, 8),
        ("select * from $satellites where name = 'Calypso'", 1, 8),
        ("SELECT * FROM $satellites WHERE name <> 'Calypso'", 176, 8),
        ("SELECT * FROM $satellites WHERE name = '********'", 0, 8),
        ("SELECT * FROM $satellites WHERE name LIKE '_a_y_s_'", 1, 8),
        ("SELECT * FROM $satellites WHERE name LIKE 'Cal%'", 4, 8),
        ("SELECT * FROM $satellites WHERE name like 'Cal%'", 4, 8),
        ("SELECT * FROM $satellites WHERE `name` = 'Calypso'", 1, 8),
        ("SELECT * FROM `$satellites` WHERE name = 'Calypso'", 1, 8),
        ("SELECT * FROM `$satellites` WHERE `name` = 'Calypso'", 1, 8),  

        ("SELECT name, id, planetId FROM $satellites", 177, 3), 
        ("SELECT name, name FROM $satellites", 177, 1),
        ("SELECT name, id, name, id FROM $satellites", 177, 2),

        ("SELECT name as Name FROM $satellites", 177, 1), 
        ("SELECT name as Name, id as Identifier FROM $satellites", 177, 2), 
        ("SELECT name as NAME FROM $satellites WHERE name = 'Calypso'", 1, 1), 
        ("SELECT name as NAME FROM $satellites GROUP BY name", 177, 1), 

        ("SELECT * FROM $satellites WHERE id = 5", 1, 8), 
        ("SELECT * FROM $satellites WHERE magnitude = 5.29", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 AND magnitude = 5.29", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 AND magnitude = 1", 0, 8),
        ("SELECT * FROM $satellites WHERE id = 5 AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 OR name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 OR name = 'Moon'", 2, 8),
        ("SELECT * FROM $satellites WHERE id BETWEEN 5 AND 8", 4, 8),
        ("SELECT * FROM $satellites WHERE ((id BETWEEN 5 AND 10) AND (id BETWEEN 10 AND 12)) OR name = 'Moon'", 2, 8),
        ("SELECT * FROM $satellites WHERE id BETWEEN 5 AND 8 OR name = 'Moon'", 5, 8),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8)", 4, 8),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8) AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8) OR name = 'Moon'", 5, 8),
        ("SELECT * FROM $satellites WHERE (id = 5 OR id = 6 OR id = 7 OR id = 8) AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE (id = 6 OR id = 7 OR id = 8) OR name = 'Europa'", 4, 8),
        ("SELECT * FROM $satellites WHERE id = 5 OR id = 6 OR id = 7 OR id = 8 OR name = 'Moon'", 5, 8),
        ("SELECT * FROM $satellites WHERE planetId = id", 1, 8),
        ("SELECT * FROM $satellites WHERE planetId > 8", 5, 8),
        ("SELECT * FROM $satellites WHERE planetId >= 8", 19, 8),
        ("SELECT * FROM $satellites WHERE planetId < 5", 3, 8),
        ("SELECT * FROM $satellites WHERE planetId <= 5", 70, 8),
        ("SELECT * FROM $satellites WHERE planetId <> 5", 110, 8),
        ("SELECT * FROM $satellites WHERE name LIKE 'C%'", 12, 8),
        ("SELECT * FROM $satellites WHERE name LIKE 'M__n'", 1, 8),
        ("SELECT * FROM $satellites WHERE name LIKE '%c%'", 11, 8),
        ("SELECT * FROM $satellites WHERE name ILIKE '%c%'", 23, 8),
        ("SELECT * FROM $satellites WHERE name NOT LIKE '%c%'", 166, 8),
        ("SELECT * FROM $satellites WHERE name NOT ILIKE '%c%'", 154, 8),
        ("SELECT * FROM $satellites WHERE name ~ '^C.'", 12, 8),

        ("SELECT COUNT(*) FROM $satellites", 1, 1),
        ("SELECT count(*) FROM $satellites", 1, 1),
        ("SELECT COUNT (*) FROM $satellites", 1, 1),
        ("SELECT\nCOUNT\n(*)\nFROM\n$satellites", 1, 1),
        ("SELECT Count(*) FROM $satellites", 1, 1),
        ("SELECT Count(*) FROM $satellites WHERE name = 'sputnik'", 1, 1),
        ("SELECT COUNT(name) FROM $satellites", 1, 1),
        ("SELECT COUNT(*) FROM $satellites GROUP BY name", 177, 1),
        ("SELECT COUNT(*) FROM $satellites GROUP BY planetId", 7, 1),
        ("SELECT COUNT(*) FROM $satellites GROUP\nBY planetId", 7, 1),
        ("SELECT COUNT(*) FROM $satellites GROUP     BY planetId", 7, 1),
        ("SELECT COUNT(*), planetId FROM $satellites GROUP BY planetId", 7, 2),
        ("SELECT COUNT(*), planetId FROM $satellites WHERE planetId < 6 GROUP BY planetId", 3, 2),                
        ("SELECT COUNT(*), planetId FROM $satellites WHERE planetId <= 6 GROUP BY planetId", 4, 2),      
        ("SELECT COUNT(*), planetId FROM $satellites WHERE name LIKE 'Cal%' GROUP BY planetId", 3, 2),
        
        ("SELECT DISTINCT planetId FROM $satellites", 7, 1),
        ("SELECT * FROM $satellites LIMIT 50", 50, 8),
        ("SELECT * FROM $satellites OFFSET 150", 27, 8),
        ("SELECT * FROM $satellites LIMIT 50 OFFSET 150", 27, 8),
        ("SELECT * FROM $satellites LIMIT 50 OFFSET 170", 7, 8),
        ("SELECT * FROM $satellites ORDER BY name", 177, 8),

        ("SELECT MAX(planetId) FROM $satellites", 1, 1),
        ("SELECT MIN(planetId) FROM $satellites", 1, 1),
        ("SELECT SUM(planetId) FROM $satellites", 1, 1),
        ("SELECT MAX(id), planetId FROM $satellites GROUP BY planetId", 7, 2),
        ("SELECT MIN(id), planetId FROM $satellites GROUP BY planetId", 7, 2),
        ("SELECT SUM(id), planetId FROM $satellites GROUP BY planetId", 7, 2),
        ("SELECT MIN(id), MAX(id), SUM(planetId), planetId FROM $satellites GROUP BY planetId", 7, 4),
        ("SELECT planetId, AGG_LIST(name) FROM $satellites GROUP BY planetId", 7, 2),

        ("SELECT BOOLEAN(planetId) FROM $satellites GROUP BY planetId, BOOLEAN(planetId)", 7, 1),
        ("SELECT VARCHAR(planetId) FROM $satellites GROUP BY planetId, VARCHAR(planetId)", 7, 1),
        ("SELECT TIMESTAMP(planetId) FROM $satellites GROUP BY planetId, TIMESTAMP(planetId)", 7, 1),
        ("SELECT NUMERIC(planetId) FROM $satellites GROUP BY planetId, NUMERIC(planetId)", 7, 1),
        ("SELECT CAST(planetId AS BOOLEAN) FROM $satellites", 177, 1),
        ("SELECT CAST(planetId AS VARCHAR) FROM $satellites", 177, 1),
        ("SELECT CAST(planetId AS TIMESTAMP) FROM $satellites", 177, 1),
        ("SELECT CAST(planetId AS NUMERIC) FROM $satellites", 177, 1),

        ("SELECT GET(name, 1) FROM $satellites GROUP BY planetId, GET(name, 1)", 56, 1),
        ("SELECT COUNT(*), ROUND(magnitude) FROM $satellites group by ROUND(magnitude)", 27, 2),
        ("SELECT ROUND(magnitude) FROM $satellites group by ROUND(magnitude)", 27, 1),
        ("SELECT round(magnitude) FROM $satellites group by round(magnitude)", 27, 1),
        ("SELECT upper(name) as NAME, id as Identifier FROM $satellites", 177, 2), 
        ("SELECT upper(name), lower(name), id as Identifier FROM $satellites", 177, 3), 

        ("SELECT planetId, Count(*) FROM $satellites group by planetId having count(*) > 5", 4, 2),
        ("SELECT planetId, min(magnitude) FROM $satellites group by planetId having min(magnitude) > 5", 5, 2),
        ("SELECT planetId, min(magnitude) FROM $satellites group by planetId having min(magnitude) > 5 limit 2 offset 1", 2, 2),
        ("SELECT planetId, count(*) FROM $satellites group by planetId order by count(*) desc", 7, 2),
        ("SELECT planetId, count(*) FROM $satellites group by planetId order by planetId desc", 7, 2),
        ("SELECT planetId, count(*) FROM $satellites group by planetId order by planetId, count(*) desc", 7, 2),
        ("SELECT planetId, count(*) FROM $satellites group by planetId order by count(*), planetId desc", 7, 2),

        ("SELECT * FROM $satellites order by name", 177, 8),
        ("SELECT * FROM $satellites order by name desc", 177, 8),
        ("SELECT name FROM $satellites order by name", 177, 1),
        ("SELECT * FROM $satellites order by magnitude, name", 177, 8),

        ("SELECT planetId as pid FROM $satellites", 177, 1),
        ("SELECT planetId as pid, round(magnitude) FROM $satellites", 177, 2),
        ("SELECT planetId as pid, round(magnitude) as minmag FROM $satellites", 177, 2),
        ("SELECT planetId as pid, round(magnitude) as roundmag FROM $satellites", 177, 2),

        ("SELECT GET(Birth_Place, 'town') FROM $astronauts", 357, 1),
        ("SELECT GET(Missions, 0) FROM $astronauts", 357, 1),
        ("SELECT GET(Birth_Place, 'town') FROM $astronauts WHERE GET(Birth_Place, 'town') = 'Warsaw'", 1, 1),
        ("SELECT COUNT(*), GET(Birth_Place, 'town') FROM $astronauts GROUP BY GET(Birth_Place, 'town')", 264, 2),
        ("SELECT Birth_Place['town'] FROM $astronauts", 357, 1),
        ("SELECT Missions[0] FROM $astronauts", 357, 1),
# Maps are currently not supported for selections or aggregations
#        ("SELECT Birth_Place['town'] FROM $astronauts WHERE Birth_Place['town'] = 'Warsaw'", 1, 1),
#        ("SELECT COUNT(*), Birth_Place['town'] FROM $astronauts GROUP BY Birth_Place['town']", 264, 2),
        ('SELECT LENGTH(Missions) FROM $astronauts', 357, 1),
        ('SELECT LENGTH(Missions) FROM $astronauts WHERE LENGTH(Missions) > 6', 2, 1),

        ("SELECT Birth_Date FROM $astronauts", 357, 1),
        ("SELECT YEAR(Birth_Date) FROM $astronauts", 357, 1),
        ("SELECT YEAR(Birth_Date) FROM $astronauts WHERE YEAR(Birth_Date) < 1930", 14, 1),

        ("SELECT RANDOM() FROM $planets", 9, 1),
        ("SELECT NOW() FROM $planets", 9, 1),
        ("SELECT TODAY() FROM $planets", 9, 1),
        ("SELECT YEAR(Birth_Date), COUNT(*) FROM $astronauts GROUP BY YEAR(Birth_Date)", 54, 2),
        ("SELECT MONTH(Birth_Date), COUNT(*) FROM $astronauts GROUP BY MONTH(Birth_Date)", 12, 2),

        ("SELECT RANDOM()", 1, 1),
        ("SELECT NOW()", 1, 1),
        ("SELECT TODAY()", 1, 1),
        ("SELECT HASH('hello')", 1, 1),
        ("SELECT MD5('hello')", 1, 1),
        ("SELECT UPPER('upper'), LOWER('LOWER')", 1, 2),

        ("SELECT * FROM (VALUES ('High', 3),('Medium', 2),('Low', 1)) AS ratings(name, rating)", 3, 2),
        ("SELECT * FROM (VALUES ('High', 3),('Medium', 2),('Low', 1)) AS ratings(name, rating) WHERE rating = 3", 1, 2),

        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred')) AS element", 8, 1),
        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred')) AS element WHERE element LIKE '%e%'", 2, 1),
        
        # this should return a single column table of all of the values in the missions columns
#        ("SELECT UNNEST(Missions) FROM $astronauts", 800, 1),

        ("SELECT * FROM tests.data.dated FOR '2020-02-03'", 25, 8),
        ("SELECT * FROM tests.data.dated FOR '2020-02-04'", 25, 8),
        ("SELECT * FROM tests.data.dated FOR '2020-02-05'", 0, 0),
        ("SELECT * FROM tests.data.dated FOR DATES BETWEEN '2020-02-01' AND '2020-02-28'", 50, 8),
        ("SELECT * FROM tests.data.dated FOR DATES BETWEEN '2020-02-01' AND today", 50, 8),
        ("SELECT * FROM tests.data.dated FOR DATES BETWEEN '2020-02-01' AND Today", 50, 8),
        ("SELECT * FROM tests.data.dated FOR DATES BETWEEN '2020-02-01' AND YESTERDAY", 50, 8),
        ("SELECT * FROM tests.data.dated FOR TODAY", 0, 0),
        ("SELECT * FROM tests.data.dated FOR YESTERDAY", 0, 0),
        ("SELECT * FROM tests.data.dated FOR '2020-02-03' OFFSET 1", 24, 8),
        ("SELECT * FROM tests.data.dated FOR DATES BETWEEN '2020-02-01' AND '2020-02-28' OFFSET 1", 49, 8),
        ("SELECT * FROM tests.data.dated FOR DATES BETWEEN '2020-02-01' AND today OFFSET 1", 49, 8),
        ("SELECT * FROM tests.data.dated FOR YESTERDAY OFFSET 1", 0, 0),

        ("SELECT Missions FROM $astronauts WHERE LIST_CONTAINS(Missions, 'Apollo 8')", 3, 1),
        ("SELECT Missions FROM $astronauts WHERE LIST_CONTAINS_ANY(Missions, ('Apollo 8', 'Apollo 13'))", 5, 1),
        ("SELECT Missions FROM $astronauts WHERE LIST_CONTAINS_ALL(Missions, ('Apollo 8', 'Gemini 7'))", 2, 1),
        ("SELECT Missions FROM $astronauts WHERE LIST_CONTAINS_ALL(Missions, ('Gemini 7', 'Apollo 8'))", 2, 1),

        ("SELECT * FROM $satellites WHERE planetId IN (SELECT id FROM $planets WHERE name = 'Earth')", 1, 8),
        ("SELECT * FROM $planets WHERE id NOT IN (SELECT DISTINCT planetId FROM $satellites)", 2, 20),
        ("SELECT name FROM $planets WHERE id IN (SELECT * FROM UNNEST((1,2,3)) as id)", 3, 1),
        ("SELECT count(planetId) FROM (SELECT DISTINCT planetId FROM $satellites)", 1, 1),
        ("SELECT COUNT(*) FROM (SELECT planetId FROM $satellites WHERE planetId < 7) GROUP BY planetId", 4, 1),

        ("EXPLAIN SELECT * FROM $satellites", 2, 3),
        ("EXPLAIN SELECT * FROM $satellites WHERE id = 8", 3, 3),

        ("SHOW COLUMNS FROM $satellites", 8, 2),
        ("SHOW COLUMNS FROM $satellites WHERE column_name ILIKE '%id'", 2, 2),
        ("SHOW COLUMNS FROM $satellites LIKE '%id'", 1, 2),

        ("SELECT * FROM $satellites CROSS JOIN $astronauts", 63189, 27),
        ("SELECT * FROM $satellites, $planets", 1593, 28),
        ("SELECT * FROM $satellites INNER JOIN $planets USING (id)", 9, 28),
        ("SELECT * FROM $satellites JOIN $planets USING (id)", 9, 28),
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(Missions) AS Mission WHERE Mission = 'Apollo 11'", 3, 20),
        ("SELECT * FROM $planets INNER JOIN $satellites ON $planets.id = $satellites.planetId", 177, 28),
        ("SELECT DISTINCT planetId FROM $satellites LEFT OUTER JOIN $planets ON $satellites.planetId = $planets.id", 8, 1),
        ("SELECT DISTINCT planetId FROM $satellites LEFT JOIN $planets ON $satellites.planetId = $planets.id", 8, 1),
        ("SELECT planetId FROM $satellites LEFT JOIN $planets ON $satellites.planetId = $planets.id", 179, 1),

        ("SELECT pid FROM ( SELECT id AS pid FROM $planets) WHERE pid > 5", 4, 1),
        ("SELECT * FROM ( SELECT id AS pid FROM $planets) WHERE pid > 5", 4, 1),
        ("SELECT * FROM ( SELECT COUNT(planetId) AS moons, planetId FROM $satellites GROUP BY planetId ) WHERE moons > 10", 4, 2),

        # These are queries which have been found to return the wrong result or not run
        # correctly
        ("SELECT * FROM $satellites FOR YESTERDAY ORDER BY planetId OFFSET 10", 167, 8),
        ("SELECT DATE(Birth_Date) FROM $astronauts FOR TODAY WHERE DATE(Birth_Date) < '1930-01-01'", 14, 1),
        ("SELECT * FROM $planets FOR '2022-03-03' INNER JOIN $satellites ON $planets.id = $satellites.planetId", 177, 28),
    ]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns", STATEMENTS)
def test_sql_battery(statement, rows, columns):
    """
    Test an battery of statements
    """
    conn = opteryx.connect(reader=DiskStorage(), partition_scheme="mabel")
    cursor = conn.cursor()
    cursor.execute(statement)

    cursor._results = list(cursor._results)
    if cursor._results:
        result = pyarrow.concat_tables(cursor._results)
        actual_rows, actual_columns = result.shape
    else:
        result = None
        actual_rows, actual_columns = 0, 0

    assert (
        rows == actual_rows
    ), f"Query returned {actual_rows} rows but {rows} were expected, {statement}\n{ascii_table(fetchmany(result, limit=10))}"
    assert (
        columns == actual_columns
    ), f"Query returned {actual_columns} cols but {columns} were expected, {statement}\n{ascii_table(fetchmany(result, limit=10))}"


if __name__ == "__main__":

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} TESTS")
    for statement, rows, cols in STATEMENTS:
        print(statement)
        test_sql_battery(statement, rows, cols)
    print("okay")
