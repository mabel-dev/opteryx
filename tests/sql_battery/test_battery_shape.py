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
import pyarrow
import pytest

import opteryx
from opteryx.storage.adapters import DiskStorage
from opteryx.utils.arrow import fetchmany
from opteryx.utils.display import ascii_table

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
        ("SELECT $satellites.* FROM $satellites", 177, 8),
        ("SELECT s.* FROM $satellites AS s", 177, 8),
        ("SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8),
        ("SELECT * FROM $satellites WHERE (name = 'Calypso')", 1, 8),
        ("SELECT * FROM $satellites WHERE NOT name = 'Calypso'", 176, 8),
        ("SELECT * FROM $satellites WHERE NOT (name = 'Calypso')", 176, 8),
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

        ("/* comment */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8),
        ("SELECT * FROM $satellites /* comment */ WHERE name = 'Calypso'", 1, 8),
        ("SELECT * FROM $satellites /* WHERE name = 'Calypso' */", 177, 8),
        ("SELECT * FROM $satellites -- WHERE name = 'Calypso'", 177, 8),
        ("SELECT * FROM $satellites -- WHERE name = 'Calypso' \n WHERE id = 1", 1, 8),
        ("-- comment\nSELECT * --comment\n FROM $satellites -- WHERE name = 'Calypso' \n WHERE id = 1", 1, 8),
        ("/* comment */ SELECT * FROM $satellites /* comment */  WHERE name = 'Calypso'", 1, 8),
        ("/* comment */ SELECT * FROM $satellites /* comment */  WHERE name = 'Calypso'  /* comment */ ", 1, 8),
        ("/* comment --inner */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8),
        ("SELECT * FROM $satellites -- comment\n FOR TODAY", 177, 8),
        ("SELECT * FROM $satellites /* comment */ FOR TODAY /* comment */", 177, 8),

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
        ("SELECT * FROM $satellites WHERE (id = 5) AND (name = 'Europa')", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 OR name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 OR name = 'Moon'", 2, 8),
        ("SELECT * FROM $satellites WHERE id < 3 AND (name = 'Europa' OR name = 'Moon')", 1, 8),
        ("SELECT * FROM $satellites WHERE id BETWEEN 5 AND 8", 4, 8),
        ("SELECT * FROM $satellites WHERE ((id BETWEEN 5 AND 10) AND (id BETWEEN 10 AND 12)) OR name = 'Moon'", 2, 8),
        ("SELECT * FROM $satellites WHERE id BETWEEN 5 AND 8 OR name = 'Moon'", 5, 8),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8)", 4, 8),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8) AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE (id IN (5,6,7,8)) AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE (id IN (5,6,7,8) AND name = 'Europa')", 1, 8),
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
        ("SELECT * FROM $satellites LIMIT 0", 0, 8),
        ("SELECT * FROM $satellites OFFSET 150", 27, 8),
        ("SELECT * FROM $satellites LIMIT 50 OFFSET 150", 27, 8),
        ("SELECT * FROM $satellites LIMIT 50 OFFSET 170", 7, 8),
        ("SELECT * FROM $satellites ORDER BY name", 177, 8),
        ("SELECT * FROM $satellites ORDER BY RANDOM()", 177, 8),

        ("SELECT MAX(planetId) FROM $satellites", 1, 1),
        ("SELECT MIN(planetId) FROM $satellites", 1, 1),
        ("SELECT SUM(planetId) FROM $satellites", 1, 1),
        ("SELECT MAX(id), planetId FROM $satellites GROUP BY planetId", 7, 2),
        ("SELECT MIN(id), planetId FROM $satellites GROUP BY planetId", 7, 2),
        ("SELECT SUM(id), planetId FROM $satellites GROUP BY planetId", 7, 2),
        ("SELECT MIN(id), MAX(id), SUM(planetId), planetId FROM $satellites GROUP BY planetId", 7, 4),
        ("SELECT planetId, LIST(name) FROM $satellites GROUP BY planetId", 7, 2),

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

        ("SELECT GET(birth_place, 'town') FROM $astronauts", 357, 1),
        ("SELECT GET(missions, 0) FROM $astronauts", 357, 1),
        ("SELECT GET(birth_place, 'town') FROM $astronauts WHERE GET(birth_place, 'town') = 'Warsaw'", 1, 1),
        ("SELECT COUNT(*), GET(birth_place, 'town') FROM $astronauts GROUP BY GET(birth_place, 'town')", 264, 2),
        ("SELECT birth_place['town'] FROM $astronauts", 357, 1),
        ("SELECT missions[0] FROM $astronauts", 357, 1),

        ("SELECT birth_place['town'] FROM $astronauts WHERE birth_place['town'] = 'Warsaw'", 1, 1),
        ("SELECT birth_place['town'] AS TOWN FROM $astronauts WHERE birth_place['town'] = 'Warsaw'", 1, 1),
        ("SELECT COUNT(*), birth_place['town'] FROM $astronauts GROUP BY birth_place['town']", 264, 2),
        ('SELECT LENGTH(missions) FROM $astronauts', 357, 1),
        ('SELECT LENGTH(missions) FROM $astronauts WHERE LENGTH(missions) > 6', 2, 1),

        ("SELECT birth_date FROM $astronauts", 357, 1),
        ("SELECT YEAR(birth_date) FROM $astronauts", 357, 1),
        ("SELECT YEAR(birth_date) FROM $astronauts WHERE YEAR(birth_date) < 1930", 14, 1),

        ("SELECT RANDOM() FROM $planets", 9, 1),
        ("SELECT NOW() FROM $planets", 9, 1),
        ("SELECT TODAY() FROM $planets", 9, 1),
        ("SELECT YEAR(birth_date), COUNT(*) FROM $astronauts GROUP BY YEAR(birth_date)", 54, 2),
        ("SELECT MONTH(birth_date), COUNT(*) FROM $astronauts GROUP BY MONTH(birth_date)", 12, 2),

        ("SELECT count(*), VARCHAR(year) FROM $astronauts GROUP BY VARCHAR(year)", 21, 2),
        ("SELECT count(*), CAST(year AS VARCHAR) FROM $astronauts GROUP BY CAST(year AS VARCHAR)", 21, 2),

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
        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred'))", 8, 1),
        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred')) WHERE unnest LIKE '%e%'", 2, 1),

        ("SELECT * FROM generate_series(10)", 10, 1),
        ("SELECT * FROM generate_series(-10,10)", 21, 1),
        ("SELECT * FROM generate_series(2,10,2)", 5, 1),
        ("SELECT * FROM generate_series(0.5,10,0.5)", 20, 1),
        ("SELECT * FROM generate_series(2,11,2)", 5, 1),
        ("SELECT * FROM generate_series(2,10,2) AS nums", 5, 1),
        ("SELECT * FROM generate_series(2,10,2) WHERE generate_series > 5", 3, 1),
        ("SELECT * FROM generate_series(2,10,2) AS nums WHERE nums < 5", 2, 1),

        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 month')", 12, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 mon')", 12, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1mon')", 12, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 day')", 365, 1),
        ("SELECT * FROM generate_series('2020-01-01', '2020-12-31', '1day')", 366, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '7 days')", 53, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-01-02', '1 hour')", 25, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-01-01 23:59', '1 hour')", 24, 1),
        ("SELECT * FROM generate_series('2022-01-01 12:00', '2022-01-01 23:59', '1 hour')", 12, 1),
        ("SELECT * FROM generate_series('2022-01-01 12:00', '2022-01-01 12:15', '1 minute')", 16, 1),
        ("SELECT * FROM generate_series('2022-01-01 12:00', '2022-01-01 12:15', '1m30s')", 11, 1),
        ("SELECT * FROM generate_series(1,10) LEFT JOIN $planets ON id = generate_series", 10, 21),
        ("SELECT * FROM generate_series(1,5) JOIN $planets ON id = generate_series", 5, 21),
        ("SELECT * FROM (SELECT * FROM generate_series(1,10,2) AS gs) INNER JOIN $planets on gs = id", 5, 21),

        ("SELECT * FROM generate_series('192.168.1.0/28')", 16, 1),

        ("SELECT * FROM tests.data.dated FOR '2020-02-03'", 25, 8),
        ("SELECT * FROM tests.data.dated FOR '2020-02-04'", 25, 8),
        ("SELECT * FROM tests.data.dated FOR '2020-02-05'", 0, 0),
        ("SELECT * FROM tests.data.dated FOR DATES BETWEEN '2020-02-01' AND '2020-02-28'", 50, 8),
        ("SELECT * FROM tests.data.dated FOR DATES BETWEEN YESTERDAY AND TODAY", 0, 0),
        ("SELECT * FROM tests.data.dated FOR TODAY", 0, 0),
        ("SELECT * FROM tests.data.dated FOR Today", 0, 0),
        ("SELECT * FROM tests.data.dated FOR today", 0, 0),
        ("SELECT * FROM tests.data.dated FOR   TODAY", 0, 0),
        ("SELECT * FROM tests.data.dated FOR\nTODAY", 0, 0),
        ("SELECT * FROM tests.data.dated FOR\tTODAY", 0, 0),
        ("SELECT * FROM tests.data.dated FOR YESTERDAY", 0, 0),
        ("SELECT * FROM tests.data.dated FOR '2020-02-03' OFFSET 1", 24, 8),
        ("SELECT * FROM tests.data.dated FOR DATES BETWEEN '2020-02-01' AND '2020-02-28' OFFSET 1", 49, 8),
        ("SELECT * FROM tests.data.dated FOR DATES IN LAST_MONTH", 0, 0),
        ("SELECT * FROM tests.data.dated FOR DATES IN THIS_MONTH", 0, 0),
        ("SELECT * FROM tests.data.dated FOR DATES IN PREVIOUS_MONTH", 0, 0),
        ("SELECT * FROM tests.data.dated FOR YESTERDAY OFFSET 1", 0, 0),
        ("SELECT * FROM $satellites FOR YESTERDAY ORDER BY planetId OFFSET 10", 167, 8),

        ("SELECT * FROM tests.data.segmented FOR '2020-02-03'", 25, 8),

        ("SELECT * FROM $satellites FOR DATES IN LAST_MONTH ORDER BY planetId OFFSET 10", 167, 8),
        ("SELECT * FROM $satellites FOR DATES IN LAST_CYCLE ORDER BY planetId OFFSET 10", 167, 8),
        ("SELECT * FROM $satellites FOR DATES IN THIS_MONTH ORDER BY planetId OFFSET 10", 167, 8),
        ("SELECT * FROM $satellites FOR DATES IN THIS_CYCLE ORDER BY planetId OFFSET 10", 167, 8),

        ("SELECT missions FROM $astronauts WHERE LIST_CONTAINS(missions, 'Apollo 8')", 3, 1),
        ("SELECT missions FROM $astronauts WHERE LIST_CONTAINS_ANY(missions, ('Apollo 8', 'Apollo 13'))", 5, 1),
        ("SELECT missions FROM $astronauts WHERE LIST_CONTAINS_ALL(missions, ('Apollo 8', 'Gemini 7'))", 2, 1),
        ("SELECT missions FROM $astronauts WHERE LIST_CONTAINS_ALL(missions, ('Gemini 7', 'Apollo 8'))", 2, 1),

        ("SELECT * FROM $satellites WHERE planetId IN (SELECT id FROM $planets WHERE name = 'Earth')", 1, 8),
        ("SELECT * FROM $planets WHERE id NOT IN (SELECT DISTINCT planetId FROM $satellites)", 2, 20),
        ("SELECT name FROM $planets WHERE id IN (SELECT * FROM UNNEST((1,2,3)) as id)", 3, 1),
        ("SELECT count(planetId) FROM (SELECT DISTINCT planetId FROM $satellites)", 1, 1),
        ("SELECT COUNT(*) FROM (SELECT planetId FROM $satellites WHERE planetId < 7) GROUP BY planetId", 4, 1),

        ("EXPLAIN SELECT * FROM $satellites", 2, 3),
        ("EXPLAIN SELECT * FROM $satellites WHERE id = 8", 3, 3),

        ("SHOW COLUMNS FROM $satellites", 8, 2),
        ("SHOW FULL COLUMNS FROM $satellites", 8, 6),
        ("SHOW EXTENDED COLUMNS FROM $satellites", 8, 12),
        ("SHOW EXTENDED COLUMNS FROM $planets", 20, 12),
        ("SHOW EXTENDED COLUMNS FROM $astronauts", 19, 12),
        ("SHOW COLUMNS FROM $satellites WHERE column_name ILIKE '%id'", 2, 2),
        ("SHOW COLUMNS FROM $satellites LIKE '%id'", 1, 2),
        ("SHOW COLUMNS FROM tests.data.dated FOR '2020-02-03'", 8, 2),

        ("SELECT * FROM $satellites CROSS JOIN $astronauts", 63189, 27),
        ("SELECT * FROM $satellites, $planets", 1593, 28),
        ("SELECT * FROM $satellites INNER JOIN $planets USING (id)", 9, 28),
        ("SELECT * FROM $satellites JOIN $planets USING (id)", 9, 28),
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(missions) AS mission WHERE mission = 'Apollo 11'", 3, 20),
#        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(Missions)", 0, 0),
        ("SELECT * FROM $planets INNER JOIN $satellites ON $planets.id = $satellites.planetId", 177, 28),
        ("SELECT DISTINCT planetId FROM $satellites LEFT OUTER JOIN $planets ON $satellites.planetId = $planets.id", 7, 1),
        ("SELECT DISTINCT planetId FROM $satellites LEFT JOIN $planets ON $satellites.planetId = $planets.id", 7, 1),
        ("SELECT DISTINCT planetId, $satellites.id FROM $planets LEFT OUTER JOIN $satellites ON $satellites.planetId = $planets.id", 178, 2),
        ("SELECT DISTINCT planetId, $satellites.id FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id", 178, 2),
        ("SELECT planetId FROM $satellites LEFT JOIN $planets ON $satellites.planetId = $planets.id", 177, 1),

        ("SELECT DISTINCT planetId FROM $satellites RIGHT OUTER JOIN $planets ON $satellites.planetId = $planets.id", 8, 1),
        ("SELECT DISTINCT planetId FROM $satellites RIGHT JOIN $planets ON $satellites.planetId = $planets.id", 8, 1),
        ("SELECT planetId FROM $satellites RIGHT JOIN $planets ON $satellites.planetId = $planets.id", 179, 1),
        ("SELECT DISTINCT planetId FROM $satellites FULL OUTER JOIN $planets ON $satellites.planetId = $planets.id", 8, 1),
        ("SELECT DISTINCT planetId FROM $satellites FULL JOIN $planets ON $satellites.planetId = $planets.id", 8, 1),
        ("SELECT planetId FROM $satellites FULL JOIN $planets ON $satellites.planetId = $planets.id", 179, 1),

        ("SELECT pid FROM ( SELECT id AS pid FROM $planets) WHERE pid > 5", 4, 1),
        ("SELECT * FROM ( SELECT id AS pid FROM $planets) WHERE pid > 5", 4, 1),
        ("SELECT * FROM ( SELECT COUNT(planetId) AS moons, planetId FROM $satellites GROUP BY planetId ) WHERE moons > 10", 4, 2),

        ("SELECT * FROM $planets WHERE id = -1", 0, 20),
        ("SELECT COUNT(*) FROM (SELECT DISTINCT a FROM $astronauts CROSS JOIN UNNEST(alma_mater) AS a ORDER BY a)", 1, 1),

        ("SELECT a.id, b.id, c.id FROM $planets AS a INNER JOIN $planets AS b ON a.id = b.id INNER JOIN $planets AS c ON c.id = b.id", 9, 3),
        ("SELECT * FROM $planets AS a INNER JOIN $planets AS b ON a.id = b.id RIGHT OUTER JOIN $satellites AS c ON c.planetId = b.id", 177, 48),

        ("SELECT $planets.* FROM $satellites INNER JOIN $planets USING (id)", 9, 20),
        ("SELECT $satellites.* FROM $satellites INNER JOIN $planets USING (id)", 9, 8),
        ("SELECT $planets.* FROM $satellites INNER JOIN $planets AS p USING (id)", 9, 20),
        ("SELECT p.* FROM $satellites INNER JOIN $planets AS p USING (id)", 9, 20),
        ("SELECT s.* FROM $satellites AS s INNER JOIN $planets USING (id)", 9, 8),
        ("SELECT $satellites.* FROM $satellites AS s INNER JOIN $planets USING (id)", 9, 8),
        ("SELECT $satellites.* FROM $satellites AS s INNER JOIN $planets AS p USING (id)", 9, 8),
        ("SELECT s.* FROM $satellites AS s INNER JOIN $planets AS p USING (id)", 9, 8),

        # These are queries which have been found to return the wrong result or not run correctly
        # FILTERING ON FUNCTIONS
        ("SELECT DATE(birth_date) FROM $astronauts FOR TODAY WHERE DATE(birth_date) < '1930-01-01'", 14, 1),
        # ORDER OF CLAUSES
        ("SELECT * FROM $planets FOR '2022-03-03' INNER JOIN $satellites ON $planets.id = $satellites.planetId", 177, 28),
        # ZERO RECORD RESULT SETS
        ("SELECT * FROM $planets WHERE id = -1 ORDER BY id", 0, 20),
        ("SELECT * FROM $planets WHERE id = -1 LIMIT 10", 0, 20),
        # LEFT JOIN THEN FILTER ON NULLS
        ("SELECT * FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id WHERE $satellites.id = NONE", 2, 28),
        ("SELECT * FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id WHERE $satellites.name = NONE", 2, 28),
        # SORT BROKEN
        ("SELECT * FROM (SELECT * FROM $planets ORDER BY id DESC LIMIT 5) WHERE id > 7", 2, 20),
        # ORDER OF JOIN CONDITION
        ("SELECT * FROM $planets INNER JOIN $satellites ON $satellites.planetId = $planets.id", 177, 28),
        # ORDER BY QUALIFIED IDENTIFIER
        ("SELECT * FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id ORDER BY $planets.name", 179, 28),
        # NAMED SUBQUERIES
        ("SELECT P.name FROM ( SELECT * FROM $planets ) AS P", 9, 1),
        # UNNEST
        ("SELECT * FROM tests.data.unnest_test CROSS JOIN UNNEST (values) AS value FOR '2000-01-01'", 15, 3),
        # FRAME HANDLING
        ("SELECT * FROM tests.data.framed FOR '2021-03-28'", 100000, 1),
        ("SELECT * FROM tests.data.framed FOR '2021-03-29'", 100000, 1),
        ("SELECT * FROM tests.data.framed FOR '2021-03-30'", 0, 0),
        ("SELECT * FROM tests.data.framed FOR DATES BETWEEN '2021-03-28' AND '2021-03-29", 200000, 1),
        ("SELECT * FROM tests.data.framed FOR DATES BETWEEN '2021-03-29' AND '2021-03-30", 100000, 1),
        ("SELECT * FROM tests.data.framed FOR DATES BETWEEN '2021-03-28' AND '2021-03-30", 200000, 1),
        # DOESN'T WORK WITH LARGE DATASETS (#179)
        ("SELECT * FROM (SELECT COUNT(*), column_1 FROM FAKE(5000,2) GROUP BY column_1 ORDER BY COUNT(*)) LIMIT 5", 5, 2),
        # FILTER CREATION FOR 3 OR MORE ANDED PREDICATES FAILS (#182)
        ("SELECT * FROM $astronauts WHERE name LIKE '%o%' AND `year` > 1900 AND gender ILIKE '%ale%' AND group IN (1,2,3,4,5,6)", 41, 19),
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
    else:  # pragma: no cover
        result = None
        actual_rows, actual_columns = 0, 0

    assert (
        rows == actual_rows
    ), f"Query returned {actual_rows} rows but {rows} were expected, {statement}\n{ascii_table(fetchmany(result, limit=10), limit=10)}"
    assert (
        columns == actual_columns
    ), f"Query returned {actual_columns} cols but {columns} were expected, {statement}\n{ascii_table(fetchmany(result, limit=10), limit=10)}"


if __name__ == "__main__":  # pragma: no cover

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SHAPE TESTS")
    for statement, rows, cols in STATEMENTS:
        print(statement)
        test_sql_battery(statement, rows, cols)
    print("okay")
