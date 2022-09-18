"""
The best way to test a SQL Engine is to throw queries at it.

This is the main SQL Battery set, others exist for testing specific features (like
reading different file types) but this is the main set of tests for if the Engine
can respond to a query.

Some test blocks have labels as to what the block is generally testing, even fewer
tests have comments as to why they exist (usually if the test is a regression test)

We have three in-memory tables, one of natural satellite data, one of planet data and
one of astronaut data. These are both small to allow us to test the SQL engine quickly
and is guaranteed to be available whereever the tests are run.

These are suppliments with a few physical tables to test conditions unable to be tested
with the in-memory tables.

The point of these tests is that we can throw many variations of queries, such as
different whitespace and capitalization and ensure we get a sensible looking response.

We test the shape in this battery because if the shape isn't right, the response isn't
going to be right, and testing shape of an in-memory dataset is quick, we can test 100s
of queries in a few seconds.

Note that testing the shape doesn't mean the response is right though.

These tests only test the shape of the response, more specific tests would be needed to
test values.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow
import pytest

import opteryx

from opteryx.utils.arrow import fetchmany
from opteryx.utils.display import ascii_table
from opteryx.connectors import DiskConnector


# fmt:off
STATEMENTS = [
        # Are the datasets the shape we expect?
        ("SELECT * FROM $satellites", 177, 8),
        ("SELECT * FROM $planets", 9, 20),
        ("SELECT * FROM $astronauts", 357, 19),

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
        ("SELECT * FROM $satellites WITH (NO_CACHE)", 177, 8),

        # Do we handle comments 
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

        ("SELECT DISTINCT name FROM $astronauts", 357, 1),
        ("SELECT DISTINCT * FROM $astronauts", 357, 19),
        ("SELECT DISTINCT birth_date FROM $astronauts", 348, 1),
        ("SELECT DISTINCT birth_place FROM $astronauts", 272, 1),
        ("SELECT DISTINCT death_date FROM $astronauts", 39, 1),
        ("SELECT DISTINCT missions FROM $astronauts", 305, 1),
        ("SELECT DISTINCT group FROM $astronauts", 21, 1),
        ("SELECT DISTINCT name, birth_date, missions, birth_place, group FROM $astronauts", 357, 5),

        ("SELECT name as Name FROM $satellites", 177, 1),
        ("SELECT name as Name, id as Identifier FROM $satellites", 177, 2),
        ("SELECT name as NAME FROM $satellites WHERE name = 'Calypso'", 1, 1),
        ("SELECT name as NAME FROM $satellites GROUP BY name", 177, 1),

        # Test infix calculations
        ("SELECT * FROM $satellites WHERE id = 5", 1, 8),
        ("SELECT * FROM $satellites WHERE name = 'Cal' || 'ypso'", 1, 8),
        ("SELECT * FROM $satellites WHERE name = 'C' || 'a' || 'l' || 'y' || 'p' || 's' || 'o'", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 * 1 AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 10 / 2 AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 3 + 2 AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id + 2 = 7 AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 15 % 10 AND name = 'Europa'", 1, 8),

        ("SELECT * FROM $satellites WHERE magnitude = 5.29", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 AND magnitude = 5.29", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 AND magnitude = 1", 0, 8),
        ("SELECT * FROM $satellites WHERE id = 5 AND name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE (id = 5) AND (name = 'Europa')", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 OR name = 'Europa'", 1, 8),
        ("SELECT * FROM $satellites WHERE id = 5 OR name = 'Moon'", 2, 8),
        ("SELECT * FROM $satellites WHERE id < 3 AND (name = 'Europa' OR name = 'Moon')", 1, 8),
        ("SELECT * FROM $satellites WHERE id BETWEEN 5 AND 8", 4, 8),
        ("SELECT * FROM $satellites WHERE id NOT BETWEEN 5 AND 8", 173, 8),
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
        ("SELECT * FROM $satellites WHERE name SIMILAR TO '^C.'", 12, 8),
        ("SELECT * FROM $satellites WHERE name !~ '^C.'", 165, 8),
        ("SELECT * FROM $satellites WHERE name NOT SIMILAR TO '^C.'", 165, 8),
        ("SELECT * FROM $satellites WHERE name ~* '^c.'", 12, 8),
        ("SELECT * FROM $satellites WHERE name !~* '^c.'", 165, 8),

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
        ("SELECT * FROM $satellites ORDER BY 1", 177, 8),
        ("SELECT * FROM $satellites ORDER BY 1 DESC", 177, 8),
        ("SELECT * FROM $satellites ORDER BY 2", 177, 8),
        ("SELECT * FROM $satellites ORDER BY 1, 2", 177, 8),
        ("SELECT * FROM $satellites ORDER BY 1 ASC", 177, 8),
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
        ("SELECT INT(planetId) FROM $satellites GROUP BY planetId, INT(planetId)", 7, 1),
        ("SELECT INTEGER(planetId) FROM $satellites GROUP BY planetId, INTEGER(planetId)", 7, 1),
        ("SELECT FLOAT(planetId) FROM $satellites GROUP BY planetId, FLOAT(planetId)", 7, 1),
        ("SELECT CAST(planetId AS BOOLEAN) FROM $satellites", 177, 1),
        ("SELECT CAST(planetId AS VARCHAR) FROM $satellites", 177, 1),
        ("SELECT CAST(planetId AS TIMESTAMP) FROM $satellites", 177, 1),
        ("SELECT CAST(planetId AS NUMERIC) FROM $satellites", 177, 1),
        ("SELECT TRY_CAST(planetId AS BOOLEAN) FROM $satellites", 177, 1),
        ("SELECT TRY_CAST(planetId AS VARCHAR) FROM $satellites", 177, 1),
        ("SELECT TRY_CAST(planetId AS TIMESTAMP) FROM $satellites", 177, 1),
        ("SELECT TRY_CAST(planetId AS NUMERIC) FROM $satellites", 177, 1),

        ("SELECT PI()", 1, 1),
        ("SELECT E()", 1, 1),
        ("SELECT PHI()", 1, 1),
        ("SELECT GET(name, 1) FROM $satellites GROUP BY planetId, GET(name, 1)", 56, 1),
        ("SELECT COUNT(*), ROUND(magnitude) FROM $satellites group by ROUND(magnitude)", 22, 2),
        ("SELECT ROUND(magnitude) FROM $satellites group by ROUND(magnitude)", 22, 1),
        ("SELECT ROUND(magnitude, 1) FROM $satellites group by ROUND(magnitude, 1)", 88, 1),
        ("SELECT VARCHAR(planetId), COUNT(*) FROM $satellites GROUP BY 1", 7, 2),
        ("SELECT LEFT(name, 1), COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 21, 2),
        ("SELECT LEFT(name, 2) as le, COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 87, 2),
        ("SELECT RIGHT(name, 10), COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 177, 2),
        ("SELECT RIGHT(name, 2) as le, COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 30, 2),
        ("SELECT round(magnitude) FROM $satellites group by round(magnitude)", 22, 1),
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
        ("SELECT CURRENT_DATE", 1, 1),
        ("SELECT CURRENT_DATE()", 1, 1),
        ("SELECT CURRENT_TIME", 1, 1),
        ("SELECT CURRENT_TIME()", 1, 1),
        ("SELECT YEAR(birth_date), COUNT(*) FROM $astronauts GROUP BY YEAR(birth_date)", 54, 2),
        ("SELECT MONTH(birth_date), COUNT(*) FROM $astronauts GROUP BY MONTH(birth_date)", 12, 2),

        ("SELECT DATE_FORMAT(birth_date, '%d-%Y') FROM $astronauts", 357, 1),
        ("SELECT DATE_FORMAT(birth_date, 'dddd') FROM $astronauts", 357, 1),
        ("SELECT DATE_FORMAT(death_date, '%Y') FROM $astronauts", 357, 1),

        ("SELECT count(*), VARCHAR(year) FROM $astronauts GROUP BY VARCHAR(year)", 21, 2),
        ("SELECT count(*), CAST(year AS VARCHAR) FROM $astronauts GROUP BY CAST(year AS VARCHAR)", 21, 2),

        ("SELECT RANDOM()", 1, 1),
        ("SELECT RAND()", 1, 1),
        ("SELECT NOW()", 1, 1),
        ("SELECT NOW() from $planets", 9, 1),
        ("SELECT TODAY()", 1, 1),
        ("SELECT HASH('hello')", 1, 1),
        ("SELECT MD5('hello')", 1, 1),
        ("SELECT SHA1('hello')", 1, 1),
        ("SELECT SHA224('hello')", 1, 1),
        ("SELECT SHA256('hello')", 1, 1),
        ("SELECT SHA384('hello')", 1, 1),
        ("SELECT SHA512('hello')", 1, 1),
        ("SELECT UPPER('upper'), LOWER('LOWER')", 1, 2),
        ("SELECT POWER(2, 10)", 1, 1),
        ("SELECT LN(10)", 1, 1),
        ("SELECT LOG10(10)", 1, 1),
        ("SELECT LOG2(10)", 1, 1),
        ("SELECT LOG(10, 4)", 1, 1),

        ("SELECT HASH(name), name from $astronauts", 357, 2),
        ("SELECT HASH(death_date), death_date from $astronauts", 357, 2),
        ("SELECT HASH(birth_place), birth_place from $astronauts", 357, 2),
        ("SELECT HASH(missions), missions from $astronauts", 357, 2),

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
        ("SELECT * FROM generate_series(2) WITH (NO_CACHE)", 2, 1),

        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 month')", 12, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 mon')", 12, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1mon')", 12, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1mo')", 12, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1mth')", 12, 1),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 months')", 12, 1),
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
        ("SELECT * FROM generate_series('192.168.1.100/29')", 8, 1),

        ("SELECT * FROM testdata.dated WITH (NO_CACHE) FOR '2020-02-03'", 25, 8),
        ("SELECT * FROM testdata.dated FOR '2020-02-03'", 25, 8),
        ("SELECT * FROM testdata.dated FOR '2020-02-04'", 25, 8),
        ("SELECT * FROM testdata.dated FOR DATES BETWEEN '2020-02-01' AND '2020-02-28'", 50, 8),
        ("SELECT * FROM testdata.dated FOR '2020-02-03' OFFSET 1", 24, 8),
        ("SELECT * FROM testdata.dated FOR DATES BETWEEN '2020-02-01' AND '2020-02-28' OFFSET 1", 49, 8),
        ("SELECT * FROM $satellites FOR YESTERDAY ORDER BY planetId OFFSET 10", 167, 8),

        ("SELECT * FROM testdata.segmented FOR '2020-02-03'", 25, 8),
        ("SELECT * FROM $planets FOR '1730-01-01'", 6, 20),
        ("SELECT * FROM $planets FOR '1830-01-01'", 7, 20),
        ("SELECT * FROM $planets FOR '1930-01-01'", 8, 20),
        ("SELECT * FROM $planets FOR '2030-01-01'", 9, 20),

        ("SELECT * FROM $astronauts WHERE death_date IS NULL", 305, 19),
        ("SELECT * FROM $astronauts WHERE death_date IS NOT NULL", 52, 19),
        ("SELECT * FROM testdata.formats.parquet WITH(NO_PARTITION) WHERE user_verified IS TRUE", 711, 13),
        ("SELECT * FROM testdata.formats.parquet WITH(NO_PARTITION) WHERE user_verified IS FALSE", 99289, 13),
        ("SELECT * FROM testdata.formats.csv WITH(NO_PARTITION) WHERE user_verified IS FALSE", 29633, 10),

        ("SELECT * FROM testdata.formats.parquet WITH(NO_PARTITION, PARALLEL_READ)", 100000, 13),

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
#        ("SELECT count(planetId) FROM (SELECT DISTINCT planetId FROM $satellites)", 1, 1),
        ("SELECT COUNT(*) FROM (SELECT planetId FROM $satellites WHERE planetId < 7) GROUP BY planetId", 4, 1),

        ("EXPLAIN SELECT * FROM $satellites", 1, 3),
        ("EXPLAIN SELECT * FROM $satellites WHERE id = 8", 2, 3),

        ("SHOW COLUMNS FROM $satellites", 8, 2),
        ("SHOW FULL COLUMNS FROM $satellites", 8, 6),
        ("SHOW EXTENDED COLUMNS FROM $satellites", 8, 12),
        ("SHOW EXTENDED COLUMNS FROM $planets", 20, 12),
        ("SHOW EXTENDED COLUMNS FROM $astronauts", 19, 12),
        ("SHOW COLUMNS FROM $satellites WHERE column_name ILIKE '%id'", 2, 2),
        ("SHOW COLUMNS FROM $satellites LIKE '%id'", 1, 2),
        ("SHOW COLUMNS FROM testdata.dated FOR '2020-02-03'", 8, 2),

        ("SELECT * FROM $satellites CROSS JOIN $astronauts", 63189, 27),
        ("SELECT * FROM $satellites WITH (NO_CACHE) CROSS JOIN $astronauts WITH (NO_CACHE)", 63189, 27),
        ("SELECT * FROM $satellites, $planets", 1593, 28),
        ("SELECT * FROM $satellites INNER JOIN $planets USING (id)", 9, 28),
        ("SELECT * FROM $satellites WITH (NO_CACHE) INNER JOIN $planets USING (id)", 9, 28),
        ("SELECT * FROM $satellites WITH (NO_CACHE) INNER JOIN $planets WITH (NO_CACHE) USING (id)", 9, 28),
        ("SELECT * FROM $satellites INNER JOIN $planets WITH (NO_CACHE) USING (id)", 9, 28),
        ("SELECT * FROM $satellites JOIN $planets USING (id)", 9, 28),
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(missions) AS mission WHERE mission = 'Apollo 11'", 3, 20),
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(missions)", 869, 20),
        ("SELECT * FROM $planets INNER JOIN $satellites ON $planets.id = $satellites.planetId", 177, 28),
        ("SELECT DISTINCT planetId FROM $satellites LEFT OUTER JOIN $planets ON $satellites.planetId = $planets.id", 7, 1),
        ("SELECT DISTINCT planetId FROM $satellites LEFT JOIN $planets ON $satellites.planetId = $planets.id", 7, 1),
        ("SELECT DISTINCT $planets.id, $satellites.id FROM $planets LEFT OUTER JOIN $satellites ON $satellites.planetId = $planets.id", 179, 2),
        ("SELECT DISTINCT $planets.id, $satellites.id FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id", 179, 2),
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

        ("SELECT DATE_TRUNC('month', birth_date) FROM $astronauts", 357, 1),
        ("SELECT DISTINCT * FROM (SELECT DATE_TRUNC('year', birth_date) AS BIRTH_YEAR FROM $astronauts)", 54, 1),
        ("SELECT DISTINCT * FROM (SELECT DATE_TRUNC('month', birth_date) AS BIRTH_YEAR_MONTH FROM $astronauts)", 247, 1),
        ("SELECT time_bucket(birth_date, 10, 'year') AS decade, count(*) from $astronauts GROUP BY time_bucket(birth_date, 10, 'year')", 6, 2),
        ("SELECT time_bucket(birth_date, 6, 'month') AS half, count(*) from $astronauts GROUP BY time_bucket(birth_date, 6, 'month')", 97, 2),
    
        ("SELECT graduate_major, undergraduate_major FROM $astronauts WHERE COALESCE(graduate_major, undergraduate_major, 'high school') = 'high school'", 4, 2),
        ("SELECT graduate_major, undergraduate_major FROM $astronauts WHERE COALESCE(graduate_major, undergraduate_major) = 'Aeronautical Engineering'", 41, 2),
        ("SELECT COALESCE(death_date, '2030-01-01') FROM $astronauts", 357, 1),
        ("SELECT * FROM $astronauts WHERE COALESCE(death_date, '2030-01-01') < '2000-01-01'", 30, 19),

        ("SELECT SEARCH(name, 'al'), name FROM $satellites", 177, 2),
        ("SELECT name FROM $satellites WHERE SEARCH(name, 'al')", 18, 1),
        ("SELECT SEARCH(missions, 'Apollo 11'), missions FROM $astronauts", 357, 2),
        ("SELECT name FROM $astronauts WHERE SEARCH(missions, 'Apollo 11')", 3, 1),
        ("SELECT name, SEARCH(birth_place, 'Italy') FROM $astronauts", 357, 2),
        ("SELECT name, birth_place FROM $astronauts WHERE SEARCH(birth_place, 'Italy')", 1, 2),
        ("SELECT name, birth_place FROM $astronauts WHERE SEARCH(birth_place, 'Rome')", 1, 2),

        ("SELECT birth_date FROM $astronauts WHERE EXTRACT(year FROM birth_date) < 1930;", 14, 1),
        ("SELECT EXTRACT(month FROM birth_date) FROM $astronauts", 357, 1),
        ("SELECT EXTRACT(day FROM birth_date) FROM $astronauts", 357, 1),
        ("SELECT birth_date FROM $astronauts WHERE EXTRACT(year FROM birth_date) < 1930;", 14, 1),
        ("SELECT EXTRACT(doy FROM birth_date) FROM $astronauts", 357, 1),
        ("SELECT EXTRACT(DOY FROM birth_date) FROM $astronauts", 357, 1),
        ("SELECT EXTRACT(dow FROM birth_date) FROM $astronauts", 357, 1),
        ("SELECT EXTRACT(DOW FROM birth_date) FROM $astronauts", 357, 1),
        ("SELECT EXTRACT(YEAR FROM '2022-02-02')", 1, 1),
        ("SELECT DATE_FORMAT(birth_date, '%m-%y') FROM $astronauts", 357, 1),
        ("SELECT DATEDIFF('year', '2017-08-25', '2011-08-25') AS DateDiff;", 1, 1),
        ("SELECT DATEDIFF('days', '2022-07-07', birth_date) FROM $astronauts", 357, 1),
        ("SELECT DATEDIFF('minutes', birth_date, '2022-07-07') FROM $astronauts", 357, 1),
        ("SELECT EXTRACT(DOW FROM birth_date) AS DOW, COUNT(*) FROM $astronauts GROUP BY EXTRACT(DOW FROM birth_date) ORDER BY COUNT(*) DESC", 7, 2),

        ("SELECT * FROM testdata.schema WITH(NO_PARTITION) ORDER BY 1", 2, 4),
        ("SELECT * FROM testdata.schema WITH(NO_PARTITION, NO_PUSH_PROJECTION) ORDER BY 1", 2, 4),
        ("SELECT * FROM $planets WITH(NO_PARTITION) ORDER BY 1", 9, 20),
        ("SELECT * FROM $planets WITH(NO_PUSH_PROJECTION) ORDER BY 1", 9, 20),
        ("SELECT * FROM $planets WITH(NO_PARTITION, NO_PUSH_PROJECTION) ORDER BY 1", 9, 20),

        ("SELECT SQRT(mass) FROM $planets", 9, 1),
        ("SELECT FLOOR(mass) FROM $planets", 9, 1),
        ("SELECT CEIL(mass) FROM $planets", 9, 1),
        ("SELECT CEILING(mass) FROM $planets", 9, 1),
        ("SELECT ABS(mass) FROM $planets", 9, 1),
        ("SELECT ABSOLUTE(mass) FROM $planets", 9, 1),
        ("SELECT SIGN(mass) FROM $planets", 9, 1),
        ("SELECT reverse(name) From $planets", 9, 1),
        ("SELECT title(reverse(name)) From $planets", 9, 1),
        ("SELECT SOUNDEX(name) From $planets", 9, 1),

        ("SELECT APPROXIMATE_MEDIAN(radius) AS AM FROM $satellites GROUP BY planetId HAVING APPROXIMATE_MEDIAN(radius) > 5;", 5, 1),
        ("SELECT APPROXIMATE_MEDIAN(radius) AS AM FROM $satellites GROUP BY planetId HAVING AM > 5;", 5, 1),
        ("SELECT COUNT(planetId) FROM $satellites", 1, 1),
        ("SELECT COUNT_DISTINCT(planetId) FROM $satellites", 1, 1),
        ("SELECT LIST(name), planetId FROM $satellites GROUP BY planetId", 7, 2),
        ("SELECT ONE(name), planetId FROM $satellites GROUP BY planetId", 7, 2),
        ("SELECT ANY_VALUE(name), planetId FROM $satellites GROUP BY planetId", 7, 2),
        ("SELECT MAX(planetId) FROM $satellites", 1, 1),
        ("SELECT MAXIMUM(planetId) FROM $satellites", 1, 1),
        ("SELECT MEAN(planetId) FROM $satellites", 1, 1),
        ("SELECT AVG(planetId) FROM $satellites", 1, 1),
        ("SELECT AVERAGE(planetId) FROM $satellites", 1, 1),
        ("SELECT MIN(planetId) FROM $satellites", 1, 1),
        ("SELECT MIN_MAX(planetId) FROM $satellites", 1, 1),
        ("SELECT PRODUCT(planetId) FROM $satellites", 1, 1),
        ("SELECT STDDEV(planetId) FROM $satellites", 1, 1),
        ("SELECT SUM(planetId) FROM $satellites", 1, 1),
        ("SELECT VARIANCE(planetId) FROM $satellites", 1, 1),

        ("SELECT name || ' ' || name FROM $planets", 9, 1),
        ("SELECT 32 * 12", 1, 1),
        ("SELECT 9 / 12", 1, 1),
        ("SELECT 3 + 3", 1, 1),
        ("SELECT 12 % 2", 1, 1),
        ("SELECT 10 - 10", 1, 1),
        ("SELECT POWER(((6.67 * POWER(10, -11) * 5.97 * POWER(10, 24) * 86400 * 86400) / (4 * PI() * PI())), 1/3)", 1, 1),
        ("SELECT name || ' ' || name AS DBL FROM $planets", 9, 1),
        ("SELECT * FROM $satellites WHERE planetId = 2 + 5", 27, 8),
        ("SELECT * FROM $satellites WHERE planetId = round(density)", 1, 8),
        ("SELECT * FROM $satellites WHERE planetId * 1 = round(density * 1)", 1, 8),
        ("SELECT ABSOLUTE(ROUND(gravity) * density * density) FROM $planets", 9, 1),
        ("SELECT COUNT(*), ROUND(gm) FROM $satellites GROUP BY ROUND(gm)", 22, 2),
        ("SELECT COALESCE(death_date, '1900-01-01') FROM $astronauts", 357, 1),
        ("SELECT * FROM (SELECT COUNT(*) FROM testdata.formats.parquet WITH(NO_PARTITION) GROUP BY followers)", 10016, 1),
        ("SELECT a.id, b.id FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id)", 9, 2),
        ("SELECT * FROM $planets INNER JOIN $planets AS b USING (id)", 9, 40),
        ("SELECT ROUND(5 + RAND() * (10 - 5)) rand_between FROM $planets", 9, 1),

        ("SELECT BASE64_DECODE(BASE64_ENCODE('this is a string'));", 1, 1),
        ("SELECT BASE64_ENCODE('this is a string');", 1, 1),
        ("SELECT BASE64_DECODE('aGVsbG8=')", 1, 1),
        ("SELECT BASE85_DECODE(BASE85_ENCODE('this is a string'));", 1, 1),
        ("SELECT BASE85_ENCODE('this is a string');", 1, 1),
        ("SELECT BASE85_DECODE('Xk~0{Zv')", 1, 1),
        ("SELECT HEX_DECODE(HEX_ENCODE('this is a string'));", 1, 1),
        ("SELECT HEX_ENCODE('this is a string');", 1, 1),
        ("SELECT HEX_ENCODE(name) FROM $planets;", 9, 1),
        ("SELECT HEX_DECODE('68656C6C6F')", 1, 1),
        ("SELECT NORMAL()", 1, 1),
        ("SELECT NORMAL() FROM $astronauts", 357, 1),
        ("SELECT CONCAT(LIST(name)) FROM $planets GROUP BY gravity", 8, 1),
        ("SELECT CONCAT(missions) FROM $astronauts", 357, 1),
        ("SELECT CONCAT(('1', '2', '3'))", 1, 1),
        ("SELECT CONCAT(('1', '2', '3')) FROM $planets", 9, 1),
        ("SELECT CONCAT_WS(', ', LIST(name)) FROM $planets GROUP BY gravity", 8, 1),
        ("SELECT CONCAT_WS('*', missions) FROM $astronauts LIMIT 5", 5, 1),
        ("SELECT CONCAT_WS('-', ('1', '2', '3'))", 1, 1),
        ("SELECT CONCAT_WS('-', ('1', '2', '3')) FROM $planets", 9, 1),
        ("SELECT IFNULL(death_date, '1970-01-01') FROM $astronauts", 357, 1),
        ("SELECT RANDOM_STRING(88) FROM $planets", 9, 1),
        ("SELECT * FROM $planets WHERE STARTS_WITH(name, 'M')", 2, 20),
        ("SELECT * FROM $astronauts WHERE STARTS_WITH(name, 'Jo')", 23, 19),
        ("SELECT * FROM $planets WHERE ENDS_WITH(name, 'r')", 1, 20),
        ("SELECT * FROM $astronauts WHERE ENDS_WITH(name, 'son')", 17, 19),
        ("SELECT CONCAT_WS(', ', LIST(mass)) as MASSES FROM $planets GROUP BY gravity", 8, 1),
        ("SELECT GREATEST(LIST(name)) as NAMES FROM $satellites GROUP BY planetId", 7, 1),
        ("SELECT GREATEST(LIST(gm)) as MASSES FROM $satellites GROUP BY planetId", 7, 1),
        ("SELECT LEAST(LIST(name)) as NAMES FROM $satellites GROUP BY planetId", 7, 1),
        ("SELECT LEAST(LIST(gm)) as MASSES FROM $satellites GROUP BY planetId", 7, 1),
        ("SELECT IIF(SEARCH(missions, 'Apollo 13'), 1, 0), SEARCH(missions, 'Apollo 13'), missions FROM $astronauts", 357, 3),
        ("SELECT IIF(year = 1960, 1, 0), year FROM $astronauts", 357, 2),
        ("SELECT SUM(IIF(year < 1970, 1, 0)), MAX(year) FROM $astronauts", 1, 2),
        ("SELECT SUM(IIF(year < 1970, 1, 0)), year FROM $astronauts GROUP BY year ORDER BY year ASC", 21, 2),
        ("SELECT SUM(id) + id FROM $planets GROUP BY id", 9, 1),
        ("SELECT today() - INTERVAL '1' YEAR", 1, 1),
        ("SELECT today() - INTERVAL '1' MONTH", 1, 1),
        ("SELECT today() - INTERVAL '1' DAY", 1, 1),
        ("SELECT today() - INTERVAL '1' HOUR", 1, 1),
        ("SELECT today() - INTERVAL '1' MINUTE", 1, 1),
        ("SELECT today() - INTERVAL '1' SECOND", 1, 1),
        ("SELECT today() + INTERVAL '1' DAY", 1, 1),
        ("SELECT INTERVAL '1 1' DAY TO HOUR", 1, 1),
        ("SELECT INTERVAL '5 6' YEAR TO MONTH", 1, 1),
        ("SELECT today() - yesterday()", 1, 1),
        ("SELECT INTERVAL '100' YEAR + birth_date, birth_date from $astronauts", 357, 2),
        ("SELECT INTERVAL '1 1' MONTH to DAY + birth_date, birth_date from $astronauts", 357, 2),
        ("SELECT birth_date - INTERVAL '1 1' MONTH to DAY, birth_date from $astronauts", 357, 2),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' IN UNNEST(missions)", 3, 19),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' NOT IN UNNEST(missions)", 331, 19),
        ("SELECT * FROM $astronauts WHERE NOT 'Apollo 11' IN UNNEST(missions)", 354, 19),
        ("SET @variable = 'Apollo 11'; SELECT * FROM $astronauts WHERE @variable IN UNNEST(missions)", 3, 19),
        ("SET @id = 3; SELECT name FROM $planets WHERE id = @id;", 1, 1),
        ("SET @id = 3; SELECT name FROM $planets WHERE id < @id;", 2, 1),
        ("SET @id = 3; SELECT name FROM $planets WHERE id < @id OR id > @id;", 8, 1),
        ("SET @dob = '1950-01-01'; SELECT name FROM $astronauts WHERE birth_date < @dob;", 149, 1),
        ("SET @dob = '1950-01-01'; SET @mission = 'Apollo 11'; SELECT name FROM $astronauts WHERE birth_date < @dob AND @mission IN UNNEST(missions);", 3, 1),
        ("SET @pples = 'b'; SET @ngles = 90; SHOW VARIABLES LIKE '%s'", 2, 2),
        ("SET @pples = 'b'; SET @rgon = 90; SHOW VARIABLES LIKE '%gon'", 1, 2),
        ("SHOW PARAMETER enable_optimizer", 1, 2),

        ("SHOW CREATE TABLE $planets", 1, 1),
        ("SHOW CREATE TABLE $satellites", 1, 1),
        ("SHOW CREATE TABLE $astronauts", 1, 1),
        ("SHOW CREATE TABLE testdata.framed FOR '2021-03-28'", 1, 1),

        # These are queries which have been found to return the wrong result or not run correctly
        # FILTERING ON FUNCTIONS
        ("SELECT DATE(birth_date) FROM $astronauts FOR TODAY WHERE DATE(birth_date) < '1930-01-01'", 14, 1),
        # ORDER OF CLAUSES (FOR before INNER JOIN)
        ("SELECT * FROM $planets FOR '2022-03-03' INNER JOIN $satellites ON $planets.id = $satellites.planetId", 177, 28),
        # ZERO RECORD RESULT SETS
        ("SELECT * FROM $planets WHERE id = -1 ORDER BY id", 0, 20),
        ("SELECT * FROM $planets WHERE id = -1 LIMIT 10", 0, 20),
        # LEFT JOIN THEN FILTER ON NULLS
        ("SELECT * FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id WHERE $satellites.id IS NULL", 2, 28),
        ("SELECT * FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id WHERE $satellites.name IS NULL", 2, 28),
        # SORT BROKEN
        ("SELECT * FROM (SELECT * FROM $planets ORDER BY id DESC LIMIT 5) WHERE id > 7", 2, 20),
        # ORDER OF JOIN CONDITION
        ("SELECT * FROM $planets INNER JOIN $satellites ON $satellites.planetId = $planets.id", 177, 28),
        # ORDER BY QUALIFIED IDENTIFIER
        ("SELECT * FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id ORDER BY $planets.name", 179, 28),
        # NAMED SUBQUERIES
        ("SELECT P.name FROM ( SELECT * FROM $planets ) AS P", 9, 1),
        # UNNEST
        ("SELECT * FROM testdata.unnest_test CROSS JOIN UNNEST (values) AS value FOR '2000-01-01'", 15, 3),
        # FRAME HANDLING
        ("SELECT * FROM testdata.framed FOR '2021-03-28'", 100000, 1),
        ("SELECT * FROM testdata.framed FOR '2021-03-29'", 100000, 1),
        ("SELECT * FROM testdata.framed FOR DATES BETWEEN '2021-03-28' AND '2021-03-29'", 200000, 1),
        ("SELECT * FROM testdata.framed FOR DATES BETWEEN '2021-03-29' AND '2021-03-30'", 100000, 1),
        ("SELECT * FROM testdata.framed FOR DATES BETWEEN '2021-03-28' AND '2021-03-30'", 200000, 1),
        # PAGING OF DATASETS AFTER A GROUP BY [#179]
        ("SELECT * FROM (SELECT COUNT(*), column_1 FROM FAKE(5000,2) GROUP BY column_1 ORDER BY COUNT(*)) LIMIT 5", 5, 2),
        # FILTER CREATION FOR 3 OR MORE ANDED PREDICATES FAILS [#182]
        ("SELECT * FROM $astronauts WHERE name LIKE '%o%' AND `year` > 1900 AND gender ILIKE '%ale%' AND group IN (1,2,3,4,5,6)", 41, 19),
        # LIKE-ING NULL
        ("SELECT * FROM testdata.nulls WHERE username LIKE 'BBC%' FOR '2000-01-01'", 3, 5),
        ("SELECT * FROM testdata.nulls WHERE username ILIKE 'BBC%' FOR '2000-01-01'", 3, 5),
        ("SELECT * FROM testdata.nulls WHERE username NOT LIKE 'BBC%' FOR '2000-01-01'", 21, 5),
        ("SELECT * FROM testdata.nulls WHERE NOT username LIKE 'BBC%' FOR '2000-01-01'", 22, 5),
        ("SELECT * FROM testdata.nulls WHERE username NOT ILIKE 'BBC%' FOR '2000-01-01'", 21, 5),
        ("SELECT * FROM testdata.nulls WHERE username ~ 'BBC.+' FOR '2000-01-01'", 3, 5),
        ("SELECT * FROM testdata.nulls WHERE username !~ 'BBC.+' FOR '2000-01-01'", 21, 5),
        ("SELECT * FROM testdata.nulls WHERE username ~* 'bbc.+' FOR '2000-01-01'", 3, 5),
        ("SELECT * FROM testdata.nulls WHERE username !~* 'bbc.+' FOR '2000-01-01'", 21, 5),
        ("SELECT * FROM testdata.nulls WHERE username SIMILAR TO 'BBC.+' FOR '2000-01-01'", 3, 5),
        ("SELECT * FROM testdata.nulls WHERE username NOT SIMILAR TO 'BBC.+' FOR '2000-01-01'", 21, 5),
        ("SELECT * FROM testdata.nulls WHERE tweet ILIKE '%Trump%' FOR '2000-01-01'", 0, 5),
        # BYTE-ARRAY FAILS [#252]
        (b"SELECT * FROM $satellites", 177, 8),
        # DISTINCT on null values [#285]
        ("SELECT DISTINCT name FROM (VALUES (null),(null),('apple')) AS booleans (name)", 2, 1),
        # empty aggregates with other columns, loose the other columns [#281]
# [#358]       ("SELECT name, COUNT(*) FROM $astronauts WHERE name = 'Jim' GROUP BY name", 1, 2),
        # JOIN from subquery regressed [#291]
        ("SELECT * FROM (SELECT id from $planets) AS ONE LEFT JOIN (SELECT id from $planets) AS TWO ON id = id", 9, 2),
        # JOIN on UNNEST [#382]
        ("SELECT name FROM $planets INNER JOIN UNNEST(('Earth')) AS n on name = n ", 1, 1),
        ("SELECT name FROM $planets INNER JOIN UNNEST(('Earth', 'Mars')) AS n on name = n", 2, 1),
        # SELECT <literal> [#409]
        ("SELECT DATE FROM (SELECT '1980-10-20' AS DATE)", 1, 1),
        ("SELECT NUMBER FROM (SELECT 1.0 AS NUMBER)", 1, 1),
        ("SELECT VARCHAR FROM (SELECT 'varchar' AS VARCHAR)", 1, 1),
        ("SELECT BOOLEAN FROM (SELECT False AS BOOLEAN)", 1, 1),
        # EXPLAIN has two heads (found looking a [#408])
        ("EXPLAIN SELECT * FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id)", 3, 3),
        # ALIAS issues [#408]
        ("SELECT $planets.* FROM $planets INNER JOIN (SELECT id FROM $planets) AS b USING (id)", 9, 21),
        # DOUBLE QUOTED STRING [#399]
        ("SELECT birth_place['town'] FROM $astronauts WHERE birth_place['town'] = \"Rome\"", 1, 1),
        # COUNT incorrect
        ("SELECT * FROM (SELECT COUNT(*) AS bodies FROM $planets) AS space WHERE space.bodies > 5", 1, 1),
        # REGRESSION
        ("SELECT VERSION()", 1, 1),
        # COALESCE doesn't work with NaNs [#404]
        ("SELECT is_reply_to FROM testdata.formats.parquet WITH(NO_PARTITION) WHERE COALESCE(is_reply_to, -1) < 0", 74765, 1),
        # Names not found / clashes [#471]
        ("SELECT P.* FROM (SELECT * FROM $planets) AS P", 9, 20),
        ("SELECT P0.id, P1.ID, P2.ID FROM $planets AS P0 JOIN (SELECT id AS ID, name FROM $planets) AS P1 ON P0.name = P1.name JOIN (SELECT id, name AS ID FROM $planets) AS P2 ON P0.name = P2.name", 9, 3),
        ("SELECT P0.id, P1.ID FROM $planets AS P0 INNER JOIN (SELECT id, name AS ID FROM $planets) AS P1 ON P0.name = P1.name", 9, 2),
        ("SELECT P0.id, P1.ID FROM $planets AS P0 INNER JOIN (SELECT name, id AS ID FROM $planets) AS P1 USING (name)", 9, 2),
        ("SELECT P0.id, P1.ID FROM $planets AS P0 LEFT JOIN (SELECT id, name AS ID FROM $planets) AS P1 ON P0.name = P1.name", 9, 2),
        # [#475] a variation of #471
        ("SELECT P0.id, P1.ID, P2.ID FROM $planets AS P0 JOIN (SELECT CONCAT_WS(' ', list(id)) AS ID, MAX(name) AS n FROM $planets GROUP BY gravity) AS P1 ON P0.name = P1.n JOIN (SELECT CONCAT_WS(' ', list(id)) AS ID, MAX(name) AS n FROM $planets GROUP BY gravity) AS P2 ON P0.name = P2.n", 8, 3),
        # no issue number - but these two caused a headache
        # FUNCTION (AGG)
        ("SELECT CONCAT(LIST(name)) FROM $planets GROUP BY gravity", 8, 1),
        # AGG (FUNCTION)
        ("SELECT SUM(IIF(year < 1970, 1, 0)), MAX(year) FROM $astronauts", 1, 2),
        # [#527] variables referenced in subqueries
        ("SET @v = 1; SELECT * FROM (SELECT @v);", 1, 1),
    ]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns", STATEMENTS)
def test_sql_battery(statement, rows, columns):
    """
    Test an battery of statements
    """

    opteryx.register_store("tests", DiskConnector)

    conn = opteryx.connect()
    cursor = conn.cursor()
    cursor.execute(statement)

    cursor._results = list(cursor._results)
    if cursor._results:
        result = pyarrow.concat_tables(cursor._results, promote=True)
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
    for index, (statement, rows, cols) in enumerate(STATEMENTS):
        print(f"{(index + 1):04}", statement)
        test_sql_battery(statement, rows, cols)

    print("✅ okay")
