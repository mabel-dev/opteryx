"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
 >  Shape Checking
    Results Checking
    Compare to DuckDB

This is the main SQL Battery set, others exist for testing specific features (like
reading different file types) but this is the main set of tests for if the Engine
can respond to a query.

This tests that the shape of the response is as expected: the right number of columns,
the right number of rows and, if appropriate, the right exception is thrown.

Some test blocks have labels as to what the block is generally testing, even fewer
tests have comments as to why they exist (usually if the test was written after a
bug-fix).

We have three in-memory tables, one of natural satellite data, one of planet data and
one of astronaut data. These are both small to allow us to test the SQL engine quickly
and is guaranteed to be available whereever the tests are run.

These are supplimented with a few physical tables to test conditions unable to be
tested with the in-memory tables.

We test the shape in this battery because if the shape isn't right, the response isn't
going to be right, and testing shape of an in-memory dataset is quick, so we can do 
bulk testing of 100s of queries in a few seconds and have some confidence the changes
have not broken existing functionality. Note that testing the shape doesn't mean the
response is right.

These tests only test the shape of the response, more specific tests would be needed to
test the body of the response.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from pyarrow.lib import ArrowInvalid
import pytest

import opteryx

# from opteryx.connectors import AwsS3Connector, DiskConnector
from opteryx.exceptions import (
    AmbiguousIdentifierError,
    ColumnNotFoundError,
    DatasetNotFoundError,
    EmptyResultSetError,
    InvalidTemporalRangeFilterError,
    MissingSqlStatement,
    ProgrammingError,
    SqlError,
    UnexpectedDatasetReferenceError,
    UnsupportedSyntaxError,
)
from opteryx.utils.formatter import format_sql

# fmt:off
STATEMENTS = [
        # Are the datasets the shape we expect?
        ("SELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $planets", 9, 20, None),
        ("SELECT * FROM $astronauts", 357, 19, None),

        # Does the error tester work
        ("THIS IS NOT VALID SQL", None, None, SqlError),

        # Now the actual tests
        ("SELECT * FROM $satellites;", 177, 8, None),
        ("SELECT * FROM $satellites\n;", 177, 8, None),
        ("select * from $satellites", 177, 8, None),
        ("Select * From $satellites", 177, 8, None),
        ("SELECT   *   FROM   $satellites", 177, 8, None),
        ("SELECT\n\t*\n  FROM\n\t$satellites", 177, 8, None),
        ("\n\n\n\tSELECT * FROM $satellites", 177, 8, None),
        ("SELECT $satellites.* FROM $satellites", 177, 8, None),
        ("SELECT s.* FROM $satellites AS s", 177, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (name = 'Calypso')", 1, 8, None),
        ("SELECT * FROM $satellites WHERE NOT name = 'Calypso'", 176, 8, None),
        ("SELECT * FROM $satellites WHERE NOT (name = 'Calypso')", 176, 8, None),
        ("SELECT * FROM $satellites WHERE `name` = 'Calypso'", 1, 8, None),
        ("select * from $satellites where name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name <> 'Calypso'", 176, 8, None),
        ("SELECT * FROM $satellites WHERE name = '********'", 0, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE '_a_y_s_'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE 'Cal%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name like 'Cal%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE `name` = 'Calypso'", 1, 8, None),
        ("SELECT * FROM `$satellites` WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM `$satellites` WHERE `name` = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WITH (NO_CACHE)", 177, 8, None),

        # Do we handle comments 
        ("/* comment */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites /* comment */ WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites /* WHERE name = 'Calypso' */", 177, 8, None),
        ("SELECT * FROM $satellites -- WHERE name = 'Calypso'", 177, 8, None),
        ("SELECT * FROM $satellites -- WHERE name = 'Calypso' \n WHERE id = 1", 1, 8, None),
        ("-- comment\nSELECT * --comment\n FROM $satellites -- WHERE name = 'Calypso' \n WHERE id = 1", 1, 8, None),
        ("/* comment */ SELECT * FROM $satellites /* comment */  WHERE name = 'Calypso'", 1, 8, None),
        ("/* comment */ SELECT * FROM $satellites /* comment */  WHERE name = 'Calypso'  /* comment */ ", 1, 8, None),
        ("/* comment --inner */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites -- comment\n FOR TODAY", 177, 8, None),
        ("SELECT * FROM $satellites /* comment */ FOR TODAY /* comment */", 177, 8, None),

        ("SELECT name, id, planetId FROM $satellites", 177, 3, None),
        ("SELECT name, name FROM $satellites", 177, 1, SqlError),  # V2 breaking
        ("SELECT name, id, name, id FROM $satellites", 177, 2, SqlError),  # V2 breaking

        ("SELECT DISTINCT name FROM $astronauts", 357, 1, None),
        ("SELECT DISTINCT * FROM $astronauts", 357, 19, None),
        ("SELECT DISTINCT birth_date FROM $astronauts", 348, 1, None),
        ("SELECT DISTINCT birth_place FROM $astronauts", 272, 1, None),
        ("SELECT DISTINCT death_date FROM $astronauts", 39, 1, None),
        ("SELECT DISTINCT missions FROM $astronauts", 305, 1, None),
        ("SELECT DISTINCT group FROM $astronauts", 21, 1, None),
        ("SELECT DISTINCT name, birth_date, missions, birth_place, group FROM $astronauts", 357, 5, None),

        ("SELECT name as Name FROM $satellites", 177, 1, None),
        ("SELECT name as Name, id as Identifier FROM $satellites", 177, 2, None),
        ("SELECT name as NAME FROM $satellites WHERE name = 'Calypso'", 1, 1, None),
        ("SELECT name as NAME FROM $satellites GROUP BY name", 177, 1, None),

        # Test infix calculations
        ("SELECT * FROM $satellites WHERE id = 5", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Cal' || 'ypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'C' || 'a' || 'l' || 'y' || 'p' || 's' || 'o'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 5 * 1 AND name = 'Europa'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 10 / 2 AND name = 'Europa'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 3 + 2 AND name = 'Europa'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id + 2 = 7 AND name = 'Europa'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 15 % 10 AND name = 'Europa'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 15 DIV 4", 1, 8, None),

        ("SELECT * FROM $satellites WHERE magnitude = 5.29", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 5 AND magnitude = 5.29", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 5 AND magnitude = 1", 0, 8, None),  # this bales early
        ("SELECT * FROM $satellites WHERE id = 5 AND name = 'Europa'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5) AND (name = 'Europa')", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 5 OR name = 'Europa'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 5 OR name = 'Moon'", 2, 8, None),
        ("SELECT * FROM $satellites WHERE id < 3 AND (name = 'Europa' OR name = 'Moon')", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id BETWEEN 5 AND 8", 4, 8, None),
        ("SELECT * FROM $satellites WHERE id NOT BETWEEN 5 AND 8", 173, 8, None),
        ("SELECT * FROM $satellites WHERE ((id BETWEEN 5 AND 10) AND (id BETWEEN 10 AND 12)) OR name = 'Moon'", 2, 8, None),
        ("SELECT * FROM $satellites WHERE id BETWEEN 5 AND 8 OR name = 'Moon'", 5, 8, None),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8)", 4, 8, None),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8) AND name = 'Europa'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id IN (5,6,7,8)) AND name = 'Europa'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id IN (5,6,7,8) AND name = 'Europa')", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id IN (5,6,7,8) OR name = 'Moon'", 5, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR id = 6 OR id = 7 OR id = 8) AND name = 'Europa'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 6 OR id = 7 OR id = 8) OR name = 'Europa'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE id = 5 OR id = 6 OR id = 7 OR id = 8 OR name = 'Moon'", 5, 8, None),
        ("SELECT * FROM $satellites WHERE planetId = id", 1, 8, None),
        ("SELECT * FROM $satellites WHERE planetId > 8", 5, 8, None),
        ("SELECT * FROM $satellites WHERE planetId >= 8", 19, 8, None),
        ("SELECT * FROM $satellites WHERE planetId < 5", 3, 8, None),
        ("SELECT * FROM $satellites WHERE planetId <= 5", 70, 8, None),
        ("SELECT * FROM $satellites WHERE planetId <> 5", 110, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE 'C%'", 12, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE 'M__n'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE '%c%'", 11, 8, None),
        ("SELECT * FROM $satellites WHERE name ILIKE '%c%'", 23, 8, None),
        ("SELECT * FROM $satellites WHERE name NOT LIKE '%c%'", 166, 8, None),
        ("SELECT * FROM $satellites WHERE name NOT ILIKE '%c%'", 154, 8, None),
        ("SELECT * FROM $satellites WHERE name ~ '^C.'", 12, 8, None),
        ("SELECT * FROM $satellites WHERE name SIMILAR TO '^C.'", 12, 8, None),
        ("SELECT * FROM $satellites WHERE name !~ '^C.'", 165, 8, None),
        ("SELECT * FROM $satellites WHERE name NOT SIMILAR TO '^C.'", 165, 8, None),
        ("SELECT * FROM $satellites WHERE name ~* '^c.'", 12, 8, None),
        ("SELECT * FROM $satellites WHERE name !~* '^c.'", 165, 8, None),

        ("SELECT COUNT(*) FROM $satellites", 1, 1, None),
        ("SELECT count(*) FROM $satellites", 1, 1, None),
        ("SELECT COUNT (*) FROM $satellites", 1, 1, None),
        ("SELECT\nCOUNT\n(*)\nFROM\n$satellites", 1, 1, None),
        ("SELECT Count(*) FROM $satellites", 1, 1, None),
        ("SELECT Count(*) FROM $satellites WHERE name = 'sputnik'", 1, 1, None),
        ("SELECT COUNT(name) FROM $satellites", 1, 1, None),
        ("SELECT COUNT(*) FROM $satellites GROUP BY name", 177, 1, None),
        ("SELECT COUNT(*) FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT COUNT(*) FROM $satellites GROUP\nBY planetId", 7, 1, None),
        ("SELECT COUNT(*) FROM $satellites GROUP     BY planetId", 7, 1, None),
        ("SELECT COUNT(*), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT COUNT(*), planetId FROM $satellites WHERE planetId < 6 GROUP BY planetId", 3, 2, None),        
        ("SELECT COUNT(*), planetId FROM $satellites WHERE planetId <= 6 GROUP BY planetId", 4, 2, None),
        ("SELECT COUNT(*), planetId FROM $satellites WHERE name LIKE 'Cal%' GROUP BY planetId", 3, 2, None),
        
        ("SELECT DISTINCT planetId FROM $satellites", 7, 1, None),
        ("SELECT * FROM $satellites LIMIT 50", 50, 8, None),
        ("SELECT * FROM $satellites LIMIT 0", 0, 8, None),
        ("SELECT * FROM $satellites OFFSET 150", 27, 8, None),
        ("SELECT * FROM $satellites LIMIT 50 OFFSET 150", 27, 8, None),
        ("SELECT * FROM $satellites LIMIT 50 OFFSET 170", 7, 8, None),
        ("SELECT * FROM $satellites ORDER BY name", 177, 8, None),
        ("SELECT * FROM $satellites ORDER BY 1", 177, 8, None),
        ("SELECT * FROM $satellites ORDER BY 1 DESC", 177, 8, None),
        ("SELECT * FROM $satellites ORDER BY 2", 177, 8, None),
        ("SELECT * FROM $satellites ORDER BY 1, 2", 177, 8, None),
        ("SELECT * FROM $satellites ORDER BY 1 ASC", 177, 8, None),
        ("SELECT * FROM $satellites ORDER BY RANDOM()", 177, 8, None),

        ("SELECT MAX(planetId) FROM $satellites", 1, 1, None),
        ("SELECT MIN(planetId) FROM $satellites", 1, 1, None),
        ("SELECT SUM(planetId) FROM $satellites", 1, 1, None),
        ("SELECT MAX(id), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT MIN(id), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT SUM(id), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT MIN(id), MAX(id), SUM(planetId), planetId FROM $satellites GROUP BY planetId", 7, 4, None),
        ("SELECT planetId, LIST(name) FROM $satellites GROUP BY planetId", 7, 2, None),

        ("SELECT planetId FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT BOOLEAN(planetId - 3) FROM $satellites GROUP BY BOOLEAN(planetId - 3)", 2, 1, None),
        ("SELECT VARCHAR(planetId) FROM $satellites GROUP BY VARCHAR(planetId)", 7, 1, None),
        ("SELECT STR(planetId) FROM $satellites GROUP BY STR(planetId)", 7, 1, None),
        ("SELECT COUNT(*) FROM $satellites GROUP BY TIMESTAMP('2022-01-0' || VARCHAR(planetId))", 7, 1, None),
        ("SELECT NUMERIC(planetId) FROM $satellites GROUP BY NUMERIC(planetId)", 7, 1, None),
        ("SELECT INT(planetId) FROM $satellites GROUP BY INT(planetId)", 7, 1, None),
        ("SELECT INTEGER(planetId) FROM $satellites GROUP BY INTEGER(planetId)", 7, 1, None),
        ("SELECT FLOAT(planetId) FROM $satellites GROUP BY FLOAT(planetId)", 7, 1, None),
        ("SELECT CAST(planetId AS BOOLEAN) FROM $satellites", 177, 1, None),
        ("SELECT CAST(planetId AS VARCHAR) FROM $satellites", 177, 1, None),
        ("SELECT CAST('2022-01-0' || VARCHAR(planetId) AS TIMESTAMP) FROM $satellites", 177, 1, None),
        ("SELECT CAST(planetId AS NUMERIC) FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS BOOLEAN) FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS VARCHAR) FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS TIMESTAMP) FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS NUMERIC) FROM $satellites", 177, 1, None),
        ("SELECT * FROM $planets WHERE id = GET(STRUCT('{\"a\":1,\"b\":\"c\"}'), 'a')", 1, 20, None),

        ("SELECT PI()", 1, 1, None),
        ("SELECT E()", 1, 1, None),
        ("SELECT PHI()", 1, 1, None),
        ("SELECT GET(name, 1) FROM $satellites GROUP BY planetId, GET(name, 1)", 56, 1, None),
        ("SELECT COUNT(*), ROUND(magnitude) FROM $satellites group by ROUND(magnitude)", 22, 2, None),
        ("SELECT ROUND(magnitude) FROM $satellites group by ROUND(magnitude)", 22, 1, None),
        ("SELECT ROUND(magnitude, 1) FROM $satellites group by ROUND(magnitude, 1)", 88, 1, None),
        ("SELECT VARCHAR(planetId), COUNT(*) FROM $satellites GROUP BY 1", 7, 2, None),
        ("SELECT LEFT(name, 1), COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 21, 2, None),
        ("SELECT LEFT(name, 2) as le, COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 87, 2, None),
        ("SELECT RIGHT(name, 10), COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 177, 2, None),
        ("SELECT RIGHT(name, 2) as le, COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 91, 2, None),
        ("SELECT round(magnitude) FROM $satellites group by round(magnitude)", 22, 1, None),
        ("SELECT upper(name) as NAME, id as Identifier FROM $satellites", 177, 2, None),
        ("SELECT upper(name), lower(name), id as Identifier FROM $satellites", 177, 3, None),

        ("SELECT planetId, Count(*) FROM $satellites group by planetId having count(*) > 5", 4, 2, None),
        ("SELECT planetId, min(magnitude) FROM $satellites group by planetId having min(magnitude) > 5", 5, 2, None),
        ("SELECT planetId, min(magnitude) FROM $satellites group by planetId having min(magnitude) > 5 limit 2 offset 1", 2, 2, None),
        ("SELECT planetId, count(*) FROM $satellites group by planetId order by count(*) desc", 7, 2, None),
        ("SELECT planetId, count(*) FROM $satellites group by planetId order by planetId desc", 7, 2, None),
        ("SELECT planetId, count(*) FROM $satellites group by planetId order by planetId, count(*) desc", 7, 2, None),
        ("SELECT planetId, count(*) FROM $satellites group by planetId order by count(*), planetId desc", 7, 2, None),

        ("SELECT * FROM $satellites order by name", 177, 8, None),
        ("SELECT * FROM $satellites order by name desc", 177, 8, None),
        ("SELECT name FROM $satellites order by name", 177, 1, None),
        ("SELECT * FROM $satellites order by magnitude, name", 177, 8, None),

        ("SELECT planetId as pid FROM $satellites", 177, 1, None),
        ("SELECT planetId as pid, round(magnitude) FROM $satellites", 177, 2, None),
        ("SELECT planetId as pid, round(magnitude) as minmag FROM $satellites", 177, 2, None),
        ("SELECT planetId as pid, round(magnitude) as roundmag FROM $satellites", 177, 2, None),

        ("SELECT GET(birth_place, 'town') FROM $astronauts", 357, 1, None),
        ("SELECT GET(missions, 0) FROM $astronauts", 357, 1, None),
        ("SELECT GET(birth_place, 'town') FROM $astronauts WHERE GET(birth_place, 'town') = 'Warsaw'", 1, 1, None),
        ("SELECT COUNT(*), GET(birth_place, 'town') FROM $astronauts GROUP BY GET(birth_place, 'town')", 264, 2, None),
        ("SELECT birth_place['town'] FROM $astronauts", 357, 1, None),
        ("SELECT missions[0] FROM $astronauts", 357, 1, None),

        ("SELECT birth_place['town'] FROM $astronauts WHERE birth_place['town'] = 'Warsaw'", 1, 1, None),
        ("SELECT birth_place['town'] AS TOWN FROM $astronauts WHERE birth_place['town'] = 'Warsaw'", 1, 1, None),
        ("SELECT COUNT(*), birth_place['town'] FROM $astronauts GROUP BY birth_place['town']", 264, 2, None),
        ('SELECT LENGTH(missions) FROM $astronauts', 357, 1, None),
        ('SELECT LENGTH(missions) FROM $astronauts WHERE LENGTH(missions) > 6', 2, 1, None),

        ("SELECT birth_date FROM $astronauts", 357, 1, None),
        ("SELECT YEAR(birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT YEAR(birth_date) FROM $astronauts WHERE YEAR(birth_date) < 1930", 14, 1, None),

        ("SELECT RANDOM() FROM $planets", 9, 1, None),
        ("SELECT NOW() FROM $planets", 9, 1, None),
        ("SELECT TODAY() FROM $planets", 9, 1, None),
        ("SELECT CURRENT_DATE", 1, 1, None),
        ("SELECT CURRENT_DATE()", 1, 1, None),
        ("SELECT CURRENT_TIME", 1, 1, None),
        ("SELECT CURRENT_TIME()", 1, 1, None),
        ("SELECT YEAR(birth_date), COUNT(*) FROM $astronauts GROUP BY YEAR(birth_date)", 54, 2, None),
        ("SELECT MONTH(birth_date), COUNT(*) FROM $astronauts GROUP BY MONTH(birth_date)", 12, 2, None),

        ("SELECT DATE_FORMAT(birth_date, '%d-%Y') FROM $astronauts", 357, 1, None),
        ("SELECT DATE_FORMAT(birth_date, 'dddd') FROM $astronauts", 357, 1, None),
        ("SELECT DATE_FORMAT(death_date, '%Y') FROM $astronauts", 357, 1, None),

        ("SELECT count(*), VARCHAR(year) FROM $astronauts GROUP BY VARCHAR(year)", 21, 2, None),
        ("SELECT count(*), STRING(year) FROM $astronauts GROUP BY STRING(year)", 21, 2, None),
        ("SELECT count(*), STR(year) FROM $astronauts GROUP BY STR(year)", 21, 2, None),
        ("SELECT count(*), CAST(year AS VARCHAR) FROM $astronauts GROUP BY CAST(year AS VARCHAR)", 21, 2, None),

        ("SELECT RANDOM()", 1, 1, None),
        ("SELECT RAND()", 1, 1, None),
        ("SELECT NOW()", 1, 1, None),
        ("SELECT NOW() from $planets", 9, 1, None),
        ("SELECT TODAY()", 1, 1, None),
        ("SELECT HASH('hello')", 1, 1, None),
        ("SELECT MD5('hello')", 1, 1, None),
        ("SELECT SHA1('hello')", 1, 1, None),
        ("SELECT SHA224('hello')", 1, 1, None),
        ("SELECT SHA256('hello')", 1, 1, None),
        ("SELECT SHA384('hello')", 1, 1, None),
        ("SELECT SHA512('hello')", 1, 1, None),
        ("SELECT UPPER('upper'), LOWER('LOWER')", 1, 2, None),
        ("SELECT POWER(2, 10)", 1, 1, None),
        ("SELECT LN(10)", 1, 1, None),
        ("SELECT LOG10(10)", 1, 1, None),
        ("SELECT LOG2(10)", 1, 1, None),
        ("SELECT LOG(10, 4)", 1, 1, None),

        ("SELECT HASH(name), name from $astronauts", 357, 2, None),
        ("SELECT HASH(death_date), death_date from $astronauts", 357, 2, None),
        ("SELECT HASH(birth_place), birth_place from $astronauts", 357, 2, None),
        ("SELECT HASH(missions), missions from $astronauts", 357, 2, None),

        # Test Aliases
        ("SELECT planet_id FROM $satellites", 177, 1, None),
        ("SELECT escape_velocity, gravity, orbitalPeriod FROM $planets", 9, 3, None),

        ("SELECT * FROM (VALUES ('High', 3),('Medium', 2),('Low', 1)) AS ratings(name, rating)", 3, 2, None),
        ("SELECT * FROM (VALUES ('High', 3),('Medium', 2),('Low', 1)) AS ratings(name, rating) WHERE rating = 3", 1, 2, None),

        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred')) AS element", 8, 1, None),
        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred')) AS element WHERE element LIKE '%e%'", 2, 1, None),
        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred'))", 8, 1, None),
        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred')) WHERE unnest LIKE '%e%'", 2, 1, None),

        ("SELECT * FROM generate_series(1, 10)", 10, 1, None),
        ("SELECT * FROM generate_series(-10,10)", 21, 1, None),
        ("SELECT * FROM generate_series(2,10,2)", 5, 1, None),
        ("SELECT * FROM generate_series(0.5,10,0.5)", 20, 1, None),
        ("SELECT * FROM generate_series(2,11,2)", 5, 1, None),
        ("SELECT * FROM generate_series(1, 10, 0.5)", 19, 1, None),
        ("SELECT * FROM generate_series(0.1, 0.2, 10)", 1, 1, None),
        ("SELECT * FROM generate_series(0, 5, 1.1)", 5, 1, None),
        ("SELECT * FROM generate_series(2,10,2) AS nums", 5, 1, None),
        ("SELECT * FROM generate_series(2,10,2) WHERE generate_series > 5", 3, 1, None),
        ("SELECT * FROM generate_series(2,10,2) AS nums WHERE nums < 5", 2, 1, None),
        ("SELECT * FROM generate_series(2) WITH (NO_CACHE)", 2, 1, SqlError),
        ("SELECT * FROM generate_series('192.168.0.0/24') WITH (NO_CACHE)", 2, 1, SqlError),

        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 month')", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 mon')", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1mon')", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1mo')", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1mth')", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 months')", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 day')", 365, 1, None),
        ("SELECT * FROM generate_series('2020-01-01', '2020-12-31', '1day')", 366, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '7 days')", 53, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-01-02', '1 hour')", 25, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-01-01 23:59', '1 hour')", 24, 1, None),
        ("SELECT * FROM generate_series('2022-01-01 12:00', '2022-01-01 23:59', '1 hour')", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01 12:00', '2022-01-01 12:15', '1 minute')", 16, 1, None),
        ("SELECT * FROM generate_series('2022-01-01 12:00', '2022-01-01 12:15', '1m30s')", 11, 1, None),
        ("SELECT * FROM generate_series(1,10) LEFT JOIN $planets ON id = generate_series", 10, 21, None),
        ("SELECT * FROM GENERATE_SERIES(5, 10) AS PID LEFT JOIN $planets ON id = PID", 6, 21, None),
        ("SELECT * FROM generate_series(1,5) JOIN $planets ON id = generate_series", 5, 21, None),
        ("SELECT * FROM (SELECT * FROM generate_series(1,10,2) AS gs) INNER JOIN $planets on gs = id", 5, 21, None),

        ("SELECT * FROM 'testdata/flat/formats/arrow/tweets.arrow'", 100000, 13, None),
        ("SELECT * FROM 'testdata/flat/../flat/formats/arrow/tweets.arrow'", None, None, DatasetNotFoundError),  # don't allow traversal

        ("SELECT * FROM testdata.partitioned.dated FOR '2020-02-03' WITH (NO_CACHE)", 25, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR '2020-02-03'", 25, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR '2020-02-04'", 25, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR DATES BETWEEN '2020-02-01' AND '2020-02-28'", 50, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR '2020-02-03' OFFSET 1", 24, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR DATES BETWEEN '2020-02-01' AND '2020-02-28' OFFSET 1", 49, 8, None),
        ("SELECT * FROM $satellites FOR YESTERDAY ORDER BY planetId OFFSET 10", 167, 8, None),

        ("SELECT * FROM testdata.partitioned.segmented FOR '2020-02-03'", 25, 8, None),
        ("SELECT * FROM $planets FOR '1730-01-01'", 6, 20, None),
        ("SELECT * FROM $planets FOR '1830-01-01'", 7, 20, None),
        ("SELECT * FROM $planets FOR '1930-01-01'", 8, 20, None),
        ("SELECT * FROM $planets FOR '2030-01-01'", 9, 20, None),
        ("SELECT * FROM $planets AS planets FOR '1730-01-01'", 6, 20, None),
        ("SELECT * FROM $planets AS p FOR '1830-01-01'", 7, 20, None),
        ("SELECT * FROM $planets AS pppp FOR '1930-01-01'", 8, 20, None),
        ("SELECT * FROM $planets AS P FOR '2030-01-01'", 9, 20, None),
        ("SELECT * FROM (SELECT * FROM $planets AS D) AS P FOR '2030-01-01'", 9, 20, None),
        ("SELECT * FROM $planets AS P FOR '1699-01-01' INNER JOIN $satellites FOR '2030-01-01' ON id = planetId;", 131, 28, None),

        ("SELECT * FROM $astronauts WHERE death_date IS NULL", 305, 19, None),
        ("SELECT * FROM $astronauts WHERE death_date IS NOT NULL", 52, 19, None),
        ("SELECT * FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified IS TRUE", 711, 13, None),
        ("SELECT * FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified IS FALSE", 99289, 13, None),
        ("SELECT * FROM testdata.flat.formats.csv WITH(NO_PARTITION) WHERE user_verified IS TRUE", 134, 10, None),
        ("SELECT * FROM testdata.flat.formats.tsv WITH(NO_PARTITION) WHERE user_verified IS TRUE", 134, 10, None),

        ("SELECT * FROM testdata.flat.formats.parquet WITH(NO_PARTITION, PARALLEL_READ)", 100000, 13, None),

        ("SELECT * FROM $satellites FOR DATES IN LAST_MONTH ORDER BY planetId OFFSET 10", 167, 8, None),
        ("SELECT * FROM $satellites FOR DATES IN PREVIOUS_MONTH ORDER BY planetId OFFSET 10", 167, 8, None),
        ("SELECT * FROM $satellites FOR DATES IN THIS_MONTH ORDER BY planetId OFFSET 10", 167, 8, None),

        ("SELECT missions FROM $astronauts WHERE LIST_CONTAINS(missions, 'Apollo 8')", 3, 1, None),
        ("SELECT missions FROM $astronauts WHERE LIST_CONTAINS_ANY(missions, ('Apollo 8', 'Apollo 13'))", 5, 1, None),
        ("SELECT missions FROM $astronauts WHERE LIST_CONTAINS_ALL(missions, ('Apollo 8', 'Gemini 7'))", 2, 1, None),
        ("SELECT missions FROM $astronauts WHERE LIST_CONTAINS_ALL(missions, ('Gemini 7', 'Apollo 8'))", 2, 1, None),

        ("SELECT * FROM $satellites WHERE planetId IN (SELECT id FROM $planets WHERE name = 'Earth')", 1, 8, None),
        ("SELECT * FROM $planets WHERE id NOT IN (SELECT DISTINCT planetId FROM $satellites)", 2, 20, None),
        ("SELECT name FROM $planets WHERE id IN (SELECT * FROM UNNEST((1,2,3)) as id)", 3, 1, None),
        ("SELECT count(planetId) FROM (SELECT DISTINCT planetId FROM $satellites)", 1, 1, None),
        ("SELECT COUNT(*) FROM (SELECT planetId FROM $satellites WHERE planetId < 7) GROUP BY planetId", 4, 1, None),

        ("EXPLAIN SELECT * FROM $satellites", 1, 3, None),
        ("EXPLAIN SELECT * FROM $satellites WHERE id = 8", 3, 3, None),
        ("SET enable_morsel_defragmentation = false; EXPLAIN SELECT * FROM $satellites WHERE id = 8", 2, 3, None),
        ("SET enable_optimizer = false; EXPLAIN SELECT * FROM $satellites WHERE id = 8", 2, 3, None),
        ("SET enable_optimizer = true; EXPLAIN SELECT * FROM $satellites WHERE id = 8 AND id = 7", 5, 3, None),
        ("SET enable_optimizer = false; EXPLAIN SELECT * FROM $satellites WHERE id = 8 AND id = 7", 2, 3, None),
        ("SET enable_optimizer = false; EXPLAIN SELECT * FROM $planets ORDER BY id LIMIT 5", 3, 3, None),
        ("SET enable_optimizer = true; EXPLAIN SELECT * FROM $planets ORDER BY id LIMIT 5", 2, 3, None),
        ("EXPLAIN SELECT * FROM $planets ORDER BY id LIMIT 5", 2, 3, None),
        ("SELECT name, id FROM $planets ORDER BY id LIMIT 5", 5, 2, None),
        ("SELECT name, id FROM $planets ORDER BY id LIMIT 100", 9, 2, None),

        ("SHOW COLUMNS FROM $satellites", 8, 2, None),
        ("SHOW FULL COLUMNS FROM $satellites", 8, 6, None),
        ("SHOW EXTENDED COLUMNS FROM $satellites", 8, 10, None),
        ("SHOW EXTENDED COLUMNS FROM $planets", 20, 10, None),
        ("SHOW EXTENDED COLUMNS FROM $astronauts", 19, 10, None),
        ("SHOW COLUMNS FROM $satellites LIKE '%d'", 2, 2, None),
        ("SHOW COLUMNS FROM testdata.partitioned.dated FOR '2020-02-03'", 8, 2, None),

        ("SELECT * FROM $satellites CROSS JOIN $astronauts", 63189, 27, None),
        ("SELECT * FROM $satellites WITH (NO_CACHE) CROSS JOIN $astronauts WITH (NO_CACHE)", 63189, 27, None),
        ("SELECT * FROM $satellites, $planets", 1593, 28, None),
        ("SELECT * FROM $satellites INNER JOIN $planets USING (id)", 9, 28, None),
        ("SELECT * FROM $satellites WITH (NO_CACHE) INNER JOIN $planets USING (id)", 9, 28, None),
        ("SELECT * FROM $satellites WITH (NO_CACHE) INNER JOIN $planets WITH (NO_CACHE) USING (id)", 9, 28, None),
        ("SELECT * FROM $satellites INNER JOIN $planets WITH (NO_CACHE) USING (id)", 9, 28, None),
        ("SELECT * FROM $satellites JOIN $planets USING (id)", 9, 28, None),
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(missions) AS mission WHERE mission = 'Apollo 11'", 3, 20, None),
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(missions)", 869, 20, None),
        ("SELECT * FROM $planets INNER JOIN $satellites ON $planets.id = $satellites.planetId", 177, 28, None),
        ("SELECT DISTINCT planetId FROM $satellites LEFT OUTER JOIN $planets ON $satellites.planetId = $planets.id", 7, 1, None),
        ("SELECT DISTINCT planetId FROM $satellites LEFT JOIN $planets ON $satellites.planetId = $planets.id", 7, 1, None),
        ("SELECT DISTINCT $planets.id, $satellites.id FROM $planets LEFT OUTER JOIN $satellites ON $satellites.planetId = $planets.id", 179, 2, None),
        ("SELECT DISTINCT $planets.id, $satellites.id FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id", 179, 2, None),
        ("SELECT planetId FROM $satellites LEFT JOIN $planets ON $satellites.planetId = $planets.id", 177, 1, None),
        ("SELECT * FROM $planets LEFT JOIN $planets USING(id)", 9, 40, None),
        ("SELECT * FROM $planets LEFT OUTER JOIN $planets USING(id)", 9, 40, None),
        ("SELECT * FROM $planets LEFT JOIN $planets FOR TODAY USING(id)", 9, 40, None),
        ("SELECT * FROM $planets LEFT JOIN $planets USING(id, name)", 9, 40, None),
        ("SELECT * FROM $planets INNER JOIN $planets ON id = id AND name = name", 9, 40, None),

        ("SELECT DISTINCT planetId FROM $satellites RIGHT OUTER JOIN $planets ON $satellites.planetId = $planets.id", 8, 1, None),
        ("SELECT DISTINCT planetId FROM $satellites RIGHT JOIN $planets ON $satellites.planetId = $planets.id", 8, 1, None),
        ("SELECT planetId FROM $satellites RIGHT JOIN $planets ON $satellites.planetId = $planets.id", 179, 1, None),
        ("SELECT DISTINCT planetId FROM $satellites FULL OUTER JOIN $planets ON $satellites.planetId = $planets.id", 8, 1, None),
        ("SELECT DISTINCT planetId FROM $satellites FULL JOIN $planets ON $satellites.planetId = $planets.id", 8, 1, None),
        ("SELECT planetId FROM $satellites FULL JOIN $planets ON $satellites.planetId = $planets.id", 179, 1, None),

        ("SELECT pid FROM ( SELECT id AS pid FROM $planets) WHERE pid > 5", 4, 1, None),
        ("SELECT * FROM ( SELECT id AS pid FROM $planets) WHERE pid > 5", 4, 1, None),
        ("SELECT * FROM ( SELECT COUNT(planetId) AS moons, planetId FROM $satellites GROUP BY planetId ) WHERE moons > 10", 4, 2, None),

        ("SELECT * FROM $planets WHERE id = -1", 0, 20, None),
        ("SELECT COUNT(*) FROM (SELECT DISTINCT a FROM $astronauts CROSS JOIN UNNEST(alma_mater) AS a ORDER BY a)", 1, 1, None),

        ("SELECT a.id, b.id, c.id FROM $planets AS a INNER JOIN $planets AS b ON a.id = b.id INNER JOIN $planets AS c ON c.id = b.id", 9, 3, None),
        ("SELECT * FROM $planets AS a INNER JOIN $planets AS b ON a.id = b.id RIGHT OUTER JOIN $satellites AS c ON c.planetId = b.id", 177, 48, None),

        ("SELECT $planets.* FROM $satellites INNER JOIN $planets USING (id)", 9, 20, None),
        ("SELECT $satellites.* FROM $satellites INNER JOIN $planets USING (id)", 9, 8, None),
        ("SELECT $satellites.* FROM $satellites INNER JOIN $planets ON $planets.id = $satellites.id", 9, 8, None),
        ("SELECT $planets.* FROM $satellites INNER JOIN $planets AS p USING (id)", 9, 20, None),
        ("SELECT p.* FROM $satellites INNER JOIN $planets AS p USING (id)", 9, 20, None),
        ("SELECT s.* FROM $satellites AS s INNER JOIN $planets USING (id)", 9, 8, None),
        ("SELECT $satellites.* FROM $satellites AS s INNER JOIN $planets USING (id)", 9, 8, None),
        ("SELECT $satellites.* FROM $satellites AS s INNER JOIN $planets AS p USING (id)", 9, 8, None),
        ("SELECT s.* FROM $satellites AS s INNER JOIN $planets AS p USING (id)", 9, 8, None),

        ("SELECT DATE_TRUNC('month', birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT DISTINCT * FROM (SELECT DATE_TRUNC('year', birth_date) AS BIRTH_YEAR FROM $astronauts)", 54, 1, None),
        ("SELECT DISTINCT * FROM (SELECT DATE_TRUNC('month', birth_date) AS BIRTH_YEAR_MONTH FROM $astronauts)", 247, 1, None),
        ("SELECT time_bucket(birth_date, 10, 'year') AS decade, count(*) from $astronauts GROUP BY time_bucket(birth_date, 10, 'year')", 6, 2, None),
        ("SELECT time_bucket(birth_date, 6, 'month') AS half, count(*) from $astronauts GROUP BY time_bucket(birth_date, 6, 'month')", 97, 2, None),
    
        ("SELECT graduate_major, undergraduate_major FROM $astronauts WHERE COALESCE(graduate_major, undergraduate_major, 'high school') = 'high school'", 4, 2, None),
        ("SELECT graduate_major, undergraduate_major FROM $astronauts WHERE COALESCE(graduate_major, undergraduate_major) = 'Aeronautical Engineering'", 41, 2, None),
        ("SELECT COALESCE(death_date, '2030-01-01') FROM $astronauts", 357, 1, None),
        ("SELECT * FROM $astronauts WHERE COALESCE(death_date, '2030-01-01') < '2000-01-01'", 30, 19, None),

        ("SELECT SEARCH(name, 'al'), name FROM $satellites", 177, 2, None),
        ("SELECT name FROM $satellites WHERE SEARCH(name, 'al')", 18, 1, None),
        ("SELECT SEARCH(missions, 'Apollo 11'), missions FROM $astronauts", 357, 2, None),
        ("SELECT name FROM $astronauts WHERE SEARCH(missions, 'Apollo 11')", 3, 1, None),
        ("SELECT name, SEARCH(birth_place, 'Italy') FROM $astronauts", 357, 2, None),
        ("SELECT name, birth_place FROM $astronauts WHERE SEARCH(birth_place, 'Italy')", 1, 2, None),
        ("SELECT name, birth_place FROM $astronauts WHERE SEARCH(birth_place, 'Rome')", 1, 2, None),

        ("SELECT birth_date FROM $astronauts WHERE EXTRACT(year FROM birth_date) < 1930;", 14, 1, None),
        ("SELECT EXTRACT(month FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(day FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT birth_date FROM $astronauts WHERE EXTRACT(year FROM birth_date) < 1930;", 14, 1, None),
        ("SELECT EXTRACT(doy FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(DOY FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(dow FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(DOW FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(YEAR FROM '2022-02-02')", 1, 1, None),
        ("SELECT DATE_FORMAT(birth_date, '%m-%y') FROM $astronauts", 357, 1, None),
        ("SELECT DATEDIFF('year', '2017-08-25', '2011-08-25') AS DateDiff;", 1, 1, None),
        ("SELECT DATEDIFF('days', '2022-07-07', birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT DATEDIFF('minutes', birth_date, '2022-07-07') FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(DOW FROM birth_date) AS DOW, COUNT(*) FROM $astronauts GROUP BY EXTRACT(DOW FROM birth_date) ORDER BY COUNT(*) DESC", 7, 2, None),

# fails on github but not locally
#        ("SELECT * FROM testdata.schema WITH(NO_PARTITION) ORDER BY 1", 2, 4, None),
#        ("SELECT * FROM testdata.schema WITH(NO_PARTITION, NO_PUSH_PROJECTION) ORDER BY 1", 2, 4, None),
        ("SELECT * FROM $planets WITH(NO_PARTITION) ORDER BY 1", 9, 20, None),
        ("SELECT * FROM $planets WITH(NO_PUSH_PROJECTION) ORDER BY 1", 9, 20, None),
        ("SELECT * FROM $planets WITH(NO_PARTITION, NO_PUSH_PROJECTION) ORDER BY 1", 9, 20, None),

        ("SELECT SQRT(mass) FROM $planets", 9, 1, None),
        ("SELECT FLOOR(mass) FROM $planets", 9, 1, None),
        ("SELECT CEIL(mass) FROM $planets", 9, 1, None),
        ("SELECT CEILING(mass) FROM $planets", 9, 1, None),
        ("SELECT ABS(mass) FROM $planets", 9, 1, None),
        ("SELECT ABSOLUTE(mass) FROM $planets", 9, 1, None),
        ("SELECT SIGN(mass) FROM $planets", 9, 1, None),
        ("SELECT reverse(name) From $planets", 9, 1, None),
        ("SELECT title(reverse(name)) From $planets", 9, 1, None),
        ("SELECT SOUNDEX(name) From $planets", 9, 1, None),

        ("SELECT APPROXIMATE_MEDIAN(radius) AS AM FROM $satellites GROUP BY planetId HAVING APPROXIMATE_MEDIAN(radius) > 5;", 5, 1, None),
        ("SELECT APPROXIMATE_MEDIAN(radius) AS AM FROM $satellites GROUP BY planetId HAVING AM > 5;", 5, 1, None),
        ("SELECT COUNT(planetId) FROM $satellites", 1, 1, None),
        ("SELECT COUNT_DISTINCT(planetId) FROM $satellites", 1, 1, None),
        ("SELECT LIST(name), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT ONE(name), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT ANY_VALUE(name), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT MAX(planetId) FROM $satellites", 1, 1, None),
        ("SELECT MAXIMUM(planetId) FROM $satellites", 1, 1, None),
        ("SELECT MEAN(planetId) FROM $satellites", 1, 1, None),
        ("SELECT AVG(planetId) FROM $satellites", 1, 1, None),
        ("SELECT AVERAGE(planetId) FROM $satellites", 1, 1, None),
        ("SELECT MIN(planetId) FROM $satellites", 1, 1, None),
        ("SELECT MIN_MAX(planetId) FROM $satellites", 1, 1, None),
        ("SELECT PRODUCT(planetId) FROM $satellites", 1, 1, None),
        ("SELECT STDDEV(planetId) FROM $satellites", 1, 1, None),
        ("SELECT SUM(planetId) FROM $satellites", 1, 1, None),
        ("SELECT VARIANCE(planetId) FROM $satellites", 1, 1, None),

        ("SELECT name || ' ' || name FROM $planets", 9, 1, None),
        ("SELECT 32 * 12", 1, 1, None),
        ("SELECT 9 / 12", 1, 1, None),
        ("SELECT 3 + 3", 1, 1, None),
        ("SELECT 12 % 2", 1, 1, None),
        ("SELECT 10 - 10", 1, 1, None),
        ("SELECT POWER(((6.67 * POWER(10, -11) * 5.97 * POWER(10, 24) * 86400 * 86400) / (4 * PI() * PI())), 1/3)", 1, 1, None),
        ("SELECT name || ' ' || name AS DBL FROM $planets", 9, 1, None),
        ("SELECT * FROM $satellites WHERE planetId = 2 + 5", 27, 8, None),
        ("SELECT * FROM $satellites WHERE planetId = round(density)", 1, 8, None),
        ("SELECT * FROM $satellites WHERE planetId * 1 = round(density * 1)", 1, 8, None),
        ("SELECT ABSOLUTE(ROUND(gravity) * density * density) FROM $planets", 9, 1, None),
        ("SELECT COUNT(*), ROUND(gm) FROM $satellites GROUP BY ROUND(gm)", 22, 2, None),
        ("SELECT COALESCE(death_date, '1900-01-01') FROM $astronauts", 357, 1, None),
        ("SELECT * FROM (SELECT COUNT(*) FROM testdata.flat.formats.parquet WITH(NO_PARTITION) GROUP BY followers)", 10016, 1, None),
        ("SELECT a.id, b.id FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id)", 9, 2, None),
        ("SELECT * FROM $planets INNER JOIN $planets AS b USING (id)", 9, 40, None),
        ("SELECT ROUND(5 + RAND() * (10 - 5)) rand_between FROM $planets", 9, 1, None),

        ("SELECT BASE64_DECODE(BASE64_ENCODE('this is a string'));", 1, 1, None),
        ("SELECT BASE64_ENCODE('this is a string');", 1, 1, None),
        ("SELECT BASE64_DECODE('aGVsbG8=')", 1, 1, None),
        ("SELECT BASE85_DECODE(BASE85_ENCODE('this is a string'));", 1, 1, None),
        ("SELECT BASE85_ENCODE('this is a string');", 1, 1, None),
        ("SELECT BASE85_DECODE('Xk~0{Zv')", 1, 1, None),
        ("SELECT HEX_DECODE(HEX_ENCODE('this is a string'));", 1, 1, None),
        ("SELECT HEX_ENCODE('this is a string');", 1, 1, None),
        ("SELECT HEX_ENCODE(name) FROM $planets;", 9, 1, None),
        ("SELECT HEX_DECODE('68656C6C6F')", 1, 1, None),
        ("SELECT NORMAL()", 1, 1, None),
        ("SELECT NORMAL() FROM $astronauts", 357, 1, None),
        ("SELECT CONCAT(LIST(name)) FROM $planets GROUP BY gravity", 8, 1, None),
        ("SELECT CONCAT(missions) FROM $astronauts", 357, 1, None),
        ("SELECT CONCAT(('1', '2', '3'))", 1, 1, None),
        ("SELECT CONCAT(('1', '2', '3')) FROM $planets", 9, 1, None),
        ("SELECT CONCAT_WS(', ', LIST(name)) FROM $planets GROUP BY gravity", 8, 1, None),
        ("SELECT CONCAT_WS('*', missions) FROM $astronauts LIMIT 5", 5, 1, None),
        ("SELECT CONCAT_WS('-', ('1', '2', '3'))", 1, 1, None),
        ("SELECT CONCAT_WS('-', ('1', '2', '3')) FROM $planets", 9, 1, None),
        ("SELECT IFNULL(death_date, '1970-01-01') FROM $astronauts", 357, 1, None),
        ("SELECT RANDOM_STRING(88) FROM $planets", 9, 1, None),
        ("SELECT * FROM $planets WHERE STARTS_WITH(name, 'M')", 2, 20, None),
        ("SELECT * FROM $astronauts WHERE STARTS_WITH(name, 'Jo')", 23, 19, None),
        ("SELECT * FROM $planets WHERE ENDS_WITH(name, 'r')", 1, 20, None),
        ("SELECT * FROM $astronauts WHERE ENDS_WITH(name, 'son')", 17, 19, None),
        ("SELECT CONCAT_WS(', ', LIST(mass)) as MASSES FROM $planets GROUP BY gravity", 8, 1, None),
        ("SELECT GREATEST(LIST(name)) as NAMES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT GREATEST(LIST(gm)) as MASSES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT LEAST(LIST(name)) as NAMES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT LEAST(LIST(gm)) as MASSES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT IIF(SEARCH(missions, 'Apollo 13'), 1, 0), SEARCH(missions, 'Apollo 13'), missions FROM $astronauts", 357, 3, None),
        ("SELECT IIF(year = 1960, 1, 0), year FROM $astronauts", 357, 2, None),
        ("SELECT SUM(IIF(year < 1970, 1, 0)), MAX(year) FROM $astronauts", 1, 2, None),
        ("SELECT SUM(IIF(year < 1970, 1, 0)), year FROM $astronauts GROUP BY year ORDER BY year ASC", 21, 2, None),
        ("SELECT SUM(id) + id FROM $planets GROUP BY id", 9, 1, None),
        ("SELECT today() - INTERVAL '1' YEAR", 1, 1, None),
        ("SELECT today() - INTERVAL '1' MONTH", 1, 1, None),
        ("SELECT today() - INTERVAL '1' DAY", 1, 1, None),
        ("SELECT today() - INTERVAL '1' HOUR", 1, 1, None),
        ("SELECT today() - INTERVAL '1' MINUTE", 1, 1, None),
        ("SELECT today() - INTERVAL '1' SECOND", 1, 1, None),
        ("SELECT today() + INTERVAL '1' DAY", 1, 1, None),
        ("SELECT INTERVAL '1 1' DAY TO HOUR", 1, 1, None),
        ("SELECT INTERVAL '5 6' YEAR TO MONTH", 1, 1, None),
        ("SELECT today() - yesterday()", 1, 1, None),
        ("SELECT INTERVAL '100' YEAR + birth_date, birth_date from $astronauts", 357, 2, None),
        ("SELECT INTERVAL '1 1' MONTH to DAY + birth_date, birth_date from $astronauts", 357, 2, None),
        ("SELECT birth_date - INTERVAL '1 1' MONTH to DAY, birth_date from $astronauts", 357, 2, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' IN UNNEST(missions)", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' NOT IN UNNEST(missions)", 331, 19, None),
        ("SELECT * FROM $astronauts WHERE NOT 'Apollo 11' IN UNNEST(missions)", 354, 19, None),
        ("SET @variable = 'Apollo 11'; SELECT * FROM $astronauts WHERE @variable IN UNNEST(missions)", 3, 19, None),
        ("SET @id = 3; SELECT name FROM $planets WHERE id = @id;", 1, 1, None),
        ("SET @id = 3; SELECT name FROM $planets WHERE id < @id;", 2, 1, None),
        ("SET @id = 3; SELECT name FROM $planets WHERE id < @id OR id > @id;", 8, 1, None),
        ("SET @dob = '1950-01-01'; SELECT name FROM $astronauts WHERE birth_date < @dob;", 149, 1, None),
        ("SET @dob = '1950-01-01'; SET @mission = 'Apollo 11'; SELECT name FROM $astronauts WHERE birth_date < @dob AND @mission IN UNNEST(missions);", 3, 1, None),
        ("SET @pples = 'b'; SET @ngles = 90; SHOW VARIABLES LIKE '%s'", 2, 2, None),
        ("SET @pples = 'b'; SET @rgon = 90; SHOW VARIABLES LIKE '%gon'", 1, 2, None),
        ("SET @variable = 44; SET @var = 'name'; SHOW VARIABLES LIKE '%ri%';", 1, 2, None),
        ("SHOW PARAMETER enable_optimizer", 1, 2, None),
        ("SET enable_optimizer = true; SHOW PARAMETER enable_optimizer;", 1, 2, None),

        ("SELECT id FROM $planets WHERE NOT NOT id > 3", 6, 1, None),
        ("SELECT id FROM $planets WHERE NOT NOT id < 3", 2, 1, None),
        ("SELECT id FROM $planets WHERE NOT id > 3", 3, 1, None),
        ("SELECT id FROM $planets WHERE NOT id < 3", 7, 1, None),
        ("SELECT id FROM $planets WHERE NOT (id < 5 AND id = 3)", 8, 1, None),
        ("SELECT id FROM $planets WHERE NOT NOT (id < 5 AND id = 3)", 1, 1, None),
        ("SELECT id FROM $planets WHERE NOT id = 2 AND NOT NOT (id < 5 AND id = 3)", 1, 1, None),
        ("SET enable_optimizer = false; SELECT id FROM $planets WHERE NOT id = 2 AND NOT NOT (id < 5 AND id = 3)", 1, 1, None),
        ("SELECT * FROM $planets WHERE NOT(id = 9 OR id = 8)", 7, 20, None),
        ("SELECT * FROM $planets WHERE NOT(id = 9 OR id = 8) OR True", 9, 20, None),
        ("SELECT * FROM $planets WHERE NOT(id = 9 OR 8 = 8)", 0, 20, None),
        ("SELECT * FROM $planets WHERE 1 = 1", 9, 20, None),
        ("SELECT * FROM $planets WHERE NOT 1 = 2", 9, 20, None),

        ("SHOW CREATE TABLE $planets", 1, 1, None),
        ("SHOW CREATE TABLE $satellites", 1, 1, None),
        ("SHOW CREATE TABLE $astronauts", 1, 1, None),
        ("SHOW CREATE TABLE testdata.partitioned.framed FOR '2021-03-28'", 1, 1, None),
        ("SET enable_optimizer = false;\nSET enable_morsel_defragmentation = true;\nSELECT COUNT(*) FROM $planets WHERE id > 3 AND name ILIKE '%e%'", 1, 1, None),
        ("SET enable_optimizer = true;\nSET enable_morsel_defragmentation = true;\nSELECT COUNT(*) FROM $planets WHERE id > 3 AND name ILIKE '%e%'", 1, 1, None),
        ("SET enable_optimizer = true;\nSET enable_morsel_defragmentation = false;\nSELECT COUNT(*) FROM $planets WHERE id > 3 AND name ILIKE '%e%'", 1, 1, None),
        ("SET enable_optimizer = false;\nSET enable_morsel_defragmentation = false;\nSELECT COUNT(*) FROM $planets WHERE id > 3 AND name ILIKE '%e%'", 1, 1, None),
        ("SELECT COUNT(*) FROM $planets WHERE id > 3 AND name ILIKE '%e%' AND id > 1 AND id > 0 AND id > 2 AND name ILIKE '%e%'", 1, 1, None),

        ("SELECT planets.* FROM $planets AS planets LEFT JOIN $planets FOR '1600-01-01' AS older ON planets.id = older.id WHERE older.name IS NULL", 3, 20, None),
        ("SELECT * FROM generate_series(1,10) LEFT JOIN $planets FOR '1600-01-01' ON id = generate_series", 10, 21, None),
        ("SELECT DISTINCT name FROM generate_series(1,10) LEFT JOIN $planets FOR '1600-01-01' ON id = generate_series", 7, 1, None),
        ("SELECT 1 WHERE ' a  b ' \t = \n\n ' ' || 'a' || ' ' || \n ' b '", 1, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name, 1, 1 ) = 'M'", 2, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name, 2, 1 ) = 'a'", 3, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name, 3 ) = 'rth'", 1, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name, -1 ) = 's'", 3, 1, None),
        ("SELECT TIMESTAMP '2022-01-02', DATEDIFF('days', TIMESTAMP '2022-01-02', TIMESTAMP '2022-10-01') FROM $astronauts;", 357, 2, None),
        ("SELECT * FROM $satellites WHERE NULLIF(planetId, 5) IS NULL", 67, 8, None),
        ("SELECT * FROM $satellites WHERE NULLIF(planetId, 5) IS NOT NULL", 110, 8, None),

        ("SHOW STORES LIKE 'apple'", None, None, SqlError),
        ("SELECT name FROM $astronauts WHERE LEFT(name, POSITION(' ' IN name) - 1) = 'Andrew'", 3, 1, None),
        ("SELECT name FROM $astronauts WHERE LEFT(name, POSITION(' ' IN name)) = 'Andrew '", 3, 1, None),
        
        ("SELECT ARRAY_AGG(name) from $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT ARRAY_AGG(DISTINCT name) from $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT ARRAY_AGG(name ORDER BY name) from $satellites GROUP BY TRUE", None, None, UnsupportedSyntaxError),
        ("SELECT ARRAY_AGG(name LIMIT 1) from $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT ARRAY_AGG(DISTINCT name LIMIT 1) from $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT COUNT(*), ARRAY_AGG(name) from $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT planetId, COUNT(*), ARRAY_AGG(name) from $satellites GROUP BY planetId", 7, 3, None),
        ("SELECT ARRAY_AGG(DISTINCT LEFT(name, 1)) from $satellites GROUP BY planetId", 7, 1, None),

        ("SELECT name FROM $satellites WHERE '192.168.0.1' | '192.168.0.0/24'", 177, 1, None),
        ("SELECT name FROM $satellites WHERE '192.168.0.1' | '192.167.0.0/24'", 0, 1, None),
        ("SELECT name FROM $satellites WHERE 12 | 22", None, None, NotImplementedError),

        ("SELECT COUNT(*), place FROM (SELECT CASE id WHEN 3 THEN 'Earth' WHEN 1 THEN 'Mercury' ELSE 'Elsewhere' END as place FROM $planets) GROUP BY place;", 3, 2, None),
        ("SELECT COUNT(*), place FROM (SELECT CASE id WHEN 3 THEN 'Earth' WHEN 1 THEN 'Mercury' END as place FROM $planets) GROUP BY place HAVING place IS NULL;", 1, 2, None),
        ("SELECT COUNT(*), place FROM (SELECT CASE id WHEN 3 THEN 'Earth' WHEN 1 THEN 'Mercury' ELSE 'Elsewhere' END as place FROM $planets) GROUP BY place HAVING place IS NULL;", 0, 2, None),

        ("SELECT TRIM(LEADING 'E' FROM name) FROM $planets;", 9, 1, None),
        ("SELECT * FROM $planets WHERE TRIM(TRAILING 'arth' FROM name) = 'E'", 1, 20, None),
        ("SELECT * FROM $planets WHERE TRIM(TRAILING 'ahrt' FROM name) = 'E'", 1, 20, None),

        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified IS TRUE", 711, 2, None),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified = TRUE", 711, 2, None),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified IS TRUE AND followers < 1000", 10, 2, None),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE followers < 1000 and user_name LIKE '%news%'", 12, 2, None),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE followers < 1000 and followers < 500 and followers < 250", 40739, 2, None),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE followers BETWEEN 0 AND 251", 40939, 2, None),

        ("SELECT * FROM 'testdata/flat/formats/arrow/tweets.arrow'", 100000, 13, None),
        ("SELECT * FROM 'testdata/flat/tweets/tweets-0000.jsonl' INNER JOIN 'testdata/flat/tweets/tweets-0001.jsonl' USING (userid)", 491, 16, None),
        ("SELECT * FROM 'testdata/flat/tweets/tweets-0000.jsonl' INNER JOIN $planets on sentiment = numberOfMoons", 12, 28, None),

        ("SELECT * FROM $planets AS p JOIN $planets AS g ON p.id = g.id AND g.name = 'Earth';", 1, 40, None),
        ("SELECT * FROM $planets AS p JOIN $planets AS g ON p.id = g.id AND p.name = 'Earth';", 1, 40, None),
        ("SELECT * FROM $planets AS p JOIN $planets AS g ON g.name = 'Earth' AND p.id = g.id;", 1, 40, None),
        ("SELECT * FROM $planets AS p JOIN $planets AS g ON p.name = 'Earth' AND p.id = g.id;", 1, 40, None),

        ("SELECT SPLIT(name, ' ', 0) FROM $astronauts", None, None, ProgrammingError),
        ("SELECT SPLIT(name, ' ', 1) FROM $astronauts", 357, 1, None),

        # virtual dataset doesn't exist
        ("SELECT * FROM $RomanGods", None, None, DatasetNotFoundError),
        # disk dataset doesn't exist
        ("SELECT * FROM non.existent", None, None, DatasetNotFoundError),
        # column doesn't exist
        ("SELECT awesomeness_factor FROM $planets;", None, None, ColumnNotFoundError),
        ("SELECT * FROM $planets WHERE awesomeness_factor > 'Mega';", None, None, ColumnNotFoundError),
        # https://trino.io/docs/current/functions/aggregate.html#filtering-during-aggregation
        ("SELECT LIST(name) FILTER (WHERE name IS NOT NULL) FROM $planets;", None, None, SqlError),
        # Can't IN an INDENTIFIER
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' IN missions", None, None, SqlError),
        # Invalid temporal ranges
        ("SELECT * FROM $planets FOR 2022-01-01", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES IN 2022", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES BETWEEN 2022-01-01 AND TODAY", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES BETWEEN today AND yesterday", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES IN '2022-01-01' AND '2022-01-02'", None, None, InvalidTemporalRangeFilterError),
        # Join hints aren't supported
        ("SELECT * FROM $satellites INNER HASH JOIN $planets USING (id)", None, None, SqlError),
        # MONTH has a bug
        ("SELECT DATEDIFF('months', birth_date, '2022-07-07') FROM $astronauts", None, None, KeyError),
        ("SELECT DATEDIFF('months', birth_date, '2022-07-07') FROM $astronauts", None, None, KeyError),
        ("SELECT DATEDIFF(MONTH, birth_date, '2022-07-07') FROM $astronauts", None, None, ColumnNotFoundError),
        ("SELECT DATEDIFF(MONTHS, birth_date, '2022-07-07') FROM $astronauts", None, None, ColumnNotFoundError),
        # DISTINCT ON detects as a function call for function ON
#        ("SELECT DISTINCT ON (name) name FROM $astronauts ORDER BY 1", None, None, UnsupportedSyntaxError),
        # SELECT EXCEPT isn't supported
        # https://towardsdatascience.com/4-bigquery-sql-shortcuts-that-can-simplify-your-queries-30f94666a046
        ("SELECT * EXCEPT id FROM $satellites", None, None, SqlError),
        # TEMPORAL QUERIES aren't part of the AST
        ("SELECT * FROM CUSTOMERS FOR SYSTEM_TIME ('2022-01-01', '2022-12-31')", None, None, InvalidTemporalRangeFilterError),
        # can't cast to a list
        ("SELECT CAST('abc' AS LIST)", None, None, SqlError),
        ("SELECT TRY_CAST('abc' AS LIST)", None, None, SqlError),

        # V2 Negative Tests
        ("SELECT $planets.id, name FROM $planets INNER JOIN $satellites ON planetId = $planets.id", None, None, AmbiguousIdentifierError),
        ("SELECT $planets.id FROM $satellites", None, None, UnexpectedDatasetReferenceError),

        # V2 New Syntax Checks
        ("SELECT * FROM $planets UNION SELECT * FROM $planets;", None, None, None),
        ("SELECT * FROM $planets LEFT ANTI JOIN $satellites ON id = id;", None, None, ArrowInvalid),  # invalid until the join is written
        ("EXPLAIN ANALYZE FORMAT JSON SELECT * FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id);", None, None, None),
        ("SELECT DISTINCT ON (planetId) planetId, name FROM $satellites ", None, None, None),
        ("SELECT 8 DIV 4", None, None, None),

        # These are queries which have been found to return the wrong result or not run correctly
        # FILTERING ON FUNCTIONS
        ("SELECT DATE(birth_date) FROM $astronauts FOR TODAY WHERE DATE(birth_date) < '1930-01-01'", 14, 1, None),
        # ORDER OF CLAUSES (FOR before INNER JOIN)
        ("SELECT * FROM $planets FOR '2022-03-03' INNER JOIN $satellites ON $planets.id = $satellites.planetId", 177, 28, None),
        # ZERO RECORD RESULT SETS
        ("SELECT * FROM $planets WHERE id = -1 ORDER BY id", 0, 20, None),
        ("SELECT * FROM $planets WHERE id = -1 LIMIT 10", 0, 20, None),
        # LEFT JOIN THEN FILTER ON NULLS
        ("SELECT * FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id WHERE $satellites.id IS NULL", 2, 28, None),
        ("SELECT * FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id WHERE $satellites.name IS NULL", 2, 28, None),
        # SORT BROKEN
        ("SELECT * FROM (SELECT * FROM $planets ORDER BY id DESC LIMIT 5) WHERE id > 7", 2, 20, None),
        # ORDER OF JOIN CONDITION
        ("SELECT * FROM $planets INNER JOIN $satellites ON $satellites.planetId = $planets.id", 177, 28, None),
        # ORDER BY QUALIFIED IDENTIFIER
        ("SELECT * FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id ORDER BY $planets.name", 179, 28, None),
        # NAMED SUBQUERIES
        ("SELECT P.name FROM ( SELECT * FROM $planets ) AS P", 9, 1, None),
        # UNNEST
        ("SELECT * FROM testdata.partitioned.unnest_test FOR '2000-01-01' CROSS JOIN UNNEST (values) AS value ", 15, 3, None),
        # FRAME HANDLING
        ("SELECT * FROM testdata.partitioned.framed FOR '2021-03-28'", 100000, 1, None),
        ("SELECT * FROM testdata.partitioned.framed FOR '2021-03-29'", 100000, 1, None),
        ("SELECT * FROM testdata.partitioned.framed FOR DATES BETWEEN '2021-03-28' AND '2021-03-29'", 200000, 1, None),
        ("SELECT * FROM testdata.partitioned.framed FOR DATES BETWEEN '2021-03-29' AND '2021-03-30'", 100000, 1, None),
        ("SELECT * FROM testdata.partitioned.framed FOR DATES BETWEEN '2021-03-28' AND '2021-03-30'", 200000, 1, None),
        # PAGING OF DATASETS AFTER A GROUP BY [#179]
        ("SELECT * FROM (SELECT COUNT(*), column_1 FROM FAKE(5000,2) GROUP BY column_1 ORDER BY COUNT(*)) LIMIT 5", 5, 2, None),
        # FILTER CREATION FOR 3 OR MORE ANDED PREDICATES FAILS [#182]
        ("SELECT * FROM $astronauts WHERE name LIKE '%o%' AND `year` > 1900 AND gender ILIKE '%ale%' AND group IN (1,2,3,4,5,6)", 41, 19, None),
        # LIKE-ING NULL
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username LIKE 'BBC%'", 3, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username ILIKE 'BBC%'", 3, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username NOT LIKE 'BBC%'", 21, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE NOT username LIKE 'BBC%'", 22, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username NOT ILIKE 'BBC%'", 21, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username ~ 'BBC.+'", 3, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username !~ 'BBC.+'", 21, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username ~* 'bbc.+'", 3, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username !~* 'bbc.+'", 21, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username SIMILAR TO 'BBC.+'", 3, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username NOT SIMILAR TO 'BBC.+'", 21, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE tweet ILIKE '%Trump%'", 0, 5, None),
        # BYTE-ARRAY FAILS [#252]
        (b"SELECT * FROM $satellites", 177, 8, None),
        # DISTINCT on null values [#285]
        ("SELECT DISTINCT name FROM (VALUES (null),(null),('apple')) AS booleans (name)", 2, 1, None),
        # empty aggregates with other columns, loose the other columns [#281]
# [#358]       ("SELECT name, COUNT(*) FROM $astronauts WHERE name = 'Jim' GROUP BY name", 1, 2, None),
        # JOIN from subquery regressed [#291]
        ("SELECT * FROM (SELECT id from $planets) AS ONE LEFT JOIN (SELECT id from $planets) AS TWO ON id = id", 9, 2, None),
        # JOIN on UNNEST [#382]
        ("SELECT name FROM $planets INNER JOIN UNNEST(('Earth')) AS n on name = n ", 1, 1, None),
        ("SELECT name FROM $planets INNER JOIN UNNEST(('Earth', 'Mars')) AS n on name = n", 2, 1, None),
        # SELECT <literal> [#409]
        ("SELECT DATE FROM (SELECT '1980-10-20' AS DATE)", 1, 1, None),
        ("SELECT NUMBER FROM (SELECT 1.0 AS NUMBER)", 1, 1, None),
        ("SELECT VARCHAR FROM (SELECT 'varchar' AS VARCHAR)", 1, 1, None),
        ("SELECT BOOLEAN FROM (SELECT False AS BOOLEAN)", 1, 1, None),
        # EXPLAIN has two heads (found looking a [#408])
        ("EXPLAIN SELECT * FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id)", 3, 3, None),
        # ALIAS issues [#408]
        ("SELECT $planets.* FROM $planets INNER JOIN (SELECT id FROM $planets) AS b USING (id)", 9, 21, None),
        # DOUBLE QUOTED STRING [#399]
        ("SELECT birth_place['town'] FROM $astronauts WHERE birth_place['town'] = \"Rome\"", 1, 1, None),
        # COUNT incorrect
        ("SELECT * FROM (SELECT COUNT(*) AS bodies FROM $planets) AS space WHERE space.bodies > 5", 1, 1, None),
        # REGRESSION
        ("SELECT VERSION()", 1, 1, None),
        # COALESCE doesn't work with NaNs [#404]
        ("SELECT is_reply_to FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE COALESCE(is_reply_to, -1) < 0", 74765, 1, None),
        # Names not found / clashes [#471]
        ("SELECT P.* FROM (SELECT * FROM $planets) AS P", 9, 20, None),
        ("SELECT P0.id, P1.ID, P2.ID FROM $planets AS P0 JOIN (SELECT id AS ID, name FROM $planets) AS P1 ON P0.name = P1.name JOIN (SELECT id, name AS ID FROM $planets) AS P2 ON P0.name = P2.name", 9, 3, None),
        ("SELECT P0.id, P1.ID FROM $planets AS P0 INNER JOIN (SELECT id, name AS ID FROM $planets) AS P1 ON P0.name = P1.name", 9, 2, None),
        ("SELECT P0.id, P1.ID FROM $planets AS P0 INNER JOIN (SELECT name, id AS ID FROM $planets) AS P1 USING (name)", 9, 2, None),
        ("SELECT P0.id, P1.ID FROM $planets AS P0 LEFT JOIN (SELECT id, name AS ID FROM $planets) AS P1 ON P0.name = P1.name", 9, 2, None),
        # [#475] a variation of #471
        ("SELECT P0.id, P1.ID, P2.ID FROM $planets AS P0 JOIN (SELECT CONCAT_WS(' ', list(id)) AS ID, MAX(name) AS n FROM $planets GROUP BY gravity) AS P1 ON P0.name = P1.n JOIN (SELECT CONCAT_WS(' ', list(id)) AS ID, MAX(name) AS n FROM $planets GROUP BY gravity) AS P2 ON P0.name = P2.n", 8, 3, None),
        # no issue number - but these two caused a headache
        # FUNCTION (AGG)
        ("SELECT CONCAT(LIST(name)) FROM $planets GROUP BY gravity", 8, 1, None),
        # AGG (FUNCTION)
        ("SELECT SUM(IIF(year < 1970, 1, 0)), MAX(year) FROM $astronauts", 1, 2, None),
        # [#527] variables referenced in subqueries
        ("SET @v = 1; SELECT * FROM (SELECT @v);", 1, 1, None),
        # [#561] HASH JOIN with an empty table
        ("SELECT * FROM $planets LEFT JOIN (SELECT planetId as id FROM $satellites WHERE id < 0) USING (id)", 0, 1, None),  
        # [#646] Incorrectly placed temporal clauses
        ("SELECT * FROM $planets WHERE 1 = 1 FOR TODAY;", None, None, SqlError),
        ("SELECT * FROM $planets GROUP BY name FOR TODAY;", None, None, SqlError),
        # [#518] SELECT * and GROUP BY can't be used together
        ("SELECT * FROM $planets GROUP BY name", None, None, SqlError),
        # found testing
        ("SELECT user_name FROM testdata.flat.formats.arrow WITH(NO_PARTITION) WHERE user_name = 'Niran'", 1, 1, None),
        #769
        ("SELECT GREATEST(ARRAY_AGG(name)) as NAMES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT LEAST(ARRAY_AGG(name)) as NAMES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT SORT(ARRAY_AGG(name)) as NAMES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT LEAST(ARRAY_AGG(DISTINCT name LIMIT 5)) as NAMES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT SORT(ARRAY_AGG(name LIMIT 5)) as NAMES FROM $satellites GROUP BY planetId", 7, 1, None),
        # 797
        ("SELECT COUNT(*) AS planets, id FROM $planets GROUP BY id ORDER BY planets DESC", 9, 2, None),
        # 833
        (b"", None, None,  MissingSqlStatement),
        ("", None, None, MissingSqlStatement),
        # 870
        ("SELECT MAX(density) FROM $planets GROUP BY orbitalInclination, escapeVelocity, orbitalInclination, numberOfMoons, escapeVelocity, density", 9, 1, None),
        ("SELECT COUNT(*) FROM $planets GROUP BY orbitalInclination, orbitalInclination", 9, 1, None),
        # 909 - zero results from pushed predicate
        ("SELECT * FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_id = -1", 0, 13, EmptyResultSetError),
        # 912 - optimized boolean evals were ignored
        ("SELECT * FROM $planets WHERE 1 = PI()", 0, 20, None),
        ("SELECT * FROM $planets WHERE PI() = 1", 0, 20, None),
        ("SET enable_optimizer = false; SELECT * FROM $planets WHERE 1 = PI()", 0, 20, None),
        ("SET enable_optimizer = false; SELECT * FROM $planets WHERE PI() = 1", 0, 20, None),
        ("SELECT * FROM $planets WHERE 3.141592653589793238462643383279502 = PI()", 9, 20, None),
        ("SELECT * FROM $planets WHERE PI() = 3.141592653589793238462643383279502", 9, 20, None),
        ("SET enable_optimizer = false; SELECT * FROM $planets WHERE 3.141592653589793238462643383279502 = PI()", 9, 20, None),
        ("SET enable_optimizer = false; SELECT * FROM $planets WHERE PI() = 3.141592653589793238462643383279502", 9, 20, None),
        # found in testing
        ("SELECT * FROM $planets WHERE id = null", 0, 20, None),
        ("SELECT * FROM $planets WHERE id != null", 0, 20, None),
        ("SELECT * FROM $planets WHERE id > null", 0, 20, None),
        ("SELECT * FROM $planets WHERE id < null", 0, 20, None),
        ("SELECT * FROM $planets WHERE id >= null", 0, 20, None),
        ("SELECT * FROM $planets WHERE id <= null", 0, 20, None),
        # 929 - handle invalid temporal range filters better
        ("SELECT * FROM $planets FOR DATES BETWEEN TODAY", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES BETWEEN TODAY AND TOMORROW", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES BETWEEN TODAY OR TOMORROW", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES BETWEEN BEFORE AND TODAY", None, None, InvalidTemporalRangeFilterError),
        # 999 - subscripting
        ("SELECT name['n'] FROM $planets", None, None, ProgrammingError),
        ("SELECT id['n'] FROM $planets", None, None, ProgrammingError),
        # [1008] fuzzy search fails on ints
        ("SELECT * FROM $planets JOIN $planets ON id = 12;", None, None, ColumnNotFoundError),
        ("SELECT * FROM $planets JOIN $planets ON 12 = id;", None, None, ColumnNotFoundError),
        # [1006] dots in filenames
        ("SELECT * FROM 'testdata/flat/multi/00.01.jsonl'", 1, 4, None),
        # [1015] predicate pushdowns
        ("SELECT * FROM $planets WHERE rotationPeriod = lengthOfDay", 3, 20, None),
        ("SELECT * FROM 'testdata.flat.planets.parquet' WITH(NO_PARTITION) WHERE rotationPeriod = lengthOfDay", 3, 20, None),
        ("SELECT * FROM 'testdata/flat/planets/parquet/planets.parquet' WITH(NO_PARTITION) WHERE rotationPeriod = lengthOfDay", 3, 20, None),
]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery(statement, rows, columns, exception):
    """
    Test an battery of statements
    """

    #    opteryx.register_store("tests", DiskConnector)
    #    opteryx.register_store("mabellabs", AwsS3Connector)

    conn = opteryx.connect()
    cursor = conn.cursor()
    try:
        cursor.execute(statement)
        actual_rows, actual_columns = cursor.shape
        assert (
            rows == actual_rows
        ), f"\n{cursor.display()}\n\033[38;5;203mQuery returned {actual_rows} rows but {rows} were expected.\033[0m\n{statement}"
        assert (
            columns == actual_columns
        ), f"\n{cursor.display()}\n\033[38;5;203mQuery returned {actual_columns} cols but {columns} were expected.\033[0m\n{statement}"
    except AssertionError as err:
        print(f"\n{err}", flush=True)
        quit()
    except Exception as err:
        assert (
            type(err) == exception
        ), f"\n{format_sql(statement)}\nQuery failed with error {type(err)} but error {exception} was expected"


if __name__ == "__main__":  # pragma: no cover
    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil
    import time

    from tests.tools import trunc_printable

    width = shutil.get_terminal_size((80, 20))[0] - 15

    nl = "\n"

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SHAPE TESTS")
    for index, (statement, rows, cols, err) in enumerate(STATEMENTS):
        start = time.monotonic_ns()
        printable = statement
        if hasattr(printable, "decode"):
            printable = printable.decode()
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(printable), width - 1)}",
            end="",
            flush=True,
        )
        test_sql_battery(statement, rows, cols, err)
        print(f"\033[0;32m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅")

    print("--- ✅ \033[0;32mdone\033[0m")
