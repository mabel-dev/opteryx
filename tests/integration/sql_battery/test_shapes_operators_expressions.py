"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
 >  Shape Checking
    Results Checking
    Compare to DuckDB

This file tests: Operators, expressions, and infix calculations

This tests that the shape of the response is as expected: the right number of columns,
the right number of rows and, if appropriate, the right exception is thrown.
"""
import pytest
import os
import sys

#import opteryx

from typing import Optional

sys.path.insert(1, os.path.join(sys.path[0], "../../../draken"))
sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../../rugo"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

# from opteryx.connectors import AwsS3Connector, DiskConnector
from opteryx.exceptions import (
    AmbiguousDatasetError,
    AmbiguousIdentifierError,
    ArrayWithMixedTypesError,
    ColumnNotFoundError,
    ColumnReferencedBeforeEvaluationError,
    DatasetNotFoundError,
    EmptyDatasetError,
    FunctionExecutionError,
    FunctionNotFoundError,
    IncompatibleTypesError,
    InconsistentSchemaError,
    IncorrectTypeError,
    InvalidFunctionParameterError,
    InvalidTemporalRangeFilterError,
    MissingSqlStatement,
    ParameterError,
    PermissionsError,
    SqlError,
    UnexpectedDatasetReferenceError,
    UnnamedColumnError,
    UnnamedSubqueryError,
    UnsupportedSyntaxError,
    VariableNotFoundError,
)
from opteryx.managers.schemes.mabel_partitions import UnsupportedSegementationError
from opteryx.utils.formatter import format_sql
from opteryx.connectors import IcebergConnector

# fmt:off
# fmt:off
STATEMENTS = [
        # Tests with comments in different parts of the query
        ("/* comment */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites /* comment */ WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites /* WHERE name = 'Calypso' */", 177, 8, None),
        ("SELECT * FROM $satellites -- WHERE name = 'Calypso'", 177, 8, None),
        ("SELECT * FROM $satellites -- WHERE name = 'Calypso' \n WHERE id = 1", 1, 8, None),
        ("-- comment\nSELECT * --comment\n FROM $satellites -- WHERE name = 'Calypso' \n WHERE id = 1", 1, 8, None),
        ("/* comment */ SELECT * FROM $satellites /* comment */  WHERE name = 'Calypso'", 1, 8, None),
        ("/* comment */ SELECT * FROM $satellites /* comment */  WHERE name = 'Calypso'  /* comment */ ", 1, 8, None),
        ("/* comment --inner */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("/* comment ; */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("-- inner ; \nSELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name != 'Ca;lypso'", 177, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Calypso';", 1, 8, None),
        (";", None, None, MissingSqlStatement),
        ("SELECT * FROM $satellites -- comment\n FOR TODAY", 177, 8, None),
        ("SELECT * FROM $satellites /* comment */ FOR TODAY /* comment */", 177, 8, None),
        ("/* comment */", None, None, MissingSqlStatement),
        ("/* SELECT * FROM $planets */", None, None, MissingSqlStatement),
        ("/* outer /* inner */ */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, SqlError),
        ("/* comment\n */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("-- comment /**/ \nSELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name = '/* Not a comment */ Calypso'", 0, 8, None),
        ("SELECT * -- comment1\n -- comment2\n FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * /* comment1 */ FROM /* comment2 */ $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Cal--ypso'", 0, 8, None),
        ("SELECT * FROM $satellites-- WHERE name = 'Calypso'", 177, 8, None),
        ("SELECT * FROM $satellites/* WHERE name = 'Calypso' */", 177, 8, None),
        ("/* multi\n line \n comment */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("/* comment */ SELECT * -- comment\n FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Calypso' --", 1, 8, None),
        ("SELECT * FROM $satellites; -- comment\n SELECT * FROM $planets;", 9, 20, None),
        ("-- comment\nSELECT * FROM $satellites WHERE name = 'Calypso \\-- Not comment'", 0, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Calypso \\/* Not comment */'", 0, 8, None),
        ("-- cómment\nSELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("-- Comment\nSELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Calypso' /* comment in WHERE */", 1, 8, None),
        ("-- SELECT\nSELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("/* comment */ /* another comment */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * /* block comment */ FROM $satellites WHERE name = 'Calypso' -- line comment", 1, 8, None),
        ("SELECT * FROM $satellites /* comment with special chars *&^%$#@! */ WHERE name = 'Calypso'", 1, 8, None),
        ("/* Starting comment */ SELECT * FROM $satellites /* Ending comment */", 177, 8, None),
        ("-- Single line comment with ;\nSELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites -- comment with special chars *&^%$#@!\n WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT /* inner */ * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * -- Comment1\n-- Comment2\n-- Comment3\nFROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Calypso' /* multi\nline\ncomment */", 1, 8, None),
        ("/* outer */ SELECT * FROM $satellites WHERE name = 'Calypso' -- inner", 1, 8, None),
        ("SELECT * FROM $satellites WHERE /* comment */ name /* comment */ = /* comment */ 'Calypso'", 1, 8, None),
        ("-- Comment before query\n\n\nSELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Calypso' /* nested /* still nested */ end of nested */", 1, 8, None),
        ("/* multi; \n line; \n comment with semicolons */ SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),

        ("SELECT name, id, planetId FROM $satellites", 177, 3, None),
        ("SELECT name, name FROM $satellites", 177, 1, AmbiguousIdentifierError),  # V2 breaking
        ("SELECT name, id, name, id FROM $satellites", 177, 2, AmbiguousIdentifierError),  # V2 breaking
        ("SELECT name, name, name, id FROM $satellites", 177, 2, AmbiguousIdentifierError),  # V2 breaking
        ("SELECT name, id, name FROM $satellites", 177, 2, AmbiguousIdentifierError),  # V2 breaking

        # test DISTINCT on various column types and combinations
        ("SELECT DISTINCT name FROM $astronauts", 357, 1, None),
        ("SELECT DISTINCT * FROM $astronauts", 357, 19, None),
        ("SELECT DISTINCT name FROM $astronauts", 357, 1, None),
        ("SELECT DISTINCT year FROM $astronauts", 21, 1, None),
        ("SELECT DISTINCT group FROM $astronauts", 21, 1, None),
        ("SELECT DISTINCT status FROM $astronauts", 4, 1, None),
        ("SELECT DISTINCT birth_date FROM $astronauts", 348, 1, None),
        ("SELECT DISTINCT birth_place FROM $astronauts", 272, 1, None),
        ("SELECT DISTINCT gender FROM $astronauts", 2, 1, None),
        ("SELECT DISTINCT alma_mater FROM $astronauts", 281, 1, None),
        ("SELECT DISTINCT undergraduate_major FROM $astronauts", 84, 1, None),
        ("SELECT DISTINCT graduate_major FROM $astronauts", 144, 1, None),
        ("SELECT DISTINCT military_rank FROM $astronauts", 13, 1, None),
        ("SELECT DISTINCT military_branch FROM $astronauts", 15, 1, None),
        ("SELECT DISTINCT space_flights FROM $astronauts", 8, 1, None),
        ("SELECT DISTINCT space_flight_hours FROM $astronauts", 270, 1, None),
        ("SELECT DISTINCT space_walks FROM $astronauts", 11, 1, None),
        ("SELECT DISTINCT space_walks_hours FROM $astronauts", 52, 1, None),
        ("SELECT DISTINCT missions FROM $astronauts", 305, 1, None),
        ("SELECT DISTINCT death_date FROM $astronauts", 39, 1, None),
        ("SELECT DISTINCT death_mission FROM $astronauts", 4, 1, None),
        ("SELECT DISTINCT name, birth_date, missions, birth_place, group FROM $astronauts", 357, 5, None),
        ("SELECT DISTINCT name, status FROM $astronauts ", 357, 2, None),
        ("SELECT DISTINCT name, year FROM $astronauts ", 357, 2, None),
        ("SELECT DISTINCT name, birth_date FROM $astronauts ", 357, 2, None),
        ("SELECT DISTINCT year, group FROM $astronauts ", 23, 2, None),
        ("SELECT DISTINCT year, birth_date FROM $astronauts ", 355, 2, None),
        ("SELECT DISTINCT name, birth_place FROM $astronauts ", 357, 2, None),
        ("SELECT DISTINCT name, alma_mater FROM $astronauts ", 357, 2, None),
        ("SELECT DISTINCT year, birth_place FROM $astronauts ", 346, 2, None),
        ("SELECT DISTINCT year, alma_mater FROM $astronauts ", 336, 2, None),
        ("SELECT DISTINCT birth_date, birth_place FROM $astronauts ", 356, 2, None),
        ("SELECT DISTINCT birth_date, alma_mater FROM $astronauts ", 357, 2, None),
        ("SELECT DISTINCT birth_place, alma_mater FROM $astronauts ", 356, 2, None),

        # alias tests
        ("SELECT name as Name FROM $satellites", 177, 1, None),
        ("SELECT name as Name, id as Identifier FROM $satellites", 177, 2, None),
        ("SELECT name as NAME FROM $satellites WHERE name = 'Calypso'", 1, 1, None),
        ("SELECT name as NAME FROM $satellites GROUP BY name", 177, 1, None),
        ("SELECT id as id FROM $satellites", 177, 1, None),
        ("SELECT planetId as planetId FROM $satellites", 177, 1, None),
        ("SELECT id as ID, planetId as PLANETID FROM $satellites", 177, 2, None),
        ("SELECT id as iD, name as nAME FROM $satellites WHERE planetId = 5", 67, 2, None),
        ("SELECT id as ID, planetId as planetId FROM $satellites WHERE name = 'Io'", 1, 2, None),
        ("SELECT name as NAME, id as ID, planetId as PLANETID FROM $satellites", 177, 3, None),
        ("SELECT id as ID FROM $satellites GROUP BY id", 177, 1, None),
        ("SELECT planetId as planetId FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT name as nAme, id as Id FROM $satellites WHERE planetId = 3", 1, 2, None),
        ("SELECT id as ID, name as Name FROM $satellites GROUP BY name, id", 177, 2, None),
        ("SELECT UPPER(name) as NAME FROM $satellites", 177, 1, None),
        ("SELECT name as n FROM $satellites WHERE n = 'Titan'", None, 1, ColumnNotFoundError),
        ("SELECT id as Identifier FROM $satellites ORDER BY Identifier", 177, 1, None),
        ("SELECT name as n FROM $satellites GROUP BY name HAVING COUNT(n) > 1", None, 1, ColumnReferencedBeforeEvaluationError),  # TEMP
        ("SELECT name as Name, name as NAME FROM $satellites", 177, 2, AmbiguousIdentifierError),
        ("SELECT COUNT(id) as countID, MIN(id) as minID FROM $satellites", 1, 2, None),
        ("SELECT s.id as satelliteID, p.id as planetID FROM $satellites s JOIN $planets p ON s.planetId = p.id", 177, 2, None),
        ("SELECT x.ID FROM (SELECT id as ID FROM $satellites WHERE id < 10) x", 9, 1, None),
        ("SELECT name as n as m FROM $satellites", None, 1, SqlError),
        ("SELECT id*2 as doubleID FROM $satellites", 177, 1, None),
        ("SELECT id as Identifier FROM $satellites ORDER BY Identifier", 177, 1, None),
        ("SELECT name as n FROM $satellites GROUP BY name HAVING COUNT(n) > 1", None, 1, ColumnReferencedBeforeEvaluationError),
        ("SELECT name as n FROM $satellites WHERE n = 'Calypso'", None, 1, ColumnNotFoundError),
        ("SELECT id * 2 as DoubleID FROM $satellites", 177, 1, None),
        ("SELECT LEFT(name, 3) as newName FROM $satellites", 177, 1, None),
        ("SELECT name as n, id as i, planetId as p FROM $satellites WHERE planetId = 3 ORDER BY n, i", 1, 3, None),
        ("SELECT name as n1, name as n2 FROM $satellites", 177, 2, AmbiguousIdentifierError),
        ("SELECT COUNT(id) as Total FROM $satellites", 1, 1, None),
        ("SELECT x.id FROM (SELECT id FROM $satellites) as x", 177, 1, None),
        ("SELECT id as Identifier, name FROM $satellites", 177, 2, None),

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
        ("SELECT * FROM $satellites WHERE id = -5 + 10", 1, 8, None),

        ("SELECT * FROM $satellites WHERE id = ABS(-5)", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 5 - 3 + 1", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = (3 * 1) + 2", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 6 DIV (3 + 1)", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id BETWEEN 4 AND 6", 3, 8, None),
        ("SELECT * FROM $satellites WHERE id ^ 1", 176, 8, None),
        ("SELECT * FROM $satellites WHERE id & 1", 89, 8, None),
        ("SELECT * FROM $satellites WHERE id | 1", 177, 8, None),
        ("SELECT * FROM $satellites WHERE id = 0x08", 1, 8, None),

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
        ("SELECT * FROM $satellites WHERE (id BETWEEN 3 AND 8) AND (3 BETWEEN 2 AND 5);", 6, 8, None),
        ("SELECT * FROM $satellites WHERE (id BETWEEN 2 AND 6) OR (6 BETWEEN 5 AND 7);", 177, 8, None),
        ("SELECT * FROM $satellites WHERE (ABS(id - 3) < 5) AND (ABS(3 - id) < 5);", 7, 8, None),
        ("SELECT * FROM $satellites WHERE (LENGTH(name) > 2) OR (LENGTH('Moon') > 2);", 177, 8, None),
        ("SELECT * FROM $satellites WHERE (id BETWEEN 7 AND 9) AND (7 > 2 AND 7 < 10);", 3, 8, None),
        ("SELECT * FROM $satellites WHERE 1 = 1;", 177, 8, None),    
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
        ("SELECT * FROM $planets WHERE name NOT RLIKE '^E.+h$'", 8, 20, None),
        ("SELECT * FROM $planets WHERE name RLIKE '^E.+h$'", 1, 20, None),
        ("SELECT * FROM $satellites WHERE name SIMILAR TO '^C.'", 12, 8, None),
        ("SELECT * FROM $satellites WHERE name !~ '^C.'", 165, 8, None),
        ("SELECT * FROM $satellites WHERE name NOT SIMILAR TO '^C.'", 165, 8, None),

        ("SELECT * FROM $satellites WHERE (id = 5 OR name = 'Europa') AND TRUE;", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR name = 'Europa') OR FALSE;", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 5 OR (name = 'Europa' AND TRUE);", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 AND TRUE) OR name = 'Europa';", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR FALSE) OR name = 'Europa';", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 5 OR (name = 'Europa' OR FALSE);", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR name = 'Europa') AND (1=1);", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 AND (2>1)) OR name = 'Europa';", 1, 8, None),
        ("SELECT * FROM $satellites WHERE id = 5 OR (name = 'Europa' AND ('a'='a'));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR (3=4)) OR name = 'Europa';", 1, 8, None),
        ("SELECT * FROM $satellites WHERE ((id = 5 AND (1=1)) OR (name = 'Europa' AND (TRUE OR FALSE)));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR (name = 'Europa' AND (TRUE AND (2>1))));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE ((id = 5 OR FALSE) AND TRUE) OR (name = 'Europa' AND ('x'='x'));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE ((id = 5 OR (4=5)) AND (6!=7)) OR (name = 'Europa');", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 AND (NOT FALSE)) OR (name = 'Europa' OR (7=8));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE ((id = 5 AND ((8<>8) OR TRUE)) OR name = 'Europa');", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR (name = 'Europa' AND (FALSE AND TRUE)));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE ((id = 5 OR (9>10)) AND ('a'!='b')) OR name = 'Europa';", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR (name = 'Europa' AND ('x'='x' OR (10=11))));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR ((11<12) AND name = 'Europa'));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE ((id = 5 AND (TRUE AND (1=1))) OR (name = 'Europa' AND (FALSE OR (2!=2))));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR ((name = 'Europa' OR (TRUE AND FALSE)) AND (3>3)));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (((id = 5 OR FALSE) AND (TRUE OR (4<4))) OR (name = 'Europa' AND ('x'='y')));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE ((id = 5 AND ((5=6) OR TRUE)) OR (name = 'Europa' AND (FALSE OR (6=7))));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR (name = 'Europa' AND (TRUE OR (7!=7) AND FALSE)));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE ((id = 5 AND (NOT (8=8))) OR (name = 'Europa' AND (9>10 OR TRUE)));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (((id = 5 OR (10<11)) AND ('a'='b')) OR (name = 'Europa' AND (TRUE AND (11=11))));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (id = 5 OR ((name = 'Europa' OR (FALSE AND TRUE)) AND (12>13)));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (((id = 5 AND (14=14)) OR FALSE) AND ((name = 'Europa' OR (14<15)) AND ('x'='x')));", 1, 8, None),
        ("SELECT * FROM $satellites WHERE ((id = 5 OR (name = 'Europa' AND ((15=16) OR FALSE))) AND (TRUE AND (16!=17)));", 1, 8, None),

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
        ("SELECT * FROM $satellites ORDER BY RANDOM()", 177, 8, None),

        ("SELECT MAX(planetId) FROM $satellites", 1, 1, None),
        ("SELECT MIN(planetId) FROM $satellites", 1, 1, None),
        ("SELECT SUM(planetId) FROM $satellites", 1, 1, None),
        ("SELECT MAX(id), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT MIN(id), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT SUM(id), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT MIN(id), MAX(id), SUM(planetId), planetId FROM $satellites GROUP BY planetId", 7, 4, None),
        ("SELECT planetId, ARRAY_AGG(name) FROM $satellites GROUP BY planetId", 7, 2, None),

        ("SELECT planetId FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT BOOLEAN(planetId - 3) FROM $satellites GROUP BY BOOLEAN(planetId - 3)", 2, 1, None),
        ("SELECT VARCHAR(planetId) FROM $satellites GROUP BY VARCHAR(planetId)", 7, 1, None),
        ("SELECT COUNT(*) FROM $satellites GROUP BY TIMESTAMP('2022-01-0' || VARCHAR(planetId))", 7, 1, None),
        ("SELECT DOUBLE(planetId) FROM $satellites GROUP BY DOUBLE(planetId)", 7, 1, None),
        ("SELECT INT(planetId) FROM $satellites GROUP BY INT(planetId)", 7, 1, None),
        ("SELECT INTEGER(planetId) FROM $satellites GROUP BY INTEGER(planetId)", 7, 1, None),
        ("SELECT CAST(planetId AS BOOLEAN) FROM $satellites", 177, 1, None),
        ("SELECT CAST(planetId AS VARCHAR) FROM $satellites", 177, 1, None),
        ("SELECT CAST('2022-01-0' || VARCHAR(planetId) AS TIMESTAMP) FROM $satellites", 177, 1, None),
        ("SELECT CAST(planetId AS INTEGER) FROM $satellites", 177, 1, None),
        ("SELECT planetId::BOOLEAN FROM $satellites", 177, 1, None),
        ("SELECT planetId::VARCHAR FROM $satellites", 177, 1, None),
        ("SELECT CAST('2022-01-0' || planetId::VARCHAR AS TIMESTAMP) FROM $satellites", 177, 1, None),
        ("SELECT planetId::INTEGER FROM $satellites", 177, 1, None),
        ("SELECT planetId::DOUBLE FROM $satellites", 177, 1, None),
        ("SELECT 1::double", 1, 1, None),
        ("SELECT TRY_CAST(planetId AS BOOLEAN) FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS VARCHAR) FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS TIMESTAMP) FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS INTEGER) FROM $satellites", 177, 1, None),
        ("SELECT DOUBLE(planetId) AS VALUE FROM $satellites GROUP BY DOUBLE(planetId)", 7, 1, None),
        ("SELECT INT(planetId) AS VALUE FROM $satellites GROUP BY INT(planetId)", 7, 1, None),
        ("SELECT INTEGER(planetId) AS VALUE FROM $satellites GROUP BY INTEGER(planetId)", 7, 1, None),
        ("SELECT CAST(planetId AS BOOLEAN) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT CAST(planetId AS VARCHAR) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT CAST('2022-01-0' || VARCHAR(planetId) AS TIMESTAMP) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT CAST(planetId AS INTEGER) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS BOOLEAN) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS VARCHAR) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS TIMESTAMP) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS DECIMAL) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT * FROM $planets WHERE id = GET('{\"a\":1,\"b\":\"c\"}', 'a')", 1, 20, None),
        ("SELECT * FROM $planets WHERE '{\"a\":1,\"b\":\"c\"}'->'a' = id", 1, 20, None),
        ("SELECT b'binary'", 1, 1, None),
        ("SELECT B'binary'", 1, 1, None),
        ("SELECT b'bi\nary'", 1, 1, None),
        ("SELECT b'binary\'", 1, 1, None),
        ("SELECT * FROM $planets WHERE name = b'Earth';", 1, 20, None),
        ("SELECT * FROM $planets WHERE name = b'Earth\';", 1, 20, None),
        ("SELECT * FROM $planets WHERE name = b'Ea\rth';", 0, 20, None),
        ("SELECT TIMESTAMP(1700000000000000)", 1, 1, None),
        ("SELECT CAST(1700000000000000 AS TIMESTAMP)", 1, 1, None),
        ("SELECT 1700000000000000::TIMESTAMP", 1, 1, None),

        ("SELECT PI()", 1, 1, None),
        ("SELECT E()", 1, 1, None),
        ("SELECT PHI()", 1, 1, None),
        ("SELECT PI() AS pi", 1, 1, None),
        ("SELECT E() AS e", 1, 1, None),
        ("SELECT PHI() AS Phi", 1, 1, None),
        ("SELECT GET(name, 1) FROM $satellites GROUP BY planetId, GET(name, 1)", 56, 1, None),
        ("SELECT COUNT(*), ROUND(magnitude) FROM $satellites group by ROUND(magnitude)", 22, 2, None),
        ("SELECT ROUND(magnitude) FROM $satellites group by ROUND(magnitude)", 22, 1, None),
        ("SELECT ROUND(magnitude, 1) FROM $satellites group by ROUND(magnitude, 1)", 88, 1, None),
        ("SELECT VARCHAR(planetId), COUNT(*) FROM $satellites GROUP BY 1", 7, 2, ColumnNotFoundError),
        ("SELECT LEFT(name, 1), COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 21, 2, UnsupportedSyntaxError),
        ("SELECT LEFT(name, 2) as le, COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 87, 2, UnsupportedSyntaxError),
        ("SELECT RIGHT(name, 10), COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 177, 2, UnsupportedSyntaxError),
        ("SELECT RIGHT(name, 2) as le, COUNT(*) FROM $satellites GROUP BY 1 ORDER BY 2 DESC", 91, 2, UnsupportedSyntaxError),
        ("SELECT VARCHAR(planetId), COUNT(*) FROM $satellites GROUP BY VARCHAR(planetId)", 7, 2, None),
        ("SELECT LEFT(name, 1), COUNT(*) FROM $satellites GROUP BY LEFT(name, 1) ORDER BY 2 DESC", 21, 2, UnsupportedSyntaxError),
        ("SELECT LEFT(name, 1), COUNT(*) FROM $satellites GROUP BY name ORDER BY 2 DESC", 177, 2, UnsupportedSyntaxError),
        ("SELECT LEFT(name, 2) as le, COUNT(*) FROM $satellites GROUP BY LEFT(name, 2) ORDER BY 2 DESC", 87, 2, UnsupportedSyntaxError),
        ("SELECT LEFT(name, 2) as le, COUNT(*) FROM $satellites GROUP BY LEFT(name, 1)", 87, 2, ColumnNotFoundError),
        ("SELECT RIGHT(name, 10), COUNT(*) FROM $satellites GROUP BY RIGHT(name, 10) ORDER BY 2 DESC", 177, 2, UnsupportedSyntaxError),
        ("SELECT RIGHT(name, 2) as le, COUNT(*) FROM $satellites GROUP BY RIGHT(name, 2) ORDER BY 2 DESC", 91, 2, UnsupportedSyntaxError),
        ("SELECT RIGHT(name, 2) as le, COUNT(*) FROM $satellites GROUP BY le ORDER BY 2 DESC", 91, 2, UnsupportedSyntaxError),
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
        ("SELECT name FROM $planets ORDER BY name DESC", 9, 1, None),
        ("SELECT name FROM $planets ORDER BY name", 9, 1, None),
        ("SELECT name FROM $planets ORDER BY name ASC", 9, 1, None),
        ("SELECT name FROM $planets ORDER BY id DESC", 9, 1, None),
        ("SELECT name FROM $planets ORDER BY name", 9, 1, None),
        ("SELECT name FROM $planets AS P ORDER BY name", 9, 1, None),
        ("SELECT name FROM $planets AS P ORDER BY P.name", 9, 1, None),
        ("SELECT name FROM $planets AS P ORDER BY P.id", 9, 1, None),
        ("SELECT P.name FROM $planets AS P ORDER BY name", 9, 1, None),
        ("SELECT P.name FROM $planets AS P ORDER BY P.id", 9, 1, None),
        ("SELECT name, id FROM $planets ORDER BY name", 9, 2, None),
        ("SELECT name FROM $planets ORDER BY name, id", 9, 1, None),
        ("SELECT P1.name FROM $planets AS P1, $planets AS P2 ORDER BY P1.name", 81, 1, None),
        ("SELECT COUNT(name), AVG(id) FROM $planets ORDER BY AVG(id)", 1, 2, None),
        ("SELECT name, id*2 AS double_id FROM $planets ORDER BY double_id", 9, 2, None),
        ("SELECT name, id*2 FROM $planets ORDER BY id*2", 9, 2, None),
        ("SELECT name FROM (SELECT * FROM $planets) AS sub ORDER BY name", 9, 1, None),
        ("SELECT name FROM $planets ORDER BY LENGTH(name)", 9, 1, UnsupportedSyntaxError),
        ("SELECT name FROM $planets ORDER BY id + 1", 9, 1, None),
        ("SELECT 1 AS const, name FROM $planets ORDER BY const", 9, 2, None),
        ("SELECT 1 AS const, name FROM $planets ORDER BY name", 9, 2, None),

        ("SELECT planetId as pid FROM $satellites", 177, 1, None),
        ("SELECT planetId as pid, round(magnitude) FROM $satellites", 177, 2, None),
        ("SELECT planetId as pid, round(magnitude) as minmag FROM $satellites", 177, 2, None),
        ("SELECT planetId as pid, round(magnitude) as roundmag FROM $satellites", 177, 2, None),

        ("SELECT GET(birth_place, 'town') FROM $astronauts", 357, 1, None),
        ("SELECT GET(missions, 0) FROM $astronauts", 357, 1, None),
        ("SELECT name FROM $astronauts WHERE GET(missions, 0) IS NOT NULL", 334, 1, None),
        ("SELECT name FROM $astronauts WHERE GET(missions, 5) IS NOT NULL", 7, 1, None),
        ("SELECT GET(birth_place, 'town') FROM $astronauts WHERE GET(birth_place, 'town') = 'Warsaw'", 1, 1, None),
        ("SELECT COUNT(*), GET(birth_place, 'town') FROM $astronauts GROUP BY GET(birth_place, 'town')", 264, 2, None),
        ("SELECT birth_place->'town' FROM $astronauts", 357, 1, None),
        ("SELECT birth_place->'town' FROM $astronauts WHERE birth_place->'town' = 'Warsaw'", 1, 1, None),
        ("SELECT birth_place->'town' FROM $astronauts WHERE 'Warsaw' = birth_place->'town'", 1, 1, None),
        ("SELECT COUNT(*), birth_place->'town' FROM $astronauts GROUP BY birth_place->'town'", 264, 2, None),
        ("SELECT birth_place->>'town' FROM $astronauts", 357, 1, None),
        ("SELECT birth_place->>'town' FROM $astronauts WHERE birth_place->>'town' = 'Warsaw'", 1, 1, None),
        ("SELECT birth_place->>'town' FROM $astronauts WHERE 'Warsaw' == birth_place->>'town'", 1, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE birth_place->'state' == 'CA'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE birth_place->>'state' == 'CA'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE birth_place->'state' == 'CA' AND name != 'bob'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE 'CA' = birth_place->'state'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE 'CA' = birth_place->>'state'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE 'CA' == birth_place->'state' AND name != 'bob'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE name != 'bob' AND birth_place->'state' == 'CA'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE name != 'bob' AND 'CA' = birth_place->'state'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE name != 'bob' AND birth_place->>'state' == 'CA'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE name != 'bob' AND 'CA' = birth_place->>'state'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE name != 'bob' AND birth_place->'state' == b'CA'", 25, 1, None),
        ("SELECT birth_place FROM $astronauts WHERE name != 'bob' AND b'CA' = birth_place->'state'", 25, 1, None),
        ("SELECT COUNT(*), birth_place->>'town' FROM $astronauts GROUP BY birth_place->>'town'", 264, 2, None),
        ("SELECT BP->'state' FROM (SELECT VARCHAR(birth_place) AS BP FROM $astronauts) AS I", 357, 1, None),
        ("SELECT BP->>'state' FROM (SELECT VARCHAR(birth_place) AS BP FROM $astronauts) AS I", 357, 1, None),
        ("SELECT BP->>'address' FROM (SELECT VARCHAR(birth_place) AS BP FROM $astronauts) AS I", 357, 1, None),
        ("SELECT dict->>'list', dict->'list' AS thisisalongercolumnname, dict->>'once', dict->'once' FROM testdata.flat.struct", 6, 4, None),
        ("SELECT cve -> 'CVE_data_meta' ->> 'ASSIGNER' FROM testdata.flat.nvd limit 10", 10, 1, None),
        ("SELECT cve ->> 'CVE_data_meta' ->> 'ASSIGNER' FROM testdata.flat.nvd limit 10", 10, 1, None),
        ("SELECT cve -> 'CVE_data_meta' -> 'ASSIGNER' FROM testdata.flat.nvd limit 10", 10, 1, None),
        
        ("SELECT dict @? 'list' FROM testdata.flat.struct", 6, 1, None),
        ("SELECT birth_place @? 'town' FROM $astronauts", 357, 1, None),
        ("SELECT dict @? '$.list' FROM testdata.flat.struct", 6, 1, None),
        ("SELECT cve @? '$.CVE_data_meta.ASSIGNER' FROM testdata.flat.nvd LIMIT 10", 10, 1, None),
        ("SELECT cve @? '$.data_meta.ASSIGNER' FROM testdata.flat.nvd LIMIT 10", 10, 1, None),
        ("SELECT cve @? '$.CVE_data_meta' FROM testdata.flat.nvd LIMIT 10", 10, 1, None),
        ("SELECT cve @? 'CVE_data_meta' FROM testdata.flat.nvd LIMIT 10", 10, 1, None),
        ("SELECT cve @? '$.CVE_data_meta.REASSIGNER' FROM testdata.flat.nvd LIMIT 10", 10, 1, None),
        ("SELECT birth_place @? '$.town' FROM $astronauts", 357, 1, None),

        ("SELECT dict @? 'list' FROM testdata.flat.atquestion", 6, 1, None),  # List exists in all but id=5
        ("SELECT dict @? 'key' FROM testdata.flat.atquestion", 6, 1, None),  # Key exists in id=1 and id=4
        ("SELECT dict @? 'other_list' FROM testdata.flat.atquestion", 6, 1, None),  # Only exists in id=3
        ("SELECT dict @? '$.list[0]' FROM testdata.flat.atquestion", 6, 1, None),  # First element of list exists in id=1, id=2, id=6
        ("SELECT dict @? '$.list[2]' FROM testdata.flat.atquestion", 6, 1, None),  # Third element of list exists in id=1
        ("SELECT dict @? '$.nested_list[0].key' FROM testdata.flat.atquestion", 6, 1, None),  # Nested key exists in id=6
        ("SELECT dict @? '$.non_existent' FROM testdata.flat.atquestion", 6, 1, None),  # Non-existent key
        ("SELECT dict @? '$.list[100]' FROM testdata.flat.atquestion", 6, 1, None),  # Out-of-bounds index
        ("SELECT dict @? '$.nested_list[10]' FROM testdata.flat.atquestion", 6, 1, None),  # Non-existent nested list index
        ("SELECT dict @? '$.nested_list[0].non_existent' FROM testdata.flat.atquestion", 6, 1, None),  # Non-existent nested key
        ("SELECT nested @? '$.level1.key' FROM testdata.flat.atquestion", 6, 1, None),  # Key exists but null in id=2
        ("SELECT nested @? '$.level1.non_existent' FROM testdata.flat.atquestion", 6, 1, None),  # Non-existent key in level1
        ("SELECT nested @? '$.non_existent' FROM testdata.flat.atquestion", 6, 1, None),  # Completely missing structure
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? 'list'", 4, 1, None),  # Rows where 'list' exists
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? 'key'", 2, 1, None),  # Rows where 'key' exists
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? 'other_list'", 1, 1, None),  # Rows where 'other_list' exists
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.list[0]'", 3, 1, None),  # Rows where the first element of 'list' exists
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.list[2]'", 1, 1, None),  # Rows where the third element of 'list' exists
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.nested_list[0].key'", 1, 1, None),  # Rows where 'nested_list[0].key' exists
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.non_existent'", 0, 1, None),  # No rows should match
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.list[100]'", 0, 1, None),  # No rows should match
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.nested_list[10]'", 0, 1, None),  # No rows should match
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.nested_list[0].non_existent'", 0, 1, None),  # No rows should match
        ("SELECT id FROM testdata.flat.atquestion WHERE nested @? '$.level1.key'", 4, 1, None),  # Rows where 'level1.key' exists (null is still considered existing)
        ("SELECT id FROM testdata.flat.atquestion WHERE nested @? '$.level1.non_existent'", 0, 1, None),  # No rows should match
        ("SELECT id FROM testdata.flat.atquestion WHERE nested @? '$.non_existent'", 0, 1, None),  # No rows should match
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.list[0]' LIMIT 2", 2, 1, None),  # Limit the matching rows to 2
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.list[0]' LIMIT 10", 3, 1, None),  # Limit exceeds matching rows
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? 'list'", 4, 1, None),  # Check existence of 'list'
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? 'key'", 2, 1, None),  # Check existence of 'key'
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.nested_list[0].key'", 1, 1, None),  # Check nested array structure
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.list[2]'", 1, 1, None),  # Index exists in one row
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.list[10]'", 0, 1, None),  # Out-of-bounds index
        ("SELECT id FROM testdata.flat.atquestion WHERE nested @? '$.level1.key'", 4, 1, None),  # Null value still exists
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? '$.key'", 2, 1, None),  # Key is present, but handle null
        ("SELECT COUNT(*) FROM testdata.flat.atquestion WHERE dict @? 'list'", 1, 1, None),  # Aggregation
        ("SELECT id FROM testdata.flat.atquestion WHERE dict @? 'list' AND dict @? 'key'", 2, 1, None),  # Compound condition
        ("SELECT id FROM testdata.flat.atquestion WHERE NOT dict @? 'list'", 2, 1, None),  # Negation
        ("SELECT id, COUNT(*) FROM testdata.flat.atquestion WHERE dict @? 'list' GROUP BY id", 4, 2, None),  # Group by

        ("SELECT birth_place['town'] FROM $astronauts", 357, 1, None),
        ("SELECT missions[0] FROM $astronauts", 357, 1, None),
        ("SELECT birth_place['town'] FROM $astronauts WHERE birth_place['town'] = 'Warsaw'", 1, 1, None),
        ("SELECT birth_place['town'] AS TOWN FROM $astronauts WHERE birth_place['town'] = 'Warsaw'", 1, 1, None),
        ("SELECT COUNT(*), birth_place['town'] FROM $astronauts GROUP BY birth_place['town']", 264, 2, None),
        ('SELECT LENGTH(missions) FROM $astronauts', 357, 1, None),
        ('SELECT LENGTH(missions) FROM $astronauts WHERE LENGTH(missions) > 6', 2, 1, None),
        ("SELECT jsonb_object_keys(birth_place) FROM $astronauts", 357, 1, None),
        ("SELECT DISTINCT key FROM (SELECT jsonb_object_keys(birth_place) as keys FROM $astronauts) AS set CROSS JOIN UNNEST(keys) AS key", 2, 1, None),
        ("SELECT jsonb_object_keys(dict) FROM testdata.flat.struct", 6, 1, None),
        ("SELECT DISTINCT key FROM (SELECT jsonb_object_keys(dict) as keys FROM testdata.flat.struct) AS set CROSS JOIN UNNEST(keys) AS key", 5, 1, None),

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
        ("SELECT CURRENT_TIMESTAMP", 1, 1, None),
        ("SELECT CURRENT_TIMESTAMP()", 1, 1, None),
        ("SELECT YEAR(birth_date), COUNT(*) FROM $astronauts GROUP BY YEAR(birth_date)", 54, 2, None),
        ("SELECT MONTH(birth_date), COUNT(*) FROM $astronauts GROUP BY MONTH(birth_date)", 12, 2, None),
        ("SELECT YEAR(), MONTH(), DAY(), HOUR(), MINUTE(), SECOND() FROM $planets", 9, 6, None),

        ("SELECT DATE_FORMAT(birth_date, '%d-%Y') FROM $astronauts", 357, 1, None),
        ("SELECT DATE_FORMAT(birth_date, 'dddd') FROM $astronauts", 357, 1, None),
        ("SELECT DATE_FORMAT(death_date, '%Y') FROM $astronauts", 357, 1, None),

        ("SELECT count(*), VARCHAR(year) FROM $astronauts GROUP BY VARCHAR(year)", 21, 2, None),
        ("SELECT count(*), CAST(year AS VARCHAR) FROM $astronauts GROUP BY CAST(year AS VARCHAR)", 21, 2, None),

        ("SELECT 1", 1, 1, None),
        ("SELECT 1.0", 1, 1, None),
        ("SELECT 'a'", 1, 1, None),
        ("SELECT TRUE", 1, 1, None),
        ("SELECT NULL", 1, 1, None),
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

        # We rewrite expressions like this, make sure all variations work
        ("SELECT * FROM $satellites WHERE id - 3 < 8;", 10, 8, None),
        ("SELECT * FROM $satellites WHERE 8 > id - 3;", 10, 8, None),
        ("SELECT * FROM $satellites WHERE 3 > 8 - id;", 172, 8, None),
        ("SELECT * FROM $satellites WHERE 8 - id < 3;", 172, 8, None),
        ("SELECT * FROM $satellites WHERE id < 8 + 3;", 10, 8, None),
        ("SELECT * FROM $satellites WHERE 8 + 3 > id;", 10, 8, None),

        # rewriting date functions has addition complexity
        ("SELECT * FROM $missions WHERE Launched_at - INTERVAL '7' DAY < current_timestamp;", 4503, 8, None),
        ("SELECT * FROM $missions WHERE current_timestamp > Launched_at - INTERVAL '7' DAY;", 4503, 8, None),
        ("SELECT * FROM $missions WHERE INTERVAL '7' DAY < current_timestamp - Launched_at;", 4503, 8, None),
        ("SELECT * FROM $missions WHERE current_timestamp - Launched_at > INTERVAL '7' DAY;", 4503, 8, None),
        ("SELECT * FROM $missions WHERE Launched_at < current_timestamp + INTERVAL '7' DAY;", 4503, 8, None),
        ("SELECT * FROM $missions WHERE current_timestamp > Launched_at + INTERVAL '7' DAY;", 4503, 8, None),

        ("SELECT ARRAY_AGG(id) FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(id) AS pids FROM $satellites GROUP BY planetId) AS sats", 7, 1, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats", 7, 2, None),
        ("SELECT * FROM $planets INNER JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats ON sats.planetId = $planets.id", 7, 22, None),
        ("SELECT * FROM $planets LEFT JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats ON sats.planetId = $planets.id", 9, 22, None),
        ("SELECT * FROM (SELECT * FROM $planets LEFT JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats ON sats.planetId = $planets.id) as satellites", 9, 22, None),
        ("SELECT pids FROM (SELECT * FROM $planets LEFT JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats ON sats.planetId = $planets.id) as satellites", 9, 1, None),
        ("SELECT pid FROM (SELECT * FROM $planets LEFT JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats ON sats.planetId = $planets.id) as satellites CROSS JOIN UNNEST(pids) AS pid", 177, 1, None),
        ("SELECT * FROM (SELECT LENGTH(ARRAY_AGG(DISTINCT planetId)) AS L FROM $satellites GROUP BY planetId) AS I WHERE L == 1;", 7, 1, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(id) AS sid FROM $satellites GROUP BY planetId) AS A WHERE sid @> (1,2,3)", 2, 1, None),

        ("SHOW CREATE VIEW mission_reports", 1, 1, None),
        ("SHOW CREATE VIEW mission.reports", 1, 1, DatasetNotFoundError),
        ("SHOW CREATE TABLE mission_reports", 1, 1, UnsupportedSyntaxError),

        ("SELECT name FROM (SELECT MD5(name) AS hash, name FROM $planets) AS S", 9, 1, None),

        ("SELECT jsonb_object_keys(birth_place) FROM $astronauts", 357, 1, None),
        ("SELECT jsonb_object_keys(VARCHAR(birth_place)) FROM $astronauts", 357, 1, None),
        ("SELECT jsonb_object_keys(BLOB(birth_place)) FROM $astronauts", 357, 1, None),
        ("SELECT jsonb_object_keys(birth_place) FROM testdata.astronauts", 357, 1, None),
        ("SELECT jsonb_object_keys(VARCHAR(birth_place)) FROM testdata.astronauts", 357, 1, None),
        ("SELECT jsonb_object_keys(BLOB(birth_place)) FROM testdata.astronauts", 357, 1, None),

        ("SELECT VARCHAR(SUBSTRING(BLOB(birth_date) FROM -4)) FROM $astronauts", 357, 1, None),
        ("SELECT SUBSTRING(BLOB(birth_date) FROM -4) FROM $astronauts", 357, 1, None),
        ("SELECT SUBSTRING(name FROM 4) FROM $astronauts", 357, 1, None),
        ("SELECT SUBSTRING(name FROM 1 FOR 1) FROM $astronauts", 357, 1, None),
        ("SELECT SUBSTRING(name FROM -1 FOR 1) FROM $astronauts", 357, 1, None),

        ("SELECT * FROM $planets LEFT JOIN $satellites USING(id) WHERE False", 0, 27, None),
        ("SELECT * FROM (SELECT * FROM $planets WHERE False) AS S LEFT JOIN $satellites USING(id)", 0, 27, None),
        ("SELECT * FROM $planets LEFT JOIN (SELECT * FROM $satellites WHERE False) AS S USING(id)", 9, 27, None),

        ("WITH cold_planets AS (SELECT id, name, meanTemperature FROM $planets WHERE meanTemperature < 0) SELECT name FROM cold_planets WHERE name != 'Uranus';", 5, 1, None),
        ("WITH eccentric_planets AS (SELECT id, name, orbitalEccentricity FROM $planets WHERE orbitalEccentricity > 0.05) SELECT name FROM eccentric_planets WHERE orbitalEccentricity < 0.1 ORDER BY name;", 2, 1, None),
        ("WITH warm AS (SELECT id, name, meanTemperature FROM $planets WHERE meanTemperature > 0) SELECT COUNT(*) FROM warm WHERE meanTemperature > 200;", 1, 1, None),
        ("WITH rounded_gravity AS (SELECT id, name, ROUND(gravity, 0) AS g FROM $planets) SELECT name FROM rounded_gravity WHERE g >= 10;", 3, 1, None),
        ("WITH giant_planets AS (SELECT id, name, diameter FROM $planets WHERE diameter > 100000) SELECT name, diameter FROM giant_planets ORDER BY diameter ASC LIMIT 2;", 2, 2, None),
        ("WITH polar AS (SELECT id, name, obliquityToOrbit FROM $planets) SELECT name FROM polar WHERE obliquityToOrbit BETWEEN 85 AND 95;", 0, 1, None),
        ("WITH dense AS (SELECT id, name, density FROM $planets WHERE density IS NOT NULL) SELECT name FROM dense WHERE density > 4 AND density < 6;", 0, 1, None),
        ("WITH lo_temp AS (SELECT id, name, meanTemperature FROM $planets) SELECT name FROM lo_temp WHERE meanTemperature < -100 ORDER BY meanTemperature DESC;", 5, 1, None),
        ("WITH orbit_years AS (SELECT id, name, orbitalPeriod FROM $planets) SELECT name FROM orbit_years WHERE orbitalPeriod > 365 ORDER BY orbitalPeriod;", 7, 1, None),
        ("WITH spin AS (SELECT id, name, rotationPeriod FROM $planets) SELECT name FROM spin WHERE rotationPeriod > 500 ORDER BY rotationPeriod;", 1, 1, None),
        ("WITH flat_names AS (SELECT id, LOWER(name) AS name FROM $planets) SELECT name FROM flat_names WHERE name LIKE 'm%' ORDER BY name;", 2, 1, None),
        ("WITH far_planets AS (SELECT id, name, distanceFromSun FROM $planets WHERE distanceFromSun > 1000) SELECT name FROM far_planets WHERE distanceFromSun < 5000;", 3, 1, None),
        ("WITH odd_moons AS (SELECT id, name, numberOfMoons FROM $planets) SELECT name FROM odd_moons WHERE numberOfMoons % 2 = 1;", 4, 1, None),
        ("WITH names AS (SELECT id, name FROM $planets) SELECT UPPER(name) FROM names WHERE LENGTH(name) < 6;", 4, 1, None),
        ("WITH nonzero_pressure AS (SELECT id, surfacePressure FROM $planets) SELECT COUNT(*) FROM nonzero_pressure WHERE surfacePressure > 0;", 1, 1, None),
        ("WITH normalized AS (SELECT id, name, round(CAST(gravity AS DOUBLE) / 9.8, 2) AS g  FROM $planets WHERE gravity IS NOT NULL) SELECT name FROM normalized WHERE g > 1.1;", 2, 1, None),
        ("WITH weird_rotation AS (SELECT id, name, rotationPeriod FROM $planets) SELECT name FROM weird_rotation WHERE rotationPeriod < 0 ORDER BY rotationPeriod;", 3, 1, None),
        ("WITH matching AS (SELECT id, name FROM $planets) SELECT COUNT(*) FROM matching WHERE name = 'Mars';", 1, 1, None),
        ("WITH categorised AS (SELECT id, name, CASE WHEN mass > 1 THEN 'large' ELSE 'small' END AS size FROM $planets) SELECT size, COUNT(*) FROM categorised GROUP BY size;", 2, 2, None),
        ("WITH orbshape AS (SELECT id, orbitalEccentricity FROM $planets) SELECT COUNT(*) FROM orbshape WHERE orbitalEccentricity BETWEEN 0.01 AND 0.05;", 1, 1, None),

        # Complex GROUP BY with Multiple Expressions
        ("SELECT COUNT(*), LENGTH(name), ROUND(density, 2) FROM $planets GROUP BY LENGTH(name), ROUND(density, 2)", 9, 3, None),
        # Testing Intervals with Arithmetic Expressions
        ("SELECT * FROM $planets WHERE TIMESTAMP '2024-10-01' + INTERVAL '2' DAY > current_timestamp", 0, 20, None),
]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery(statement:str, rows:int, columns:int, exception: Optional[Exception]):
    """
    Test a battery of statements
    """
    from tests import set_up_iceberg
    from opteryx.connectors import IcebergConnector
    iceberg = set_up_iceberg()
    opteryx.register_store("iceberg", connector=IcebergConnector, catalog=iceberg)

    from opteryx.connectors import DiskConnector, SqlConnector
    from opteryx.managers.schemes import MabelPartitionScheme

    opteryx.register_store(
        "testdata.partitioned", DiskConnector, partition_scheme=MabelPartitionScheme
    )
    opteryx.register_store(
        "sqlite",
        SqlConnector,
        remove_prefix=True,
        connection="sqlite:///testdata/sqlite/database.db",
    )

    try:
        # query to arrow is the fastest way to query
        result = opteryx.query_to_arrow(statement, memberships=["Apollo 11", "opteryx"])
        actual_rows, actual_columns = result.shape
        assert (
            rows == actual_rows
        ), f"\n\033[38;5;203mQuery returned {actual_rows} rows but {rows} were expected.\033[0m\n{statement}"
        assert (
            columns == actual_columns
        ), f"\n\033[38;5;203mQuery returned {actual_columns} cols but {columns} were expected.\033[0m\n{statement}"
        assert (
            exception is None
        ), f"Exception {exception} not raised but expected\n{format_sql(statement)}"
    except AssertionError as error:
        raise error
    except Exception as error:
        if not type(error) == exception:
            raise ValueError(
                f"{format_sql(statement)}\nQuery failed with error {type(error)} but error {exception} was expected"
            ) from error


if __name__ == "__main__":  # pragma: no cover
    import shutil
    import time
    from tests import trunc_printable

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 15
    passed:int = 0
    failed:int = 0
    nl:str = "\n"
    failures = []

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} OPERATORS_EXPRESSIONS SHAPE TESTS")
    for index, (statement, rows, cols, err) in enumerate(STATEMENTS):
        printable = statement
        if hasattr(printable, "decode"):
            printable = printable.decode()
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(printable), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_sql_battery(statement, rows, cols, err)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(f" \033[0;31m{failed}\033[0m")
            else:
                print()
        except Exception as err:
            failed += 1
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ {failed}\033[0m")
            print(">", err)
            failures.append((statement, err))

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for statement, err in failures:
            print(err)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )

    # Exit with appropriate code to signal success/failure to parent process
    if failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)
