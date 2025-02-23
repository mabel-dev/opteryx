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
import pytest
import sys

from typing import Optional

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

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
STATEMENTS = [
        # Are the datasets the shape we expect?
        ("SELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $planets", 9, 20, None),
        ("SELECT * FROM $astronauts", 357, 19, None),
        ("SELECT * FROM $no_table", 1, 1, None),
        ("SELECT * FROM sqlite.planets", 9, 20, None),
        ("SELECT * FROM $variables", 42, 5, None),
        ("SELECT * FROM $missions", 4630, 8, None),
        ("SELECT * FROM $statistics", 17, 2, None),
        ("SELECT * FROM $stop_words", 305, 1, None),
        (b"SELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM testdata.missions", 4630, 8, None),
        ("SELECT * FROM testdata.satellites", 177, 8, None),
        ("SELECT * FROM testdata.planets", 9, 20, None),

        ("SELECT COUNT(*) FROM testdata.missions", 1, 1, None),
        ("SELECT COUNT(*) FROM testdata.satellites", 1, 1, None),
        ("SELECT COUNT(*) FROM testdata.planets", 1, 1, None),

        # Does the error tester work
        ("THIS IS NOT VALID SQL", None, None, SqlError),

        # Randomly generated but consistently tested queries (note we have a fuzzer in the full suite)
        ("SELECT * FROM $planets WHERE `name` = 'Earth'", 1, 20, None),
        ("SELECT * FROM $planets WHERE name = 'Mars'", 1, 20, None),
        ("SELECT * FROM $planets WHERE name <> 'Venus'", 8, 20, None),
        ("SELECT * FROM $planets WHERE name = '********'", 0, 20, None),
        ("SELECT id FROM $planets WHERE diameter > 10000", 6, 1, None),
        ("SELECT name, mass FROM $planets WHERE gravity > 5", 6, 2, None),
        ("SELECT * FROM $planets WHERE numberOfMoons >= 5", 5, 20, None),
        ("SELECT * FROM $planets WHERE mass < 10 AND diameter > 1000", 5, 20, None),
        ("SELECT * FROM $planets ORDER BY distanceFromSun DESC", 9, 20, None),
        ("SELECT name FROM $planets WHERE escapeVelocity < 10", 3, 1, None),
        ("SELECT * FROM $planets WHERE obliquityToOrbit > 25", 6, 20, None),
        ("SELECT * FROM $planets WHERE meanTemperature < 0", 6, 20, None),
        ("SELECT * FROM $planets WHERE surfacePressure IS NULL", 4, 20, None),
        ("SELECT name FROM $planets WHERE orbitalEccentricity < 0.1", 7, 1, None),
        ("SELECT * FROM $planets WHERE orbitalPeriod BETWEEN 100 AND 1000", 3, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) <> 5", 6, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) == 5", 3, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) != 5", 6, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM $planets WHERE NOT LENGTH(name) = 5", 6, 20, None),
        ("SELECT * FROM $planets WHERE NOT LENGTH(name) == 5", 6, 20, None),
        ("SELECT * FROM $planets LIMIT 5", 5, 20, None),
        ("SELECT * FROM $planets WHERE numberOfMoons = 0", 2, 20, None),
        ("SELECT id FROM $planets WHERE density > 4000 ORDER BY id ASC", 3, 1, None),
        ("SELECT * FROM $planets WHERE gravity > 10 OR meanTemperature > 100", 4, 20, None),
        ("SELECT * FROM $planets WHERE orbitalInclination <= 5", 7, 20, None),
        ("SELECT * FROM $planets WHERE perihelion >= 150", 6, 20, None),
        ("SELECT name FROM $planets WHERE aphelion < 1000", 5, 1, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) >= 7", 3, 20, None),
        ("SELECT * FROM $planets WHERE id IN (1, 3, 5)", 3, 20, None),
        ("SELECT * FROM $planets WHERE id NOT IN (1, 3, 5)", 6, 20, None),
        ("SELECT * FROM $planets WHERE rotationPeriod < 0", 3, 20, None),
        ("SELECT * FROM $planets WHERE mass > 100 AND mass < 1000", 2, 20, None),
        ("SELECT * FROM $planets WHERE name LIKE 'M%'", 2, 20, None),
        ("SELECT * FROM $planets WHERE orbitalVelocity > 20", 4, 20, None),
        ("SELECT * FROM $planets WHERE escapeVelocity BETWEEN 10 AND 20", 2, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) <= 6", 6, 20, None),
        ("SELECT * FROM $planets ORDER BY id DESC LIMIT 3", 3, 20, None),
        ("SELECT * FROM $planets WHERE gravity IS NOT NULL", 9, 20, None),
        ("SELECT * FROM $planets WHERE surfacePressure IS NOT NULL", 5, 20, None),
        ("SELECT * FROM $planets WHERE meanTemperature <= 100", 7, 20, None),
        ("SELECT * FROM $planets WHERE numberOfMoons > 10", 4, 20, None),
        ("SELECT * FROM $planets WHERE LENGTH(name) > 7", 0, 20, None),
        ("SELECT * FROM $planets WHERE distanceFromSun BETWEEN 100 AND 1000", 4, 20, None),

        # Randomly generated but consistently tested queries
        # the same queries as above, but against parquet, which has a complex reader and
        # is the most used in active deployments
        ("SELECT * FROM testdata.planets WHERE `name` = 'Earth'", 1, 20, None),
        ("SELECT * FROM testdata.planets WHERE name = 'Mars'", 1, 20, None),
        ("SELECT * FROM testdata.planets WHERE name <> 'Venus'", 8, 20, None),
        ("SELECT * FROM testdata.planets WHERE name = '********'", 0, 20, None),
        ("SELECT id FROM testdata.planets WHERE diameter > 10000", 6, 1, None),
        ("SELECT name, mass FROM testdata.planets WHERE gravity > 5", 6, 2, None),
        ("SELECT * FROM testdata.planets WHERE numberOfMoons >= 5", 5, 20, None),
        ("SELECT * FROM testdata.planets WHERE mass < 10 AND diameter > 1000", 5, 20, None),
        ("SELECT * FROM testdata.planets ORDER BY distanceFromSun DESC", 9, 20, None),
        ("SELECT name FROM testdata.planets WHERE escapeVelocity < 10", 3, 1, None),
        ("SELECT * FROM testdata.planets WHERE obliquityToOrbit > 25", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE meanTemperature < 0", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE surfacePressure IS NULL", 4, 20, None),
        ("SELECT name FROM testdata.planets WHERE orbitalEccentricity < 0.1", 7, 1, None),
        ("SELECT * FROM testdata.planets WHERE orbitalPeriod BETWEEN 100 AND 1000", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) <> 5", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) == 5", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) != 5", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE NOT LENGTH(name) = 5", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE NOT LENGTH(name) == 5", 6, 20, None),
        ("SELECT * FROM testdata.planets LIMIT 5", 5, 20, None),
        ("SELECT * FROM testdata.planets WHERE numberOfMoons = 0", 2, 20, None),
        ("SELECT id FROM testdata.planets WHERE density > 4000 ORDER BY id ASC", 3, 1, None),
        ("SELECT * FROM testdata.planets WHERE gravity > 10 OR meanTemperature > 100", 4, 20, None),
        ("SELECT * FROM testdata.planets WHERE orbitalInclination <= 5", 7, 20, None),
        ("SELECT * FROM testdata.planets WHERE perihelion >= 150", 6, 20, None),
        ("SELECT name FROM testdata.planets WHERE aphelion < 1000", 5, 1, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) >= 7", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE id IN (1, 3, 5)", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE id NOT IN (1, 3, 5)", 6, 20, None),
        ("SELECT * FROM testdata.planets WHERE rotationPeriod < 0", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE mass > 100 AND mass < 1000", 2, 20, None),
        ("SELECT * FROM testdata.planets WHERE name LIKE 'M%'", 2, 20, None),
        ("SELECT * FROM testdata.planets WHERE orbitalVelocity > 20", 4, 20, None),
        ("SELECT * FROM testdata.planets WHERE escapeVelocity BETWEEN 10 AND 20", 2, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) <= 6", 6, 20, None),
        ("SELECT * FROM testdata.planets ORDER BY id DESC LIMIT 3", 3, 20, None),
        ("SELECT * FROM testdata.planets WHERE gravity IS NOT NULL", 9, 20, None),
        ("SELECT * FROM testdata.planets WHERE surfacePressure IS NOT NULL", 5, 20, None),
        ("SELECT * FROM testdata.planets WHERE meanTemperature <= 100", 7, 20, None),
        ("SELECT * FROM testdata.planets WHERE numberOfMoons > 10", 4, 20, None),
        ("SELECT * FROM testdata.planets WHERE LENGTH(name) > 7", 0, 20, None),
        ("SELECT * FROM testdata.planets WHERE distanceFromSun BETWEEN 100 AND 1000", 4, 20, None),

        # Randomly generated but consistently tested queries
        # the same queries as above, but against iceberg, which we anticipate will be our 
        # most utilized data source
        ("SELECT * FROM iceberg.planets WHERE `name` = 'Earth'", 1, 20, None),
        ("SELECT * FROM iceberg.planets WHERE name = 'Mars'", 1, 20, None),
        ("SELECT * FROM iceberg.planets WHERE name <> 'Venus'", 8, 20, None),
        ("SELECT * FROM iceberg.planets WHERE name = '********'", 0, 20, None),
        ("SELECT id FROM iceberg.planets WHERE diameter > 10000", 6, 1, None),
        ("SELECT name, mass FROM iceberg.planets WHERE gravity > 5", 6, 2, None),
        ("SELECT * FROM iceberg.planets WHERE numberOfMoons >= 5", 5, 20, None),
        ("SELECT * FROM iceberg.planets WHERE mass < 10 AND diameter > 1000", 5, 20, None),
        ("SELECT * FROM iceberg.planets ORDER BY distanceFromSun DESC", 9, 20, None),
        ("SELECT name FROM iceberg.planets WHERE escapeVelocity < 10", 3, 1, None),
        ("SELECT * FROM iceberg.planets WHERE obliquityToOrbit > 25", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE meanTemperature < 0", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE surfacePressure IS NULL", 4, 20, None),
        ("SELECT name FROM iceberg.planets WHERE orbitalEccentricity < 0.1", 7, 1, None),
        ("SELECT * FROM iceberg.planets WHERE orbitalPeriod BETWEEN 100 AND 1000", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) <> 5", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) == 5", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) != 5", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) = 5", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE NOT LENGTH(name) = 5", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE NOT LENGTH(name) == 5", 6, 20, None),
        ("SELECT * FROM iceberg.planets LIMIT 5", 5, 20, None),
        ("SELECT * FROM iceberg.planets WHERE numberOfMoons = 0", 2, 20, None),
        ("SELECT id FROM iceberg.planets WHERE density > 4000 ORDER BY id ASC", 3, 1, None),
        ("SELECT * FROM iceberg.planets WHERE gravity > 10 OR meanTemperature > 100", 4, 20, None),
        ("SELECT * FROM iceberg.planets WHERE orbitalInclination <= 5", 7, 20, None),
        ("SELECT * FROM iceberg.planets WHERE perihelion >= 150", 6, 20, None),
        ("SELECT name FROM iceberg.planets WHERE aphelion < 1000", 5, 1, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) >= 7", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE id IN (1, 3, 5)", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE id NOT IN (1, 3, 5)", 6, 20, None),
        ("SELECT * FROM iceberg.planets WHERE rotationPeriod < 0", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE mass > 100 AND mass < 1000", 2, 20, None),
        ("SELECT * FROM iceberg.planets WHERE name LIKE 'M%'", 2, 20, None),
        ("SELECT * FROM iceberg.planets WHERE orbitalVelocity > 20", 4, 20, None),
        ("SELECT * FROM iceberg.planets WHERE escapeVelocity BETWEEN 10 AND 20", 2, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) <= 6", 6, 20, None),
        ("SELECT * FROM iceberg.planets ORDER BY id DESC LIMIT 3", 3, 20, None),
        ("SELECT * FROM iceberg.planets WHERE gravity IS NOT NULL", 9, 20, None),
        ("SELECT * FROM iceberg.planets WHERE surfacePressure IS NOT NULL", 5, 20, None),
        ("SELECT * FROM iceberg.planets WHERE meanTemperature <= 100", 7, 20, None),
        ("SELECT * FROM iceberg.planets WHERE numberOfMoons > 10", 4, 20, None),
        ("SELECT * FROM iceberg.planets WHERE LENGTH(name) > 7", 0, 20, None),
        ("SELECT * FROM iceberg.planets WHERE distanceFromSun BETWEEN 100 AND 1000", 4, 20, None),

        # Some tests of the same query in different formats
        ("SELECT * FROM $satellites;", 177, 8, None),
        ("SELECT * FROM $satellites\n;", 177, 8, None),
        ("select * from $satellites", 177, 8, None),
        ("Select * From $satellites", 177, 8, None),
        ("SELECT   *   FROM   $satellites", 177, 8, None),
        ("SELECT\n\t*\n  FROM\n\t$satellites", 177, 8, None),
        ("\n\n\n\tSELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites\t;", 177, 8, None),
        ("SELECT *\tFROM $satellites", 177, 8, None),
        ("  SELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites  ", 177, 8, None),
        ("SELECT * \n\n\n FROM $satellites", 177, 8, None),
        ("SeLeCt * FrOm $satellites", 177, 8, None),
        ("SeLeCt * fRoM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites;  ", 177, 8, None),
        ("\t\tSELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites  ;", 177, 8, None),
        ("SELECT * FROM $satellites\n\t;", 177, 8, None),
        ("SeLeCt * FrOm $satellites ;", 177, 8, None),
        ("SELECT\t*\tFROM\t$satellites\t;", 177, 8, None),
        ("  SELECT  *  FROM  $satellites  ;  ", 177, 8, None),
        ("SELECT*\nFROM$satellites", 177, 8, None),
        ("SELECT*FROM$satellites", 177, 8, None),
        ("SELECT *\r\nFROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites\r\n", 177, 8, None),
        ("SELECT * FROM $satellites\r\n;", 177, 8, None),
        ("SeLEcT * fROm $satellites", 177, 8, None),
        ("sElEcT * FrOm $satellites;", 177, 8, None),
        ("\n\r\nSELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites\n\r\n;", 177, 8, None),
        ("\r\nSELECT * FROM $satellites\r\n", 177, 8, None),
        ("SELECT * FROM $satellites\t\r\n;", 177, 8, None),
        ("  \t  SELECT  *  FROM  $satellites  \t  ;  \t  ", 177, 8, None),
        ("SELECT * \n/* comment */\n FROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites\n-- comment\n;", 177, 8, None),
        ("/* comment */SELECT * FROM $satellites--comment", 177, 8, None),
        ("SELECT * FROM $satellites;--comment", 177, 8, None),
        ("SELECT * --comment\nFROM $satellites", 177, 8, None),
        ("SELECT * FROM $satellites --comment\n;", 177, 8, None),
        ("""
/* This is a comment */
SELECT *
/* This is a multiline
comment
*/
FROM --
$planets
WHERE /* FALSE AND */
/* FALSE -- */
id > /* 0 */ 1
-- AND name = 'Earth')
         """, 8, 20, None),

        # basic test of the operators
        ("SELECT $satellites.* FROM $satellites", 177, 8, None),
        ("SELECT s.* FROM $satellites AS s", 177, 8, None),
        ("SELECT * FROM $satellites WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE (name = 'Calypso')", 1, 8, None),
        ("SELECT * FROM $satellites WHERE NOT name = 'Calypso'", 176, 8, None),
        ("SELECT * FROM $satellites WHERE NOT (name = 'Calypso')", 176, 8, None),
        ("SELECT * FROM $satellites WHERE `name` = 'Calypso'", 1, 8, None),
        ("select * from $satellites where name = 'Calypso'", 1, 8, None),
        ("select * from $satellites where name == 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name <> 'Calypso'", 176, 8, None),
        ("SELECT * FROM $satellites WHERE name < 'Calypso'", 21, 8, None),
        ("SELECT * FROM $satellites WHERE name <= 'Calypso'", 22, 8, None),
        ("SELECT * FROM $satellites WHERE name > 'Calypso'", 155, 8, None),
        ("SELECT * FROM $satellites WHERE name >= 'Calypso'", 156, 8, None),
        ("SELECT * FROM $satellites WHERE name is null", 0, 8, None),
        ("SELECT * FROM $satellites WHERE not name is null", 177, 8, None),
        ("SELECT * FROM $satellites WHERE name is not null", 177, 8, None),
        ("SELECT * FROM $satellites WHERE name is true", 0, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE not name is true", 177, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE name is not true", 177, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE name is false", 0, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE not name is false", 177, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE name is not false", 177, 8, IncorrectTypeError),
        ("SELECT * FROM $satellites WHERE name != 'Calypso'", 176, 8, None),
        ("SELECT * FROM $satellites WHERE name = '********'", 0, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE '_a_y_s_'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE 'Cal%%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name LIKE 'Cal%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name like 'Cal%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name ILIKE '_a_y_s_'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name ILIKE 'Cal%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name ilike 'Cal%'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name RLIKE '.a.y.s.'", 1, 8, None),
        ("SELECT * FROM $satellites WHERE name RLIKE '^Cal.*'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE name rlike '^Cal.*'", 4, 8, None),
        ("SELECT * FROM $satellites WHERE TRUE", 177, 8, None),
        ("SELECT * FROM $satellites WHERE FALSE", 0, 8, None),
        ("SELECT * FROM $satellites WHERE NOT TRUE", 0, 8, None),
        ("SELECT * FROM $satellites WHERE NOT FALSE", 177, 8, None),
        ("SELECT * FROM $satellites WHERE `name` = 'Calypso'", 1, 8, None),
        ("SELECT * FROM `$satellites` WHERE name = 'Calypso'", 1, 8, None),
        ("SELECT * FROM `$satellites` WHERE `name` = 'Calypso'", 1, 8, None),
        ("SELECT * FROM $satellites WITH (NO_CACHE)", 177, 8, None),

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
        ("SELECT NUMERIC(planetId) AS VALUE FROM $satellites GROUP BY NUMERIC(planetId)", 7, 1, None),
        ("SELECT INT(planetId) AS VALUE FROM $satellites GROUP BY INT(planetId)", 7, 1, None),
        ("SELECT INTEGER(planetId) AS VALUE FROM $satellites GROUP BY INTEGER(planetId)", 7, 1, None),
        ("SELECT FLOAT(planetId) AS VALUE FROM $satellites GROUP BY FLOAT(planetId)", 7, 1, None),
        ("SELECT CAST(planetId AS BOOLEAN) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT CAST(planetId AS VARCHAR) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT CAST('2022-01-0' || VARCHAR(planetId) AS TIMESTAMP) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT CAST(planetId AS INTEGER) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS BOOLEAN) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS VARCHAR) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS TIMESTAMP) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT TRY_CAST(planetId AS DECIMAL) AS VALUE FROM $satellites", 177, 1, None),
        ("SELECT * FROM $planets WHERE id = GET(STRUCT('{\"a\":1,\"b\":\"c\"}'), 'a')", 1, 20, None),
        ("SELECT * FROM $planets WHERE STRUCT('{\"a\":1,\"b\":\"c\"}')->'a' = id", 1, 20, None),
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
        ("SELECT dict->>'list', dict->'list' AS thisisalongercolumnname, STRUCT(dict)->'list', dict->>'once', dict->'once' FROM testdata.flat.struct", 6, 5, None),
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
        ("SELECT struct(dict) @? '$.list' FROM testdata.flat.struct", 6, 1, None),
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
        ("SELECT YEAR(birth_date), COUNT(*) FROM $astronauts GROUP BY YEAR(birth_date)", 54, 2, None),
        ("SELECT MONTH(birth_date), COUNT(*) FROM $astronauts GROUP BY MONTH(birth_date)", 12, 2, None),

        ("SELECT DATE_FORMAT(birth_date, '%d-%Y') FROM $astronauts", 357, 1, None),
        ("SELECT DATE_FORMAT(birth_date, 'dddd') FROM $astronauts", 357, 1, None),
        ("SELECT DATE_FORMAT(death_date, '%Y') FROM $astronauts", 357, 1, None),

        ("SELECT count(*), VARCHAR(year) FROM $astronauts GROUP BY VARCHAR(year)", 21, 2, None),
        ("SELECT count(*), STRING(year) FROM $astronauts GROUP BY STRING(year)", 21, 2, None),
        ("SELECT count(*), STR(year) FROM $astronauts GROUP BY STR(year)", 21, 2, None),
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

        # Test Aliases
        ("SELECT planet_id FROM $satellites", 177, 1, None),
        ("SELECT escape_velocity, gravity, orbitalPeriod FROM $planets", 9, 3, None),

        ("SELECT * FROM (VALUES ('High', 3),('Medium', 2),('Low', 1)) AS ratings(name, rating)", 3, 2, None),
        ("SELECT * FROM (VALUES ('High', 3),('Medium', 2),('Low', 1)) AS ratings(name, rating) WHERE rating = 3", 1, 2, None),

        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred'))", 8, 1, UnnamedColumnError),
        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred')) AS element", 8, 1, None),
        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred')) AS element WHERE element LIKE '%e%'", 2, 1, None),
        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred')) AS UN", 8, 1, None),
        ("SELECT * FROM UNNEST(('foo', 'bar', 'baz', 'qux', 'corge', 'garply', 'waldo', 'fred')) AS UN WHERE UN LIKE '%e%'", 2, 1, None),
        ("SELECT * FROM $astronauts LEFT JOIN UNNEST(missions) as s", None, None, UnsupportedSyntaxError),

        ("SELECT * FROM generate_series(1, 10)", 10, 1, UnnamedColumnError),
        ("SELECT * FROM generate_series(1, 10) AS GS", 10, 1, None),
        ("SELECT * FROM generate_series(-10,10) AS GS", 21, 1, None),
        ("SELECT * FROM generate_series(2,10,2) AS GS", 5, 1, None),
        ("SELECT * FROM generate_series(0.5,10,0.5) AS GS", 20, 1, None),
        ("SELECT * FROM generate_series(2,11,2) AS GS", 5, 1, None),
        ("SELECT * FROM generate_series(1, 10, 0.5) AS GS", 19, 1, None),
        ("SELECT * FROM generate_series(0.1, 0.2, 10) AS GS", 1, 1, None),
        ("SELECT * FROM generate_series(0, 5, 1.1) AS GS", 5, 1, None),
        ("SELECT * FROM generate_series(2,10,2) AS nums", 5, 1, None),
        ("SELECT * FROM generate_series(2,10,2) AS GS WHERE GS > 5", 3, 1, None),
        ("SELECT * FROM generate_series(2,10,2) AS nums WHERE nums < 5", 2, 1, None),
        ("SELECT * FROM generate_series(2) AS GS WITH (NO_CACHE)", 2, 1, None),
        ("SELECT * FROM generate_series('192.168.0.0/24') AS GS WITH (NO_CACHE)", 2, 1, InvalidFunctionParameterError),

        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 month') AS GS", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 mon') AS GS", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1mon') AS GS", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1mo') AS GS", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1mth') AS GS", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 months') AS GS", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '1 day') AS GS", 365, 1, None),
        ("SELECT * FROM generate_series('2020-01-01', '2020-12-31', '1day') AS GS", 366, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-12-31', '7 days') AS GS", 53, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-01-02', '1 hour') AS GS", 25, 1, None),
        ("SELECT * FROM generate_series('2022-01-01', '2022-01-01 23:59', '1 hour') AS GS", 24, 1, None),
        ("SELECT * FROM generate_series('2022-01-01 12:00', '2022-01-01 23:59', '1 hour') AS GS", 12, 1, None),
        ("SELECT * FROM generate_series('2022-01-01 12:00', '2022-01-01 12:15', '1 minute') AS GS", 16, 1, None),
        ("SELECT * FROM generate_series('2022-01-01 12:00', '2022-01-01 12:15', '1m30s') AS GS", 11, 1, None),
        ("SELECT * FROM generate_series(1,10) AS GS LEFT JOIN $planets ON id = GS", 10, 21, None),
        ("SELECT * FROM GENERATE_SERIES(5, 10) AS PID LEFT JOIN $planets ON id = PID", 6, 21, None),
        ("SELECT * FROM generate_series(1,5) AS GS JOIN $planets ON id = GS", 5, 21, None),
        ("SELECT * FROM (SELECT * FROM generate_series(1,10,2) AS gs) AS GS INNER JOIN $planets on gs = id", 5, 21, None),

        ("SELECT * FROM 'testdata/flat/formats/arrow/tweets.arrow'", 100000, 13, None),
        ("SELECT * FROM 'testdata/flat/../flat/formats/arrow/tweets.arrow'", None, None, DatasetNotFoundError),  # don't allow traversal

        ("SELECT * FROM testdata.partitioned.dated", None, None, EmptyDatasetError),  # it's there, but no partitions for today
        ("SELECT * FROM testdata.partitioned.dated FOR '2024-02-03' WITH (NO_CACHE)", 25, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR '2024-02-03'", 25, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR '2024-02-04'", 25, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR DATES BETWEEN '2024-02-01' AND '2024-02-28'", 50, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR '2024-02-03' OFFSET 1", 24, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR DATES BETWEEN '2024-02-01' AND '2024-02-28' OFFSET 1", 49, 8, None),
        ("SELECT * FROM $satellites FOR YESTERDAY ORDER BY planetId OFFSET 10", 167, 8, None),

        ("SELECT * FROM testdata.partitioned.dated FOR '2024-02-03 00:00' WITH (NO_CACHE)", 25, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR '2024-02-03 12:00'", 25, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR '2024-02-04 00:00'", 25, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR DATES BETWEEN '2024-02-01 00:00' AND '2024-02-28 00:00'", 50, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR '2024-02-03 00:00' OFFSET 1", 24, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR DATES BETWEEN '2024-02-01 00:00' AND '2024-02-28 00:00' OFFSET 1", 49, 8, None),

        ("SELECT * FROM testdata.partitioned.dated FOR DATES SINCE '2024-02-01'", 50, 8, None),
        ("SELECT * FROM testdata.partitioned.dated FOR DATES SINCE '2024-02-04 00:00'", 25, 8, None),

        ("SELECT * FROM testdata.partitioned.mixed FOR '2020-02-03'", None, None, UnsupportedSegementationError),
        ("SELECT * FROM $planets FOR '1730-01-01'", 6, 20, None),
        ("SELECT * FROM $planets FOR '1730-01-01 12:45'", 6, 20, None),
        ("SELECT * FROM $planets FOR '1830-01-01'", 7, 20, None),
        ("SELECT * FROM $planets FOR '1930-01-01'", 8, 20, None),
        ("SELECT * FROM $planets FOR '2030-01-01'", 9, 20, None),
        ("SELECT * FROM $planets FOR MONDAY", 9, 20, None),
        ("SELECT * FROM $planets FOR SUNDAY", 9, 20, None),
        ("SELECT * FROM $planets FOR THURSDAY", 9, 20, None),
        ("SELECT * FROM $planets FOR DATES SINCE MONDAY", 9, 20, None),
        ("SELECT * FROM $planets FOR DATES SINCE SUNDAY", 9, 20, None),
        ("SELECT * FROM $planets FOR DATES SINCE THURSDAY", 9, 20, None),
        ("SELECT * FROM $planets FOR DATES BETWEEN MONDAY AND TODAY", 9, 20, None),
        ("SELECT * FROM $planets FOR DATES BETWEEN SUNDAY AND TODAY", 9, 20, None),
        ("SELECT * FROM $planets FOR DATES BETWEEN THURSDAY AND TODAY", 9, 20, None),
        ("SELECT * FROM $planets FOR YESTERDAY", 9, 20, None),
        ("SELECT * FROM $planets FOR TODAY", 9, 20, None),
        ("SELECT * FROM $planets AS planets FOR '1730-01-01'", 6, 20, None),
        ("SELECT * FROM $planets AS p FOR '1830-01-01'", 7, 20, None),
        ("SELECT * FROM $planets AS pppp FOR '1930-01-01'", 8, 20, None),
        ("SELECT * FROM $planets AS P FOR '2030-01-01'", 9, 20, None),
        ("SELECT * FROM (SELECT * FROM $planets AS D) AS P FOR '2030-01-01'", 9, 20, None),
        ("SELECT * FROM $planets AS P FOR '1699-01-01' INNER JOIN $satellites FOR '2030-01-01' ON P.id = planetId;", 131, 28, None),
        ("SELECT today.name FROM $planets FOR TODAY AS today LEFT JOIN $planets FOR '1600-01-01' AS sixteen ON sixteen.id = today.id WHERE sixteen.id IS NULL;", 3, 1, None),

        ("SELECT id, name FROM (SELECT id, name FROM $satellites) as S", 177, 2, None),
        ("SELECT S.id, name FROM (SELECT id, name FROM $satellites) as S", 177, 2, None),
        ("SELECT S.id, S.name FROM (SELECT id, name FROM $satellites) as S", 177, 2, None),
        ("SELECT S.id, S.name FROM (SELECT id, name FROM $satellites AS P) as S", 177, 2, None),
        ("SELECT S.id, S.name FROM (SELECT id, P.name FROM $satellites AS P) as S", 177, 2, None),
        ("SELECT S.id, S.name FROM (SELECT P.id, P.name FROM $satellites AS P) as S", 177, 2, None),
        ("SELECT S.id, name FROM (SELECT id, name FROM $satellites AS P) as S", 177, 2, None),
        ("SELECT id, name FROM (SELECT id, P.name FROM $satellites AS P) as S", 177, 2, None),
        ("SELECT P.id, name FROM (SELECT P.id, P.name FROM $satellites AS P) as S", None, None, UnexpectedDatasetReferenceError),
        ("SELECT * FROM (SELECT id, name FROM $satellites AS P) as S", 177, 2, None),
        ("SELECT id, name FROM (SELECT * FROM $satellites AS P) as S", 177, 2, None),
        ("SELECT id, name FROM (SELECT * FROM $satellites) as S", 177, 2, None),
        ("SELECT * FROM (SELECT id, name FROM $satellites) as S", 177, 2, None),
        ("SELECT id, name FROM (SELECT id, name FROM (SELECT id, name FROM $satellites) AS T1) AS T2", 177, 2, None),
        ("SELECT id, name FROM (SELECT id, name FROM (SELECT id, name FROM $satellites) AS T1) AS T2", 177, 2, None),
        ("SELECT id, name FROM (SELECT id, name FROM (SELECT id, name FROM (SELECT id, name FROM $satellites) AS T0) AS T1) AS T2", 177, 2, None),
        ("SELECT X.id, X.name FROM (SELECT Y.id, Y.name FROM (SELECT Z.id, Z.name FROM $satellites AS Z) AS Y) AS X", 177, 2, None),
        ("SELECT id, name FROM (SELECT id, name FROM (SELECT id, name FROM $satellites WHERE id > 100) AS T1 WHERE id < 150) AS T2", 49, 2, None),
        ("SELECT id, name FROM (SELECT id, name FROM (SELECT id, name FROM $satellites ORDER BY id ASC) AS T1 ORDER BY id DESC) AS T2", 177, 2, None),
        ("SELECT id, name FROM (SELECT id, name FROM (SELECT id, name FROM $satellites GROUP BY id, name) AS T1) AS T2", 177, 2, None),
        ("SELECT MAX(id), MIN(name) FROM (SELECT id, name FROM (SELECT id, name FROM $satellites) AS T1) AS T2", 1, 2, None),

        ("SELECT * FROM $astronauts  WHERE name IS NULL", 0, 19, None),
        ("SELECT * FROM $astronauts  WHERE name IS NOT NULL", 357, 19, None),
        ("SELECT * FROM $astronauts  WHERE year IS NULL", 27, 19, None),
        ("SELECT * FROM $astronauts  WHERE year IS NOT NULL", 330, 19, None),
        ("SELECT * FROM $astronauts  WHERE group IS NULL", 27, 19, None),
        ("SELECT * FROM $astronauts  WHERE group IS NOT NULL", 330, 19, None),
        ("SELECT * FROM $astronauts  WHERE status IS NULL", 0, 19, None),
        ("SELECT * FROM $astronauts  WHERE status IS NOT NULL", 357, 19, None),
        ("SELECT * FROM $astronauts  WHERE birth_date IS NULL", 0, 19, None),
        ("SELECT * FROM $astronauts  WHERE birth_date IS NOT NULL", 357, 19, None),
        ("SELECT * FROM $astronauts  WHERE birth_place IS NULL", 0, 19, None),
        ("SELECT * FROM $astronauts  WHERE birth_place IS NOT NULL", 357, 19, None),
        ("SELECT * FROM $astronauts  WHERE gender IS NULL", 0, 19, None),
        ("SELECT * FROM $astronauts  WHERE gender IS NOT NULL", 357, 19, None),
        ("SELECT * FROM $astronauts  WHERE alma_mater IS NULL", 0, 19, None),
        ("SELECT * FROM $astronauts  WHERE alma_mater IS NOT NULL", 357, 19, None),
        ("SELECT * FROM $astronauts  WHERE undergraduate_major IS NULL", 22, 19, None),
        ("SELECT * FROM $astronauts  WHERE undergraduate_major IS NOT NULL", 335, 19, None),
        ("SELECT * FROM $astronauts  WHERE graduate_major IS NULL", 59, 19, None),
        ("SELECT * FROM $astronauts  WHERE graduate_major IS NOT NULL", 298, 19, None),
        ("SELECT * FROM $astronauts  WHERE military_rank IS NULL", 150, 19, None),
        ("SELECT * FROM $astronauts  WHERE military_rank IS NOT NULL", 207, 19, None),
        ("SELECT * FROM $astronauts  WHERE military_branch IS NULL", 146, 19, None),
        ("SELECT * FROM $astronauts  WHERE military_branch IS NOT NULL", 211, 19, None),
        ("SELECT * FROM $astronauts  WHERE space_flights IS NULL", 0, 19, None),
        ("SELECT * FROM $astronauts  WHERE space_flights IS NOT NULL", 357, 19, None),
        ("SELECT * FROM $astronauts  WHERE space_flight_hours IS NULL", 0, 19, None),
        ("SELECT * FROM $astronauts  WHERE space_flight_hours IS NOT NULL", 357, 19, None),
        ("SELECT * FROM $astronauts  WHERE space_walks IS NULL", 0, 19, None),
        ("SELECT * FROM $astronauts  WHERE space_walks IS NOT NULL", 357, 19, None),
        ("SELECT * FROM $astronauts  WHERE space_walks_hours IS NULL", 3, 19, None),
        ("SELECT * FROM $astronauts  WHERE space_walks_hours IS NOT NULL", 354, 19, None),
        ("SELECT * FROM $astronauts  WHERE missions IS NULL", 23, 19, None),
        ("SELECT * FROM $astronauts  WHERE missions IS NOT NULL", 334, 19, None),
        ("SELECT * FROM $astronauts  WHERE death_date IS NULL", 305, 19, None),
        ("SELECT * FROM $astronauts  WHERE death_date IS NOT NULL", 52, 19, None),
        ("SELECT * FROM $astronauts  WHERE death_mission IS NULL", 341, 19, None),
        ("SELECT * FROM $astronauts  WHERE death_mission IS NOT NULL", 16, 19, None),
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
        ("SELECT missions FROM $astronauts WHERE ARRAY_CONTAINS(missions, 'Apollo 8')", 3, 1, None),
        ("SELECT missions FROM $astronauts WHERE ARRAY_CONTAINS_ANY(missions, ('Apollo 8', 'Apollo 13'))", 5, 1, None),
        ("SELECT missions FROM $astronauts WHERE ARRAY_CONTAINS_ALL(missions, ('Apollo 8', 'Gemini 7'))", 2, 1, None),
        ("SELECT missions FROM $astronauts WHERE ARRAY_CONTAINS_ALL(missions, ('Gemini 7', 'Apollo 8'))", 2, 1, None),
        ("SELECT missions FROM $astronauts WHERE missions @> ('Apollo 8', 'Apollo 13')", 5, 1, None),

        ("SELECT * FROM $astronauts WHERE 'Apollo 11' = any(missions)", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE 'X' > any(alma_mater)", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE 'B' < any(alma_mater)", 15, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' != any(missions)", 331, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' != all(missions)", 331, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' = all(missions)", 0, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' = any(missions) AND True", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE 'X' > any(alma_mater) OR 'Z' > any(alma_mater)", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE name != 'Brian' AND 'B' < any(alma_mater)", 15, 19, None),
        ("SELECT * FROM $astronauts WHERE name != 'Brian' OR 'Apollo 11' != any(missions)", 357, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' != all(missions) AND name != 'Brian'", 331, 19, None),
        ("SELECT * FROM $astronauts WHERE name != 'Brian' AND 'Apollo 11' = all(missions)", 0, 19, None),
        ("SELECT * FROM $astronauts WHERE 'X' >= any(alma_mater)", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE 'B' <= any(alma_mater)", 15, 19, None),

        ("SELECT * FROM $satellites WHERE planetId IN (SELECT id FROM $planets WHERE name = 'Earth')", 1, 8, UnsupportedSyntaxError),  # temp
        ("SELECT * FROM $planets WHERE id NOT IN (SELECT DISTINCT planetId FROM $satellites)", 2, 20, UnsupportedSyntaxError),  # temp
        ("SELECT name FROM $planets WHERE id IN (SELECT * FROM UNNEST((1,2,3)) as id)", 3, 1, UnsupportedSyntaxError),  # temp
        ("SELECT count(planetId) FROM (SELECT DISTINCT planetId FROM $satellites) AS SQ", 1, 1, None),
        ("SELECT COUNT(*) FROM (SELECT planetId FROM $satellites WHERE planetId < 7) AS SQ GROUP BY planetId", 4, 1, None),

        ("EXPLAIN SELECT * FROM $satellites", 1, 3, None),
        ("EXPLAIN SELECT * FROM $satellites WHERE id = 8", 2, 3, None),
        ("EXPLAIN SELECT * FROM $planets WHERE TRUE", 1, 3, None),
        ("EXPLAIN SELECT * FROM $planets WHERE 1 > 1", 2, 3, None),
        ("SET version = '1.0';", None, None, PermissionsError),
        ("SET schmersion = '1.0';", None, None, VariableNotFoundError),
        ("SET disable_optimizer = 100;", None, None, ValueError),
        ("SET disable_optimizer = false; EXPLAIN SELECT * FROM $satellites WHERE id = 8", 2, 3, None),
        ("SET disable_optimizer = true; EXPLAIN SELECT * FROM $satellites WHERE id = 8 AND id = 7", 3, 3, None),
        ("SET disable_optimizer = false; EXPLAIN SELECT * FROM $satellites WHERE id = 8 AND id = 7", 3, 3, None),
        ("SET disable_optimizer = false; EXPLAIN SELECT * FROM $planets ORDER BY id LIMIT 5", 2, 3, None),
        ("SET disable_optimizer = true; EXPLAIN SELECT * FROM $planets ORDER BY id LIMIT 5", 2, 3, None),
        ("EXPLAIN SELECT * FROM $planets ORDER BY id LIMIT 5", 2, 3, None),
        ("SELECT name, id FROM $planets ORDER BY id LIMIT 5", 5, 2, None),
        ("SELECT name, id FROM $planets ORDER BY id LIMIT 100", 9, 2, None),

        ("SHOW COLUMNS FROM $satellites", 8, 4, None),
        ("SHOW FULL COLUMNS FROM $satellites", 8, 4, None),
        ("SHOW EXTENDED COLUMNS FROM $satellites", 8, 4, None),
        ("SHOW EXTENDED COLUMNS FROM $planets", 20, 4, None),
        ("SHOW EXTENDED COLUMNS FROM $astronauts", 19, 4, None),
        ("SHOW COLUMNS FROM $satellites LIKE '%d'", 2, 4, UnsupportedSyntaxError),
        ("SHOW COLUMNS FROM testdata.partitioned.dated FOR '2024-02-03'", 8, 4, None),

        ("SELECT * FROM $satellites CROSS JOIN $astronauts", 63189, 27, None),
        ("SELECT * FROM $satellites WITH (NO_CACHE) CROSS JOIN $astronauts WITH (NO_CACHE)", 63189, 27, None),
        ("SELECT * FROM $satellites, $planets", 1593, 28, None),
        ("SELECT * FROM $satellites INNER JOIN $planets USING (id)", 9, 27, None),
        ("SELECT * FROM $satellites WITH (NO_CACHE) INNER JOIN $planets USING (id)", 9, 27, None),
        ("SELECT * FROM $satellites WITH (NO_CACHE) INNER JOIN $planets WITH (NO_CACHE) USING (id)", 9, 27, None),
        ("SELECT * FROM $satellites INNER JOIN $planets WITH (NO_CACHE) USING (id)", 9, 27, None),
        ("SELECT * FROM $satellites JOIN $planets USING (id)", 9, 27, None),
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(missions) AS mission WHERE mission = 'Apollo 11'", 3, 20, None),
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(missions) AS m", 846, 20, None),
        ("SELECT * FROM $planets INNER JOIN $satellites ON $planets.id = $satellites.planetId", 177, 28, None),
        ("SELECT DISTINCT planetId FROM $satellites LEFT OUTER JOIN $planets ON $satellites.planetId = $planets.id", 7, 1, None),
        ("SELECT DISTINCT planetId FROM $satellites LEFT JOIN $planets ON $satellites.planetId = $planets.id", 7, 1, None),
        ("SELECT DISTINCT $planets.id, $satellites.id FROM $planets LEFT OUTER JOIN $satellites ON $satellites.planetId = $planets.id", 179, 2, None),
        ("SELECT DISTINCT $planets.id, $satellites.id FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id", 179, 2, None),
        ("SELECT planetId FROM $satellites LEFT JOIN $planets ON $satellites.planetId = $planets.id", 177, 1, None),
        ("SELECT * FROM $planets LEFT JOIN $planets USING(id)", 9, 40, AmbiguousDatasetError),
        ("SELECT * FROM $planets LEFT OUTER JOIN $planets USING(id)", 9, 40, AmbiguousDatasetError),
        ("SELECT * FROM $planets LEFT JOIN $planets FOR TODAY USING(id)", 9, 40, AmbiguousDatasetError),
        ("SELECT * FROM $planets LEFT JOIN $planets USING(id, name)", 9, 40, AmbiguousDatasetError),
        ("SELECT * FROM $planets INNER JOIN $planets ON id = id AND name = name", None, None, AmbiguousDatasetError),

        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id", 9, 20, None),
        ("SELECT P_2.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id", 9, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_2.id = P_1.id", 9, 20, None),
        ("SELECT P_2.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_2.id = P_1.id", 9, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.id", 9, 20, None),
        ("SELECT P_2.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.id", 9, 8, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_2.id = P_1.id", 9, 20, None),
        ("SELECT P_2.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_2.id = P_1.id", 9, 8, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id AND P_1.name = P_2.name", 9, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_2.id = P_1.id AND P_2.name = P_1.name", 9, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id AND P_2.name = P_1.name", 9, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_2.id = P_1.id AND P_1.name = P_2.name", 9, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.id AND P_1.name = P_2.name", 0, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_2.id = P_1.id AND P_2.name = P_1.name", 0, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.id AND P_2.name = P_1.name", 0, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_2.id = P_1.id AND P_1.name = P_2.name", 0, 20, None),
        ("SELECT P_1.id, P_2.name FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id", 9, 2, None),
        ("SELECT P_1.id, P_2.id FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.planet_id", 177, 2, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id WHERE P_1.id > 5", 4, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.planet_id WHERE P_2.id > 5", 172, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id ORDER BY P_1.id", 9, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.planet_id ORDER BY P_2.id", 177, 20, None),
        ("SELECT COUNT(*) FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id", 1, 1, None),
        ("SELECT COUNT(*) FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.planet_id", 1, 1, None),
        ("SELECT P_1.id, COUNT(P_2.id) FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.planet_id GROUP BY P_1.id", 7, 2, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.planet_id INNER JOIN $planets AS P_3 ON P_2.planet_id = P_3.id", 177, 20, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id AND P_1.id > 2", 7, 20, UnsupportedSyntaxError),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $satellites AS P_2 ON P_1.id = P_2.planet_id AND P_2.id != 5", 177, 20, UnsupportedSyntaxError),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id AND P_1.name = P_2.name AND P_1.mass = P_2.mass", 9, 20, None),

        ("SELECT * FROM $planets AS P_1 INNER JOIN $planets AS P_2 ON P_1.id = P_2.id AND P_2.name = P_1.name", 9, 40, None),
        ("SELECT * FROM $planets NATURAL JOIN generate_series(1, 5) as id", 5, 20, None),
        ("SELECT name, surfacePressure FROM $planets AS P1 NATURAL JOIN $planets AS P2", 5, 2, None),
        ("SELECT * FROM $satellites AS P1 NATURAL JOIN $satellites AS P2", 170, 8, None),
        ("SELECT * FROM $planets AS P NATURAL JOIN $satellites AS S", None, None, IncompatibleTypesError),
        ("SELECT id FROM $planets AS P_1 INNER JOIN $planets AS P_2 USING (id)", 9, 1, None),
        ("SELECT id, name FROM $planets AS P_1 INNER JOIN $planets AS P_2 USING (id, name)", 9, 2, None),
        ("SELECT P_1.* FROM $planets AS P_1 INNER JOIN $planets AS P_2 USING (id, name)", 9, 20, None),
        ("SELECT * FROM $satellites AS P_1 INNER JOIN $satellites AS P_2 USING (id, name)", 177, 14, None),
        ("SELECT $satellites.name, $planets.name from $planets LEFT JOIN $satellites USING (id) WHERE $planets.name != 'Earth'", 8, 2, None),

        ("SELECT * FROM $missions WHERE COSINE_SIMILARITY(Location, 'LC-18A, Cape Canaveral AFS, Florida, USA') > 0.7", 658, 8, None),

        ("SELECT DISTINCT planetId FROM $satellites RIGHT OUTER JOIN $planets ON $satellites.planetId = $planets.id", 8, 1, None),
        ("SELECT DISTINCT planetId FROM $satellites RIGHT JOIN $planets ON $satellites.planetId = $planets.id", 8, 1, None),
        ("SELECT planetId FROM $satellites RIGHT JOIN $planets ON $satellites.planetId = $planets.id", 179, 1, None),
        ("SELECT DISTINCT planetId FROM $satellites FULL OUTER JOIN $planets ON $satellites.planetId = $planets.id", 8, 1, None),
        ("SELECT DISTINCT planetId FROM $satellites FULL JOIN $planets ON $satellites.planetId = $planets.id", 8, 1, None),
        ("SELECT planetId FROM $satellites FULL JOIN $planets ON $satellites.planetId = $planets.id", 179, 1, None),

        ("SELECT pid FROM ( SELECT id AS pid FROM $planets) AS SQ WHERE pid > 5", 4, 1, None),
        ("SELECT * FROM ( SELECT id AS pid FROM $planets) AS SQ WHERE pid > 5", 4, 1, None),
        ("SELECT * FROM ( SELECT COUNT(planetId) AS moons, planetId FROM $satellites GROUP BY planetId ) AS SQ WHERE moons > 10", 4, 2, None),

        ("SELECT * FROM $planets WHERE id = -1", 0, 20, None),
        ("SELECT COUNT(*) FROM (SELECT DISTINCT a FROM $astronauts CROSS JOIN UNNEST(alma_mater) AS a ORDER BY a) AS SQ", 1, 1, None),

        ("SELECT a.id, b.id, c.id FROM $planets AS a INNER JOIN $planets AS b ON a.id = b.id INNER JOIN $planets AS c ON c.id = b.id", 9, 3, None),
        ("SELECT * FROM $planets AS a INNER JOIN $planets AS b ON a.id = b.id RIGHT OUTER JOIN $satellites AS c ON c.planetId = b.id", 177, 48, None),

        ("SELECT $planets.* FROM $satellites INNER JOIN $planets USING (id)", 9, 20, None),
        ("SELECT $satellites.* FROM $satellites INNER JOIN $planets USING (id)", 9, 8, None),
        ("SELECT $satellites.* FROM $satellites INNER JOIN $planets ON $planets.id = $satellites.id", 9, 8, None),
        ("SELECT p.* FROM $satellites INNER JOIN $planets AS p USING (id)", 9, 20, None),
        ("SELECT s.* FROM $satellites AS s INNER JOIN $planets USING (id)", 9, 8, None),
        ("SELECT s.* FROM $satellites AS s INNER JOIN $planets AS p USING (id)", 9, 8, None),
        ("SELECT s.* FROM $satellites AS s INNER JOIN $planets AS p USING (id)", 9, 8, None),
        ("SELECT s.* FROM $planets AS s INNER JOIN $planets AS p USING (id, name)", 9, 20, None),
        ("SELECT p.* FROM $planets AS s INNER JOIN $planets AS p USING (id, name)", 9, 20, None),
        ("SELECT id, name FROM $planets AS s INNER JOIN $planets AS p USING (id, name)", 9, 2, None),

        ("SELECT DATE_TRUNC('month', birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT DISTINCT * FROM (SELECT DATE_TRUNC('year', birth_date) AS BIRTH_YEAR FROM $astronauts) AS SQ", 54, 1, None),
        ("SELECT DISTINCT * FROM (SELECT DATE_TRUNC('month', birth_date) AS BIRTH_YEAR_MONTH FROM $astronauts) AS SQ", 247, 1, None),
        ("SELECT time_bucket(birth_date, 10, 'year') AS decade, count(*) from $astronauts GROUP BY time_bucket(birth_date, 10, 'year')", 6, 2, None),
        ("SELECT time_bucket(birth_date, 6, 'month') AS half, count(*) from $astronauts GROUP BY time_bucket(birth_date, 6, 'month')", 97, 2, None),
    
        ("SELECT graduate_major, undergraduate_major FROM $astronauts WHERE COALESCE(graduate_major, undergraduate_major, 'high school') = 'high school'", 4, 2, None),
        ("SELECT graduate_major, undergraduate_major FROM $astronauts WHERE COALESCE(graduate_major, undergraduate_major) = 'Aeronautical Engineering'", 41, 2, None),
        ("SELECT COALESCE(death_date, '2030-01-01') FROM $astronauts", 357, 1, None),
        ("SELECT * FROM $astronauts WHERE COALESCE(death_date, '2030-01-01') < '2000-01-01'", 30, 19, None),

        ("SELECT TOUNT(*) FROM $planets", None, None, FunctionNotFoundError),

        ("SELECT SEARCH(name, 'al'), name FROM $satellites", 177, 2, None),
        ("SELECT name FROM $satellites WHERE SEARCH(name, 'al')", 18, 1, None),
        ("SELECT SEARCH(missions, 'Apollo 11'), missions FROM $astronauts", 357, 2, None),
        ("SELECT name FROM $astronauts WHERE SEARCH(missions, 'Apollo 11')", 3, 1, None),
        ("SELECT name, SEARCH(birth_place, 'Italy') FROM $astronauts", 357, 2, None),
        ("SELECT name, birth_place FROM $astronauts WHERE SEARCH(birth_place, 'Italy')", 1, 2, None),
        ("SELECT name, birth_place FROM $astronauts WHERE SEARCH(birth_place, 'Rome')", 1, 2, None),
        ("SELECT SEARCH($satellites.name, 'a') FROM $planets LEFT JOIN $satellites ON $planets.id = $satellites.planetId", 179, 1, None),

        ("SELECT birth_date FROM $astronauts WHERE EXTRACT(year FROM birth_date) < 1930;", 14, 1, None),
        ("SELECT EXTRACT(month FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(day FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT birth_date FROM $astronauts WHERE EXTRACT(year FROM birth_date) < 1930;", 14, 1, None),
        ("SELECT EXTRACT(doy FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(DOY FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(dow FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(DAYOFYEAR FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(DAYOFWEEK FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(DOW FROM birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(YEAR FROM '2022-02-02')", 1, 1, None),
        ("SELECT EXTRACT(ISOYEAR FROM '2022-02-02')", 1, 1, None),
        ("SELECT EXTRACT(WEEK FROM '2022-02-02')", 1, 1, None),
        ("SELECT EXTRACT(ISOWEEK FROM '2022-02-02')", 1, 1, None),
        ("SELECT EXTRACT(millisecond FROM NOW())", 1, 1, None),
        ("SELECT EXTRACT(milliseconds FROM NOW())", 1, 1, None),
        ("SELECT EXTRACT(nanosecond FROM NOW())", 1, 1, None),
        ("SELECT EXTRACT(millisecond FROM NOW())", 1, 1, None),
        ("SELECT EXTRACT(nanoseconds FROM NOW())", 1, 1, None),
        ("SELECT EXTRACT(millisecond FROM NOW())", 1, 1, None),
        ("SELECT EXTRACT(TIME FROM NOW())", 1, 1, InvalidFunctionParameterError),
        ("SELECT EXTRACT(DECADE FROM NOW())", 1, 1, None),
        ("SELECT EXTRACT(CENTURY FROM NOW())", 1, 1, None),
        ("SELECT EXTRACT(EPOCH FROM NOW())", 1, 1, None),
        ("SELECT EXTRACT(JULIAN FROM NOW())", 1, 1, None),
        ("SELECT EXTRACT(VANILLA FROM NOW())", 1, 1, SqlError), # InvalidFunctionParameterError),
        ("SELECT EXTRACT(millenium FROM NOW())", None, None, InvalidFunctionParameterError),

        ("SELECT DATE_FORMAT(birth_date, '%m-%y') FROM $astronauts", 357, 1, None),
        ("SELECT DATEDIFF('year', '2017-08-25', '2011-08-25') AS DateDiff;", 1, 1, None),
        ("SELECT DATEDIFF('days', '2022-07-07', birth_date) FROM $astronauts", 357, 1, None),
        ("SELECT DATEDIFF('minutes', birth_date, '2022-07-07') FROM $astronauts", 357, 1, None),
        ("SELECT EXTRACT(DOW FROM birth_date) AS DOW, COUNT(*) FROM $astronauts GROUP BY EXTRACT(DOW FROM birth_date) ORDER BY COUNT(*) DESC", 7, 2, None),

        ("SELECT * FROM $planets WITH(NO_PARTITION)", 9, 20, None),
        ("SELECT * FROM $planets WITH(NO_PUSH_PROJECTION)", 9, 20, None),
        ("SELECT * FROM $planets WITH(NO_PARTITION, NO_PUSH_PROJECTION)", 9, 20, None),

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
        ("SELECT * FROM $satellites WHERE 1 * planetId = round(density * 1)", 1, 8, None),
        ("SELECT * FROM $satellites WHERE 0 + planetId = round(density * 1)", 1, 8, None),
        ("SELECT * FROM $satellites WHERE planetId + 0 = round(density * 1)", 1, 8, None),
        ("SELECT * FROM $satellites WHERE planetId - 0 = round(density * 1)", 1, 8, None),
        ("SELECT ABSOLUTE(ROUND(gravity) * density * density) FROM $planets", 9, 1, None),
        ("SELECT COUNT(*), ROUND(gm) FROM $satellites GROUP BY ROUND(gm)", 22, 2, None),
        ("SELECT COALESCE(death_date, '1900-01-01') FROM $astronauts", 357, 1, None),
        ("SELECT * FROM (SELECT COUNT(*) FROM testdata.flat.formats.parquet WITH(NO_PARTITION) GROUP BY followers) AS SQ", 10016, 1, None),
        ("SELECT a.id, b.id FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id)", 9, 2, AmbiguousIdentifierError),
        ("SELECT * FROM $planets INNER JOIN $planets AS b USING (id)", 9, 39, None),
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
        ("SELECT IFNOTNULL(death_date, '1970-01-01') FROM $astronauts", 357, 1, None),
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
        ("SELECT IIF(year > 1960, 1, 0), year FROM $astronauts", 357, 2, None),
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
        ("SELECT birth_date, death_date FROM $astronauts WHERE death_date - birth_date > INTERVAL '1' DAY", 51, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE birth_date + INTERVAL '64' YEAR > death_date", 41, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE birth_date + INTERVAL '64' YEAR < death_date", 11, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE death_date < birth_date + INTERVAL '64' YEAR", 41, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE death_date > birth_date + INTERVAL '64' YEAR", 11, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE death_date - INTERVAL '64' YEAR < birth_date", 41, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE death_date - INTERVAL '64' YEAR > birth_date", 11, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE birth_date < death_date - INTERVAL '64' YEAR", 11, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE birth_date > death_date - INTERVAL '64' YEAR", 41, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE NOT (birth_date + INTERVAL '64' YEAR > death_date)", 11, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE NOT (birth_date + INTERVAL '64' YEAR < death_date)", 41, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE birth_date + INTERVAL '50' YEAR = death_date", 0, 2, None),
        ("SELECT birth_date, death_date FROM $astronauts WHERE death_date - birth_date > INTERVAL '50' YEAR", 26, 2, None),
        ("SELECT * FROM $astronauts WHERE DATE '1932-02-07' - birth_date < INTERVAL '1' DAY", 324, 19, None),
        ("SELECT * FROM $astronauts WHERE DATE '1932-02-07' - birth_date = INTERVAL '1' DAY", 0, 19, None),
        ("SELECT * FROM $astronauts WHERE DATE '1932-02-07' - birth_date > INTERVAL '1' DAY", 33, 19, None),
        ("SELECT * FROM $astronauts WHERE DATE '1930-08-06' - birth_date = INTERVAL '1' DAY", 1, 19, None),

        ("SELECT * FROM $astronauts WHERE 'Apollo 11' IN UNNEST(missions)", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' NOT IN UNNEST(missions)", 331, 19, None),
        ("SELECT * FROM $astronauts WHERE NOT 'Apollo 11' IN UNNEST(missions)", 331, 19, None),
        ("SELECT * FROM $astronauts WHERE NOT 'Apollo 11' IN UNNEST(missions) and name ILIKE '%arm%'", 0, 19, None),
        ("SELECT * FROM $astronauts WHERE NOT 'Apollo 12' IN UNNEST(missions) and name ILIKE '%arm%'", 1, 19, None),
        ("SET @variable = 'Apollo 11'; SELECT * FROM $astronauts WHERE @variable IN UNNEST(missions)", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE NOT 'Apollo 12' = ANY(missions) or missions is null", 354, 19, None),
        ("SELECT * FROM $astronauts WHERE NOT 'Apollo 12' = ANY(missions) and missions is null", 0, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' != ANY(missions)", 331, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' != ANY(missions) and missions is null", 0, 19, None),
        ("SET @id = 3; SELECT name FROM $planets WHERE id = @id;", 1, 1, None),
        ("SET @id = 3; SELECT name FROM $planets WHERE id < @id;", 2, 1, None),
        ("SET @id = 3; SELECT name FROM $planets WHERE id < @id OR id > @id;", 8, 1, None),
        ("SET @dob = '1950-01-01'; SELECT name FROM $astronauts WHERE birth_date < @dob;", 149, 1, None),
        ("SET @dob = '1950-01-01'; SET @mission = 'Apollo 11'; SELECT name FROM $astronauts WHERE birth_date < @dob AND @mission IN UNNEST(missions);", 3, 1, None),
        ("SET @pples = 'b'; SET @ngles = 90; SHOW VARIABLES LIKE '@%s'", 2, 4, UnsupportedSyntaxError),
        ("SET @pples = 'b'; SET @rgon = 90; SHOW VARIABLES LIKE '@%gon'", 1, 4, UnsupportedSyntaxError),
        ("SET @variable = 44; SET @var = 'name'; SHOW VARIABLES LIKE '@%ri%';", 1, 4, UnsupportedSyntaxError),
        ("SHOW PARAMETER disable_optimizer", 1, 2, UnsupportedSyntaxError),
        ("SET disable_optimizer = true; SHOW PARAMETER disable_optimizer;", 1, 2, UnsupportedSyntaxError),
        ("SET disable_optimizer TO true; SHOW PARAMETER disable_optimizer;", 1, 2, UnsupportedSyntaxError),

        ("SELECT id FROM $planets WHERE NOT NOT id > 3", 6, 1, None),
        ("SELECT id FROM $planets WHERE NOT NOT id < 3", 2, 1, None),
        ("SELECT id FROM $planets WHERE NOT id > 3", 3, 1, None),
        ("SELECT id FROM $planets WHERE NOT id < 3", 7, 1, None),
        ("SELECT id FROM $planets WHERE NOT (id < 5 AND id = 3)", 8, 1, None),
        ("SELECT id FROM $planets WHERE NOT NOT (id < 5 AND id = 3)", 1, 1, None),
        ("SELECT id FROM $planets WHERE NOT id = 2 AND NOT NOT (id < 5 AND id = 3)", 1, 1, None),
        ("SET disable_optimizer = true; SELECT id FROM $planets WHERE NOT id = 2 AND NOT NOT (id < 5 AND id = 3)", 1, 1, None),
        ("SELECT * FROM $planets WHERE NOT(id = 9 OR id = 8)", 7, 20, None),
        ("SELECT * FROM $planets WHERE NOT(id = 9 OR id = 8) OR True", 9, 20, None),
        ("SELECT * FROM $planets WHERE NOT(id = 9 OR 8 = 8)", 0, 20, None),
        ("SELECT * FROM $planets WHERE 1 = 1", 9, 20, None),
        ("SELECT * FROM $planets WHERE NOT 1 = 2", 9, 20, None),

        ("SHOW CREATE TABLE $planets", 1, 1, UnsupportedSyntaxError),
        ("SHOW CREATE TABLE $satellites", 1, 1, UnsupportedSyntaxError),
        ("SHOW CREATE TABLE $astronauts", 1, 1, UnsupportedSyntaxError),
        ("SHOW CREATE TABLE testdata.partitioned.framed FOR '2021-03-28'", 1, 1, UnsupportedSyntaxError),
        ("SET disable_optimizer = true;\nSELECT COUNT(*) FROM $planets WHERE id > 3 AND name ILIKE '%e%'", 1, 1, None),
        ("SET disable_optimizer = false;\nSELECT COUNT(*) FROM $planets WHERE id > 3 AND name ILIKE '%e%'", 1, 1, None),
        ("SET disable_optimizer = false;\nSELECT COUNT(*) FROM $planets WHERE id > 3 AND name ILIKE '%e%'", 1, 1, None),
        ("SET disable_optimizer = true;\nSELECT COUNT(*) FROM $planets WHERE id > 3 AND name ILIKE '%e%'", 1, 1, None),
        ("SELECT COUNT(*) FROM $planets WHERE id > 3 AND name LIKE '%e%' AND id > 1 AND id > 0 AND id > 2 AND name ILIKE '%e%'", 1, 1, None),

        ("SELECT planets.* FROM $planets AS planets LEFT JOIN $planets FOR '1600-01-01' AS older ON planets.id = older.id WHERE older.name IS NULL", 3, 20, None),
        ("SELECT * FROM generate_series(1,10) AS GS LEFT JOIN $planets FOR '1600-01-01' ON id = GS", 10, 21, None),
        ("SELECT DISTINCT name FROM generate_series(1,10) AS GS LEFT JOIN $planets FOR '1600-01-01' ON id = GS", 7, 1, None),
        ("SELECT 1 WHERE ' a  b ' \t = \n\n ' ' || 'a' || ' ' || \n ' b '", 1, 1, UnsupportedSyntaxError),
        ("SELECT 1 FROM $planets WHERE ' a  b ' \t = \n\n ' ' || 'a' || ' ' || \n ' b '", 9, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name, 1, 1 ) = 'M'", 2, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name, 2, 1 ) = 'a'", 3, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name, 3 ) = 'rth'", 1, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name, -1 ) = 's'", 3, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name FROM 1 FOR 1 ) = 'M'", 2, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name FROM 2 FOR 1 ) = 'a'", 3, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name FROM 3 ) = 'rth'", 1, 1, None),
        ("SELECT name FROM $planets WHERE SUBSTRING ( name FROM  -1 ) = 's'", 3, 1, None),
        ("SELECT SUBSTRING ( name FROM 5 FOR 2 ) FROM $planets", 9, 1, None),
        ("SELECT name FROM $planets FOR TODAY WHERE SUBSTRING ( name, 1, 1 ) = 'M'", 2, 1, None),
        ("SELECT name FROM $planets FOR TODAY WHERE SUBSTRING ( name, 2, 1 ) = 'a'", 3, 1, None),
        ("SELECT name FROM $planets FOR TODAY WHERE SUBSTRING ( name, 3 ) = 'rth'", 1, 1, None),
        ("SELECT name FROM $planets FOR TODAY WHERE SUBSTRING ( name, -1 ) = 's'", 3, 1, None),
        ("SELECT name FROM $planets FOR TODAY WHERE SUBSTRING ( name FROM 1 FOR 1 ) = 'M'", 2, 1, None),
        ("SELECT name FROM $planets FOR TODAY WHERE SUBSTRING ( name FROM 2 FOR 1 ) = 'a'", 3, 1, None),
        ("SELECT name FROM $planets FOR TODAY WHERE SUBSTRING ( name FROM 3 ) = 'rth'", 1, 1, None),
        ("SELECT name FROM $planets FOR TODAY WHERE SUBSTRING ( name FROM  -1 ) = 's'", 3, 1, None),
        ("SELECT SUBSTRING ( name FROM 5 FOR 2 ) FROM $planets FOR TODAY ", 9, 1, None),
        ("SELECT TIMESTAMP '2022-01-02', DATEDIFF('days', TIMESTAMP '2022-01-02', TIMESTAMP '2022-10-01') FROM $astronauts;", 357, 2, None),
        ("SELECT * FROM $satellites WHERE NULLIF(planetId, 5) IS NULL", 67, 8, None),
        ("SELECT * FROM $satellites WHERE NULLIF(planetId, 5) IS NOT NULL", 110, 8, None),

        ("SHOW STORES LIKE 'apple'", None, None, UnsupportedSyntaxError),
        ("SELECT name FROM $astronauts WHERE LEFT(name, POSITION(' ' IN name) - 1) = 'Andrew'", 3, 1, None),
        ("SELECT name FROM $astronauts WHERE LEFT(name, POSITION(' ' IN name)) = 'Andrew '", 3, 1, None),

        ("SELECT ARRAY_AGG(name) from $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT ARRAY_AGG(DISTINCT name) from $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT ARRAY_AGG(name ORDER BY name) from $satellites GROUP BY TRUE", 1, 1, UnsupportedSyntaxError),
        ("SELECT ARRAY_AGG(name LIMIT 1) from $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT ARRAY_AGG(DISTINCT name LIMIT 1) from $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT COUNT(*), ARRAY_AGG(name) from $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT planetId, COUNT(*), ARRAY_AGG(name) from $satellites GROUP BY planetId", 7, 3, None),
        ("SELECT ARRAY_AGG(DISTINCT LEFT(name, 1)) from $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT ARRAY_AGG(name ORDER BY name LIMIT 2) FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT ARRAY_AGG(name ORDER BY name DESC LIMIT 2) FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT ARRAY_AGG(name ORDER BY id) FROM $satellites GROUP BY planetId", None, None, UnsupportedSyntaxError),
        ("SELECT ARRAY_AGG(name ORDER BY name, name) FROM $satellites GROUP BY planetId", None, None, UnsupportedSyntaxError),

        ("SELECT name FROM $satellites WHERE '192.168.0.1' | '192.168.0.0/24'", 177, 1, None),
        ("SELECT name FROM $satellites WHERE '192.168.0.1' | '192.167.0.0/24'", 0, 1, None),
        ("SELECT name FROM $satellites WHERE 12 | 22", 177, 1, None),
        ("SELECT '1' | '1'", None, None, IncorrectTypeError),
        ("SELECT 'abc' | '192.168.1.1'", None, None, IncorrectTypeError),
        ("SELECT 123 | '192.168.1.1'", None, None, IncorrectTypeError),
        ("SELECT '192.168.1.1' | 'abc'", None, None, IncorrectTypeError),
        ("SELECT '192.168.1.1' | 123", None, None, IncorrectTypeError),
        ("SELECT '192.168.1.1' | 0", None, None, IncorrectTypeError),
        ("SELECT 0 | '192.168.1.1'", None, None, IncorrectTypeError),
        ("SELECT '0' | '192.168.1.1'", None, None, IncorrectTypeError),
        ("SELECT '192.168.1.1' | '0'", None, None, IncorrectTypeError),
        ("SELECT name FROM $satellites WHERE '1' | '1'", None, None, IncorrectTypeError),
        ("SELECT name FROM $satellites WHERE 'abc' | '192.168.1.1'", None, None, IncorrectTypeError),
        ("SELECT name FROM $satellites WHERE null | '192.168.1.1/8'", 0, 1, None),
        ("SELECT name FROM $satellites WHERE 123 | '192.168.1.1'", None, None, IncorrectTypeError),
        ("SELECT name FROM $satellites WHERE '10.10.10.10' | '192.168.1.1'", 0, 1, IncorrectTypeError),
        ("SELECT name FROM $satellites WHERE 0 | 0", 0, 1, None),
        ("SELECT name FROM $satellites WHERE 0 | 123456", 177, 1, None),
        ("SELECT name FROM $satellites WHERE 123456 | 0", 177, 1, None),
        ("SELECT name FROM $satellites WHERE 987654321 | 123456789", 177, 1, None),
        ("SELECT '192.168.1.1' | '255.255..255'", None, None, IncorrectTypeError),
        ("SELECT '192.168.1.1/32' | '192.168.1.1/8'", None, None, IncorrectTypeError),
        ("SELECT '192.168.1.*' | '192.168.1.1/8'", None, None, IncorrectTypeError),
        ("SELECT '!!' | '192.168.1.1/8'", None, None, IncorrectTypeError),
        ("SELECT null | '192.168.1.1'", 1, 1, IncorrectTypeError),
        ("SELECT null | '192.168.1.1/8'", 1, 1, None),

        ("SELECT * FROM testdata.flat.different", 196902, 15, None),
        ("SELECT * FROM testdata.flat.different WHERE following < 10", 7814, 15, None),
        ("SELECT is_quoting, COUNT(*) FROM testdata.flat.different GROUP BY is_quoting", 13995, 2, None),
        ("SELECT is_quoting FROM testdata.flat.different", 196902, 1, None),
        ("SELECT * FROM testdata.flat.different WHERE following IS NULL", 9, 15, None),
        ("SELECT name FROM testdata.flat.different", None, None, ColumnNotFoundError),

        ("SELECT COUNT(*), place FROM (SELECT CASE id WHEN 3 THEN 'Earth' WHEN 1 THEN 'Mercury' ELSE 'Elsewhere' END as place FROM $planets) AS SQ GROUP BY place;", 3, 2, None),
        ("SELECT COUNT(*), place FROM (SELECT CASE id WHEN 3 THEN 'Earth' WHEN 1 THEN 'Mercury' END as place FROM $planets) AS SQ GROUP BY place HAVING place IS NULL;", 1, 2, None),
        ("SELECT COUNT(*), place FROM (SELECT CASE id WHEN 3 THEN 'Earth' WHEN 1 THEN 'Mercury' ELSE 'Elsewhere' END as place FROM $planets) AS SQ GROUP BY place HAVING place IS NULL;", 0, 2, None),

        ("SELECT TRIM(LEADING 'E' FROM name) FROM $planets;", 9, 1, None),
        ("SELECT * FROM $planets WHERE TRIM(TRAILING 'arth' FROM name) = 'E'", 1, 20, None),
        ("SELECT * FROM $planets WHERE TRIM(TRAILING 'ahrt' FROM name) = 'E'", 1, 20, None),
        ("SELECT TRIM ( 'MVEJSONP' FROM name ) FROM $planets", 9, 1, None),
        ("SELECT TRIM ( 'MVEJSONP' FROM name ) FROM $planets FOR TODAY", 9, 1, None),

        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified IS TRUE", 711, 2, None),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified = TRUE", 711, 2, None),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified IS TRUE AND followers < 1000", 10, 2, None),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE followers < 1000 and user_name LIKE '%news%'", 12, 2, None),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE followers < 1000 and followers < 500 and followers < 250", 40739, 2, None),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE followers BETWEEN 0 AND 251", 40939, 2, None),

        ("SELECT * FROM 'testdata/flat/formats/arrow/tweets.arrow'", 100000, 13, None),
        ("SELECT * FROM 'testdata/flat/tweets/tweets-0000.jsonl' INNER JOIN 'testdata/flat/tweets/tweets-0001.jsonl' USING (userid)", 491, 15, None),
        ("SELECT * FROM 'testdata/flat/tweets/tweets-0000.jsonl' INNER JOIN $planets on sentiment = numberOfMoons", 12, 28, IncompatibleTypesError),

        ("SELECT * FROM $planets AS p JOIN $planets AS g ON p.id = g.id AND g.name = 'Earth';", 1, 40, None),
        ("SELECT * FROM $planets AS p JOIN $planets AS g ON p.id = g.id AND p.name = 'Earth';", 1, 40, None),
        ("SELECT * FROM $planets AS p JOIN $planets AS g ON g.name = 'Earth' AND p.id = g.id;", 1, 40, None),
        ("SELECT * FROM $planets AS p JOIN $planets AS g ON p.name = 'Earth' AND p.id = g.id;", 1, 40, None),
        ("SELECT * FROM $planets AS p JOIN $planets AS g ON p.id = g.id AND 'Earth' = g.name;", 1, 40, None),
        ("SELECT * FROM $planets AS p JOIN $planets AS g ON p.id = g.id AND 'Earth' = p.name;", 1, 40, None),
        ("SELECT * FROM $planets AS p JOIN $planets AS g ON 'Earth' = g.name AND p.id = g.id;", 1, 40, None),
        ("SELECT * FROM $planets AS p JOIN $planets AS g ON 'Earth' = p.name AND p.id = g.id;", 1, 40, None),

        ("SELECT SPLIT(name, ' ', 0) FROM $astronauts", None, None, InvalidFunctionParameterError),
        ("SELECT SPLIT(name, ' ', 1) FROM $astronauts", 357, 1, None),

        ("SELECT * FROM FAKE(100, (Name, Name)) AS FK(nom, nim, nam)", 100, 2, None),
        ("SELECT * FROM FAKE(100, (Name, Name)) AS FK(nom)", 100, 2, None),
        ("SELECT * FROM FAKE(100, (Name, Name)) AS FK", 100, 2, None),
        ("SELECT * FROM FAKE(100, 10) AS FK(nom, nim, nam)", 100, 10, None),
        ("SELECT * FROM FAKE(10, (Age)) AS FK", None, None, InvalidFunctionParameterError),

        ("SELECT * FROM $planets WHERE diameter > 10000 AND gravity BETWEEN 0.5 AND 2.0;", 0, 20, None),
        ("SELECT * FROM $planets WHERE diameter > 100 AND gravity BETWEEN 0.5 AND 2.0;", 1, 20, None),

        ("SELECT * FROM $planets INNER JOIN $satellites ON $planets.name <> $satellites.name", 0, 28, UnsupportedSyntaxError),
        ("SELECT * FROM $planets CROSS JOIN $satellites WHERE $planets.name != $satellites.name", 1593, 28, None),
        ("SELECT * FROM $planets INNER JOIN $satellites WHERE $planets.name != $satellites.name", None, None, SqlError),
        ("SELECT * FROM $planets INNER JOIN $satellites ON $planets.name != $satellites.name", 0, 28, UnsupportedSyntaxError),
        ("SELECT a.name, b.name FROM sqlite.planets a JOIN sqlite.planets b ON a.numberOfMoons = b.numberOfMoons WHERE a.name <> b.name", 2, 2, None),
        ("SELECT * FROM $planets INNER JOIN $satellites ON INTEGER($planets.id) = INTEGER($satellites.planetId)", None, None, UnsupportedSyntaxError),
        ("SELECT alma_mater LIKE '%a%' FROM $astronauts", None, None, IncompatibleTypesError),
        ("SELECT * FROM $planets INNER JOIN $satellites ON gm = 4", None, None, IncompatibleTypesError),
        ("SELECT * FROM $planets INNER JOIN $satellites ON $planets.id = 4", None, None, UnsupportedSyntaxError),
        ("SELECT * FROM $planets CROSS JOIN UNNEST(name) AS G", None, None, IncorrectTypeError),

        ("SELECT VARCHAR(birth_place) FROM $astronauts", 357, 1, None),
        ("SELECT name FROM $astronauts WHERE GET(STRUCT(VARCHAR(birth_place)), 'state') = birth_place['state']", 357, 1, None),

#        ("SELECT * FROM $missions WHERE MATCH (Location) AGAINST ('Florida USA')", 911, 8, None),

        ("SELECT * FROM testdata.partitioned.hourly FOR '2024-01-01 01:00'", 1, 2, None),
        ("SELECT * FROM testdata.partitioned.hourly FOR '2024-01-01'", 2, 2, None),
        ("SELECT * EXCEPT (id, name, gravity) FROM $planets", 9, 17, None),


        # 10-way join
        ("SELECT p1.name AS planet1_name, p2.name AS planet2_name, p3.name AS planet3_name, p4.name AS planet4_name, p5.name AS planet5_name, p6.name AS planet6_name, p7.name AS planet7_name, p8.name AS planet8_name, p9.name AS planet9_name, p10.name AS planet10_name, p1.diameter AS planet1_diameter, p2.gravity AS planet2_gravity, p3.orbitalPeriod AS planet3_orbitalPeriod, p4.numberOfMoons AS planet4_numberOfMoons, p5.meanTemperature AS planet5_meanTemperature FROM $planets p1 JOIN $planets p2 ON p1.id = p2.id JOIN $planets p3 ON p1.id = p3.id JOIN $planets p4 ON p1.id = p4.id JOIN $planets p5 ON p1.id = p5.id JOIN $planets p6 ON p1.id = p6.id JOIN $planets p7 ON p1.id = p7.id JOIN $planets p8 ON p1.id = p8.id JOIN $planets p9 ON p1.id = p9.id JOIN $planets p10 ON p1.id = p10.id WHERE p1.diameter > 10000 ORDER BY p1.name, p2.name, p3.name, p4.name, p5.name;", 6, 15, None),

        ("SELECT mission, LIST(name) FROM $missions INNER JOIN (SELECT * FROM $astronauts CROSS JOIN UNNEST(missions) AS mission) AS astronauts ON Mission = mission GROUP BY mission", 16, 2, None),
        ("SELECT alma_matered FROM (SELECT alma_mater FROM $astronauts CROSS JOIN $satellites) AS bulked CROSS JOIN UNNEST(alma_mater) AS alma_matered", 120537, 1, None),

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
        ("SELECT * FROM $planets FOR 2022-01-01", 9, 20, None),
        ("SELECT * FROM $planets FOR DATES IN 2022", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES BETWEEN 2022-01-01 AND TODAY", 9, 20, None),
        ("SELECT * FROM $planets FOR DATES BETWEEN today AND yesterday", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES IN '2022-01-01' AND '2022-01-02'", None, None, InvalidTemporalRangeFilterError),
        # Join hints aren't supported
        ("SELECT * FROM $satellites INNER HASH JOIN $planets USING (id)", None, None, SqlError),
        # MONTH has a bug
        ("SELECT DATEDIFF('months', birth_date, '2022-07-07') FROM $astronauts", None, None, KeyError),
        ("SELECT DATEDIFF('months', birth_date, '2022-07-07') FROM $astronauts", None, None, KeyError),
        ("SELECT DATEDIFF(MONTH, birth_date, '2022-07-07') FROM $astronauts", None, None, ColumnNotFoundError),
        ("SELECT DATEDIFF(MONTHS, birth_date, '2022-07-07') FROM $astronauts", None, None, ColumnNotFoundError),
        # TEMPORAL QUERIES aren't part of the AST
        ("SELECT * FROM CUSTOMERS FOR SYSTEM_TIME ('2022-01-01', '2022-12-31')", None, None, InvalidTemporalRangeFilterError),
        # can't cast to a list
        ("SELECT CAST('abc' AS LIST)", None, None, SqlError),
        ("SELECT TRY_CAST('abc' AS LIST)", None, None, SqlError),

        ("SELECT STRUCT(dict) FROM testdata.flat.struct", 6, 1, None),

        # Test the order of the predicates shouldn't matter
        ("SELECT * FROM sqlite.planets WHERE id > gravity", 2, 20, None),
        ("SELECT * FROM sqlite.planets WHERE 1 > gravity", 1, 20, None),
        ("SELECT * FROM sqlite.planets WHERE id > 1", 8, 20, None),

        ("SELECT DISTINCT ON (id) FROM $planets;", None, None, SqlError),
        ("SELECT (name, id) FROM $planets;", None, None, UnsupportedSyntaxError),

        # V2 Negative Tests
        ("SELECT $planets.id, name FROM $planets INNER JOIN $satellites ON planetId = $planets.id", None, None, AmbiguousIdentifierError),
        ("SELECT $planets.id FROM $satellites", None, None, UnexpectedDatasetReferenceError),

        # V2 New Syntax Checks
#        ("SELECT * FROM $planets AS P1 UNION SELECT * FROM $planets AS P2;", 9, 20, UnsupportedSyntaxError),
        ("SELECT * FROM $planets AS P LEFT ANTI JOIN $satellites AS S ON S.id = P.id;", 0, 20, None),
        ("SELECT * FROM $planets AS P RIGHT ANTI JOIN $satellites AS S ON S.id = P.id;", 168, 8, UnsupportedSyntaxError),
        ("SELECT * FROM $planets AS P LEFT SEMI JOIN $satellites AS S ON S.id = P.id;", 9, 20, None),
        ("SELECT * FROM $planets AS P RIGHT SEMI JOIN $satellites AS S ON S.id = P.id;", 9, 8, UnsupportedSyntaxError),
        ("SELECT * FROM $planets AS P LEFT ANTI JOIN $satellites AS S USING(id);", 0, 20, None),
        ("SELECT * FROM $planets AS P RIGHT ANTI JOIN $satellites AS S USING(id);", 168, 8, UnsupportedSyntaxError),
        ("SELECT * FROM $planets AS P LEFT SEMI JOIN $satellites AS S USING(id);", 9, 20, None),
        ("SELECT * FROM $planets AS P RIGHT SEMI JOIN $satellites AS S USING(id);", 9, 8, UnsupportedSyntaxError),
        ("SELECT * FROM $planets AS P LEFT ANTI JOIN $satellites AS S ON S.id = P.id WHERE P.id > 5;", 0, 20, None),
        ("SELECT * FROM $planets AS P LEFT ANTI JOIN (SELECT id FROM $satellites WHERE name LIKE 'Moon%') AS S ON S.id = P.id;", 8, 20, None),
        ("SELECT * FROM GENERATE_SERIES(1, 10) AS C LEFT ANTI JOIN $satellites AS S ON S.id = C;", 0, 1, None),
        ("SELECT * FROM $planets AS P LEFT SEMI JOIN $satellites AS S ON S.id = P.id WHERE P.name LIKE 'E%';", 1, 20, None),
        ("SELECT * FROM $planets AS P LEFT SEMI JOIN $satellites AS S ON S.id = P.id WHERE S.name LIKE 'E%';", 1, 20, UnexpectedDatasetReferenceError),
        ("SELECT * FROM $planets AS P LEFT SEMI JOIN (SELECT id FROM $satellites WHERE name != 'Moon') AS S ON S.id = P.id;", 8, 20, None),
        ("SELECT * FROM $planets AS P LEFT SEMI JOIN $satellites AS S ON S.id = P.id WHERE P.name != 'Earth';", 8, 20, None),
        ("SELECT * FROM GENERATE_SERIES(1, 10) AS G LEFT SEMI JOIN $satellites AS S ON S.id = G;", 10, 1, None),
        ("EXPLAIN ANALYZE FORMAT TEXT SELECT * FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id);", 3, 7, None),
        ("EXPLAIN ANALYZE FORMAT JSON SELECT * FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id);", 3, 7, UnsupportedSyntaxError),
        ("EXPLAIN ANALYZE FORMAT GRAPHVIZ SELECT * FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id);", 3, 7, UnsupportedSyntaxError),
        ("EXPLAIN ANALYZE FORMAT MERMAID SELECT * FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id);", 1, 1, None),
        ("SELECT DISTINCT ON (planetId) planetId, name FROM $satellites ", 7, 2, None),
        ("SELECT 8 DIV 4", 1, 1, None),

        ("SELECT * FROM $planets WHERE id IN (1)", 1, 20, None),
        ("SELECT * FROM $planets WHERE id NOT IN (1)", 8, 20, None),
        ("SELECT * FROM $planets WHERE id IN (1, 'one')", None, None, ArrayWithMixedTypesError),

        # hitting the boolean rewriter
        ("SELECT * FROM $planets WHERE not(not(id = 7))", 1, 20, None),
        ("SELECT name from sqlite.planets WHERE NOT (id = 1 or id = 9)", 7, 1, None),
        ("SELECT name from sqlite.planets WHERE NOT (id != 1)", 1, 1, None),
        ("SELECT * FROM $planets WHERE (id = 2 AND id = 3) OR (id = 4 AND id = 5)", 0, 20, None),
        ("SELECT * FROM $planets WHERE NOT (id < 3 OR id > 7)", 5, 20, None),
        ("SELECT * FROM $planets WHERE id >= 1 AND id <= 9", 9, 20, None),
        ("SELECT * FROM $planets WHERE id = 4 OR NOT (id = 4)", 9, 20, None),
        ("SELECT * FROM $planets WHERE id > 0", 9, 20, None),
        ("SELECT * FROM $planets WHERE id <= 9", 9, 20, None),
        ("SELECT * FROM $planets WHERE (id = 2 OR id = 4) AND NOT (id = 3 OR id = 5)", 2, 20, None),
        ("SELECT * FROM $planets WHERE (id = 1) XOR (id = 2)", 2, 20, None),
        ("SELECT * FROM $planets WHERE NOT (id < 3 AND id > 7)", 9, 20, None),
        ("SELECT * FROM $planets WHERE (id = 3 AND NOT (id = 3))", 0, 20, None),
        ("SELECT * FROM $planets WHERE (id = 6 OR id != 6)", 9, 20, None),

        # Test cases for UNION
        ("SELECT name FROM $planets AS p1 UNION SELECT name FROM $planets AS p2", 9, 1, None),
        ("SELECT name FROM $planets p1 UNION SELECT name FROM $planets p2 WHERE p2.name != 'Earth'", 9, 1, None),
        ("SELECT name FROM $planets UNION SELECT name FROM $planets", 18, 1, AmbiguousDatasetError),
        ("SELECT name FROM $planets AS p1 UNION ALL SELECT name FROM $planets AS p2", 18, 1, None),
        ("SELECT name FROM $planets p1 UNION ALL SELECT name FROM $planets p2 WHERE p2.name = 'Mars'", 10, 1, None),
        ("SELECT name AS planet_name FROM $planets p1 UNION SELECT name FROM $planets p2", 9, 1, None),
        ("SELECT name FROM $planets p1 UNION ALL SELECT name AS planet_name FROM $planets p2", 18, 1, None),
        ("SELECT name FROM $planets AS p1 WHERE name LIKE 'M%' UNION SELECT name FROM $planets AS p2 WHERE name LIKE 'V%'", 3, 1, None),
        ("SELECT name FROM $planets P1 WHERE name LIKE 'E%' UNION ALL SELECT name FROM $planets P2 WHERE name LIKE 'M%'", 3, 1, None),
        ("SELECT name FROM $planets P1 INTERSECT SELECT name FROM $planets P2", 0, 0, UnsupportedSyntaxError),
        ("SELECT name FROM $planets p1 EXCEPT SELECT name FROM $planets p2", 0, 0, UnsupportedSyntaxError),
        ("(SELECT name FROM $planets AS p1) UNION (SELECT name FROM $planets) LIMIT 5", 5, 1, None),
        ("(SELECT name FROM $planets AS p1) UNION (SELECT name FROM $planets) LIMIT 5 OFFSET 4", 5, 1, None),
        ("(SELECT name FROM $planets AS p1) UNION (SELECT name FROM $planets) LIMIT 3 OFFSET 6", 3, 1, None),
        ("(SELECT name FROM $planets AS p1 LIMIT 3) UNION ALL (SELECT name FROM $planets LIMIT 2)", 5, 1, None),
        ("(SELECT name FROM $planets AS p1 OFFSET 2) UNION ALL (SELECT name FROM $planets OFFSET 3)", 13, 1, None),
        ("(SELECT name FROM $planets AS p1 LIMIT 4 OFFSET 1) UNION ALL (SELECT name FROM $planets LIMIT 3 OFFSET 2)", 7, 1, None),
        ("(SELECT name FROM $planets AS p1) UNION ALL (SELECT name FROM $planets) LIMIT 10", 10, 1, None),
        ("(SELECT name FROM $planets AS p1) UNION ALL (SELECT name FROM $planets) OFFSET 8", 10, 1, None),
        ("(SELECT name FROM $planets AS p1 LIMIT 5) UNION ALL (SELECT name FROM $planets OFFSET 3)", 11, 1, None),
        ("(SELECT name FROM $planets AS p1 LIMIT 4 OFFSET 2) UNION ALL (SELECT name FROM $planets LIMIT 3 OFFSET 1) LIMIT 5 OFFSET 3", 4, 1, None),
        ("SELECT mass FROM $planets AS p1 UNION SELECT diameter FROM $planets", 18, 1, None),
        ("SELECT mass AS m FROM $planets AS p1 UNION SELECT mass FROM $planets", 9, 1, None),
        ("SELECT name FROM $planets AS p1 WHERE mass IS NULL UNION SELECT name FROM $planets WHERE diameter IS NULL", 0, 1, None),
        ("SELECT name FROM $planets AS p1 UNION ALL SELECT name FROM $planets", 18, 1, None),  # Assuming no large data set available
        ("SELECT name FROM (SELECT name FROM $planets P1 UNION SELECT name FROM $planets) AS subquery", 9, 1, None),
        ("SELECT a.name FROM $planets a JOIN (SELECT name FROM $planets AS P1 UNION SELECT name FROM $planets) b ON a.name = b.name", 9, 1, None),
        ("(SELECT name FROM $planets AS P1  ORDER BY mass DESC) UNION (SELECT name FROM $planets ORDER BY diameter ASC)", 9, 1, None),
        ("SELECT gravity FROM $planets AS P1 UNION SELECT gravity FROM $planets", 8, 1, None),  # Assuming two planets have the same gravity
        ("(SELECT name FROM $planets AS p1 LIMIT 3) UNION ALL (SELECT name FROM $planets OFFSET 2)", 10, 1, None),
        ("SELECT AVG(mass) FROM $planets AS p1 UNION SELECT SUM(diameter) FROM $planets", 2, 1, None),
        ("SELECT name FROM $planets AS p1 WHERE mass > 1.5 OR diameter < 10000 UNION SELECT name FROM $planets WHERE gravity = 3.7", 9, 1, None),

        # New and improved JOIN UNNESTs
        ("SELECT * FROM $planets CROSS JOIN UNNEST(('Earth', 'Moon')) AS n", 18, 21, None),
        ("SELECT * FROM $planets INNER JOIN UNNEST(('Earth', 'Moon')) AS n ON name = n", 1, 21, None),
        ("SELECT name, mission FROM $astronauts INNER JOIN UNNEST(missions) as mission ON mission = name", 0, 2, None),
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(missions) AS mission WHERE mission IN ('Apollo 11', 'Apollo 12')", 6, 20, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(name) as n FROM $astronauts GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn", 357, 2, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(name) as n FROM $astronauts WHERE status = 'Retired' GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn", 220, 2, None),
        ("SELECT number FROM $astronauts CROSS JOIN UNNEST((1, 2, 3, 4, 5)) AS number", 1785, 1, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(name) as n FROM $astronauts WHERE birth_date < '1960-01-01' GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn", 269, 2, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(name) as n FROM $astronauts WHERE status = 'Active' AND birth_date > '1970-01-01' GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn", 10, 2, None),
        ("SELECT group, nn FROM (SELECT group, ARRAY_AGG(name) as n FROM $astronauts GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn", 357, 2, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(CASE WHEN LENGTH(alma_mater) > 10 THEN alma_mater ELSE NULL END) as alma_mater_arr FROM $astronauts GROUP BY group) AS alma CROSS JOIN UNNEST(alma_mater_arr) AS alma", 357, 2, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(1) as num_arr FROM $astronauts GROUP BY group) AS numbers CROSS JOIN UNNEST(num_arr) AS number", 357, 2, None),
        ("SELECT * FROM $astronauts INNER JOIN UNNEST(alma_mater) AS n ON name = n", 0, 20, None),
        ("SELECT * FROM $astronauts INNER JOIN UNNEST(alma_mater) AS n ON name = n WHERE group = 10", 0, 20, None),
        ("SELECT name, group, astronaut_year, LEAST(year_list) FROM (SELECT ARRAY_AGG(year) as year_list, name, group FROM $astronauts WHERE status = 'Retired' and year is not null GROUP BY group, name) AS alma CROSS JOIN UNNEST(year_list) AS astronaut_year", 196, 4, None),

        # PUSHDOWN (the result should be the same without pushdown)
        ("SELECT p.name, s.name FROM $planets p, $satellites s WHERE p.id = s.planetId AND p.mass > 1000 AND s.gm < 500;", 63, 2, None),
        ("SELECT p.name, sub.name FROM $planets p CROSS JOIN (SELECT name, planetId FROM $satellites WHERE gm < 1000) AS sub WHERE p.id = sub.planetId;", 170, 2, None),
        ("SELECT p.name, s.name FROM $planets p, $satellites s WHERE p.id = s.planetId AND p.id = s.id;", 1, 2, None),
        ("SELECT p.name, COUNT(s.id) FROM $planets p JOIN $satellites s ON p.id = s.planetId GROUP BY p.name HAVING COUNT(s.id) > 3;", 5, 2, None),
        ("SELECT COUNT(*) FROM $planets WHERE TRUE AND 3 = 2 AND 3 > 2", 1, 1, None),

        ("SELECT missions[0] as m FROM $astronauts CROSS JOIN FAKE(1, 1) AS F order by m", 357, 1, None),
        ("SELECT name[id] as m FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT * FROM $astronauts WHERE LIST_CONTAINS_ANY(missions, @@user_memberships)", 3, 19, None),
        ("SELECT $missions.* FROM $missions INNER JOIN $user ON Mission = value WHERE attribute = 'membership'", 1, 8, None),
        ("SELECT * FROM $planets WHERE name = any(@@user_memberships)", 0, 20, None),
        ("SELECT name FROM $planets WHERE 'Apollo 11' = ANY(@@user_memberships) OR name = 'Saturn'", 9, 1, None),
        ("SELECT name FROM $planets WHERE 'Apollo 11' = ANY(@@user_memberships) AND name = 'Saturn'", 1, 1, None),
        ("SELECT name FROM $planets WHERE 'Apollo 11' = ANY(@@user_memberships)", 9, 1, None),
        ("SELECT name FROM sqlite.planets WHERE name = ANY(('Earth', 'Mars'))", 2, 1, None),
        ("SELECT name FROM $planets WHERE REGEXP_REPLACE(name, '^E', 'G') == 'Garth'", 1, 1, None),

        # TEST FUNCTIONS
        ("EXECUTE PLANETS_BY_ID (id=1)", 1, 20, None),  # simple case
        ("EXECUTE PLANETS_BY_ID (1)", None, None, ParameterError),  # simple case)
        ("EXECUTE PLANETS_BY_ID (name=1)", None, None, ParameterError),  # simple case)
        ("EXECUTE VERSION", 1, 1, None),  # no paramters
        ("EXECUTE VERSION()", 1, 1, None),
        ("EXECUTE get_satellites_by_planet_name(name='Jupiter')", 67, 1, None),  # string param
        ("EXECUTE GET_SATELLITES_BY_PLANET_NAME(name='Jupiter')", 67, 1, None),  # string param
        ("EXECUTE multiply_two_numbers (one=1.0, two=9.9)", 1, 1, None),  # multiple params

        ("EXECUTE multiply_two_numbers (one=0, two=9.9)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=1.0, two=0)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=0, two=0)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=-1, two=9.9)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=1.0, two=-9.9)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=-1, two=-9.9)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=0.000000001, two=9.9)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=1.0, two=0.000000001)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=0.000000001, two=0.000000001)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=0, two=-9.9)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=-1, two=0)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=0, two=0.000000001)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=1, two=9.9)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=1, two=-9.9)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=-1, two=1)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (two=9.9, two=-1)", 1, 1, ParameterError),
        ("EXECUTE multiply_two_numbers (two=0.000000001, three=0.000000001)", 1, 1, ParameterError),
        ("EXECUTE multiply_two_numbers (two=0, one=1.0)", 1, 1, None),
        ("EXECUTE multiply_two_numbers (one=-9.9, one=0)", 1, 1, ParameterError),

        # TEST VIEWS
        ("SELECT * FROM mission_reports", 177, 1, None),
        ("SELECT * FROM mission_reports AS MR", 177, 1, None),
        ("SELECT MR.* FROM mission_reports AS MR", 177, 1, None),
        ("SELECT satellite_name FROM mission_reports", 177, 1, None),
        ("SELECT MR.satellite_name FROM mission_reports AS MR", 177, 1, None),
        ("SELECT satellite_name FROM mission_reports AS MR WHERE satellite_name ILIKE '%a%'", 90, 1, None),
        ("SELECT satellite_name FROM mission_reports AS MR WHERE satellite_name ILIKE '%a%'", 90, 1, None),
        ("SELECT * FROM mission_reports INNER JOIN $satellites ON satellite_name = name", 177, 9, None),
        ("SELECT * FROM my_mission_reports", 3, 19, None),
        ("SELECT * FROM my_mission_reports WHERE year = 1963", 2, 19, None),
        ("SELECT * FROM my_mission_reports ORDER BY name", 3, 19, None),
        ("SELECT name, status FROM my_mission_reports", 3, 2, None),

        ("SELECT id, CASE WHEN id = 1 THEN 'Mercury' WHEN id = 3 THEN 'Earth' ELSE 'Elsewhere' END as place FROM $planets", 9, 2, None),
        ("SELECT id, CASE WHEN id = 1 THEN 'Mercury' WHEN id = 3 THEN 'Earth' ELSE NULL END as place FROM $planets", 9, 2, None),
        ("SELECT IFNULL(NULL, 'default') as result", 1, 1, None),
        ("SELECT IFNULL('value', 'default') as result", 1, 1, None),
        ("SELECT IFNULL(NULL, NULL) as result", 1, 1, None),
        ("SELECT IFNOTNULL(NULL, 'default') as result", 1, 1, None),
        ("SELECT IFNOTNULL('value', 'default') as result", 1, 1, None),
        ("SELECT IFNOTNULL(NULL, NULL) as result", 1, 1, None),
        ("SELECT COALESCE(NULL, 'default') as coalesced_value", 1, 1, None),
        ("SELECT COALESCE(NULL, 'default', 'fallback') as coalesced_value", 1, 1, None),
        ("SELECT COALESCE('first', NULL, 'fallback') as coalesced_value", 1, 1, None),
        ("SELECT LENGTH(name) FROM $planets", 9, 1, None),
        ("SELECT LENGTH(name) FROM $astronauts WHERE LENGTH(name) > 15", 248, 1, None),
        ("SELECT planetId, COUNT(*) FROM $satellites GROUP BY planetId HAVING COUNT(*) > 10", 4, 2, None),
        ("SELECT planetId, MIN(magnitude) FROM $satellites GROUP BY planetId HAVING MIN(magnitude) < 5", 2, 2, None),
        ("SELECT ABS(-5) as abs_value", 1, 1, None),
        ("SELECT ROUND(3.14159, 2) as rounded_value", 1, 1, None),
        ("SELECT CEIL(3.14159) as ceil_value", 1, 1, None),
        ("SELECT FLOOR(3.14159) as floor_value", 1, 1, None),
        ("SELECT CEIL(3.14159, 2) as ceil_value", 1, 1, None),
        ("SELECT FLOOR(3.14159, 2) as floor_value", 1, 1, None),
        ("SELECT CEIL(3.14159, 0) as ceil_value", 1, 1, None),
        ("SELECT FLOOR(3.14159, 0) as floor_value", 1, 1, None),
        ("SELECT CEIL(3.14159, -1) as ceil_value", 1, 1, None),
        ("SELECT FLOOR(3.14159, -1) as floor_value", 1, 1, None),
        ("SELECT UPPER(name) FROM $planets", 9, 1, None),
        ("SELECT LOWER(name) FROM $astronauts WHERE UPPER(name) LIKE 'A%'", 11, 1, None),
        ("SELECT REVERSE(name) FROM $planets", 9, 1, None),
        ("SELECT TRIM('   space   ') as trimmed", 1, 1, None),
        ("SELECT SUBSTRING('planet', 1, 3) as substring", 1, 1, None),
        ("SELECT LPAD(name, 10, '*') FROM $planets", 9, 1, None),
        ("SELECT RPAD(name, 10, '-') FROM $planets", 9, 1, None),
        ("SELECT SEARCH(name, 'a') FROM $planets", 9, 1, None),
        ("SELECT CAST(id AS VARCHAR) FROM $planets", 9, 1, None),
        ("SELECT CAST(magnitude AS INTEGER) FROM $satellites WHERE magnitude IS NOT NULL", 171, 1, None),
        ("SELECT CAST('2022-01-01' AS DATE)", 1, 1, None),
#        ("SELECT DATE_ADD('2022-01-01', INTERVAL 1 DAY)", 1, 1, None),
#        ("SELECT DATE_SUB('2022-01-01', INTERVAL 1 DAY)", 1, 1, None),
#        ("SELECT DATE_DIFF('2022-01-01', '2021-12-31', DAY)", 1, 1, None),
#        ("SELECT CONCAT(name, ' is a planet') FROM $planets", 9, 1, None),
#        ("SELECT CONCAT('Astronaut: ', name) FROM $astronauts WHERE LENGTH(name) > 20", 10, 1, None),
#        ("SELECT CONCAT('Hello', ' ', 'World') as concatenated", 1, 1, None),
        ("SELECT id + 1 FROM $planets", 9, 1, None),
        ("SELECT id - 1 FROM $planets", 9, 1, None),
        ("SELECT id * 2 FROM $planets", 9, 1, None),
        ("SELECT id / 2 FROM $planets", 9, 1, None),
        ("SELECT id % 2 FROM $planets", 9, 1, None),
        ("SELECT id, id + 1 as incremented_id FROM $planets", 9, 2, None),
        ("SELECT id, id * 2 as doubled_id FROM $planets", 9, 2, None),
        ("SELECT id, id - 1 as decremented_id FROM $planets", 9, 2, None),
        ("SELECT id, id / 2.0 as halved_id FROM $planets", 9, 2, None),
        ("SELECT * FROM $planets WHERE id = 1 AND name = 'Mercury'", 1, 20, None),
        ("SELECT * FROM $planets WHERE id = 1 OR id = 2", 2, 20, None),
        ("SELECT * FROM $planets WHERE NOT (id = 1)", 8, 20, None),
        ("SELECT ASCII('A') as ascii_value", 1, 1, None),
        ("SELECT CHAR(65) as char_value", 1, 1, None),

        ("SELECT CEIL(id) FROM $planets", 9, 1, None),  # ints
        ("SELECT FLOOR(id) FROM $planets", 9, 1, None),
        ("SELECT CEIL(id, 2) FROM $planets", 9, 1, None),
        ("SELECT FLOOR(id, 2) FROM $planets", 9, 1, None),
        ("SELECT CEIL(id, 0) FROM $planets", 9, 1, None),
        ("SELECT FLOOR(id, 0) FROM $planets", 9, 1, None),
        ("SELECT CEIL(gravity) FROM $planets", 9, 1, None),  # decimal
        ("SELECT FLOOR(gravity) FROM $planets", 9, 1, None),
        ("SELECT CEIL(gravity, 2) FROM $planets", 9, 1, None),
        ("SELECT FLOOR(gravity, 2) FROM $planets", 9, 1, None),
        ("SELECT CEIL(gravity, 0) FROM $planets", 9, 1, None),
        ("SELECT FLOOR(gravity, 0) FROM $planets", 9, 1, None),
        ("SELECT CEIL(mass) FROM $planets", 9, 1, None),  # double
        ("SELECT FLOOR(mass) FROM $planets", 9, 1, None),
        ("SELECT CEIL(mass, 2) FROM $planets", 9, 1, None),
        ("SELECT FLOOR(mass, 2) FROM $planets", 9, 1, None),
        ("SELECT CEIL(mass, 0) FROM $planets", 9, 1, None),
        ("SELECT FLOOR(mass, 0) FROM $planets", 9, 1, None),
        ("SELECT CEIL(surface_pressure) FROM $planets", 9, 1, None),  # with nulls
        ("SELECT FLOOR(surface_pressure) FROM $planets", 9, 1, None),
        ("SELECT CEIL(surface_pressure, 2) FROM $planets", 9, 1, None),
        ("SELECT FLOOR(surface_pressure, 2) FROM $planets", 9, 1, None),
        ("SELECT CEIL(surface_pressure, 0) FROM $planets", 9, 1, None),
        ("SELECT FLOOR(surface_pressure, 0) FROM $planets", 9, 1, None),

        # We rewrite expressions like this, make sure all variations work
        ("SELECT * FROM $satellites WHERE id - 3 < 8;", 10, 8, None),
        ("SELECT * FROM $satellites WHERE 8 > id - 3;", 10, 8, None),
        ("SELECT * FROM $satellites WHERE 3 > 8 - id;", 172, 8, None),
        ("SELECT * FROM $satellites WHERE 8 - id < 3;", 172, 8, None),
        ("SELECT * FROM $satellites WHERE id < 8 + 3;", 10, 8, None),
        ("SELECT * FROM $satellites WHERE 8 + 3 > id;", 10, 8, None),

        # rewriting date functions has addition complexity
        ("SELECT * FROM $missions WHERE Launched_at - INTERVAL '7' DAY < current_time;", 4503, 8, None),
        ("SELECT * FROM $missions WHERE current_time > Launched_at - INTERVAL '7' DAY;", 4503, 8, None),
        ("SELECT * FROM $missions WHERE INTERVAL '7' DAY < current_time - Launched_at;", 4503, 8, None),
        ("SELECT * FROM $missions WHERE current_time - Launched_at > INTERVAL '7' DAY;", 4503, 8, None),
        ("SELECT * FROM $missions WHERE Launched_at < current_time + INTERVAL '7' DAY;", 4503, 8, None),
        ("SELECT * FROM $missions WHERE current_time > Launched_at + INTERVAL '7' DAY;", 4503, 8, None),

        ("SELECT ARRAY_AGG(id) FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(id) AS pids FROM $satellites GROUP BY planetId) AS sats", 7, 1, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats", 7, 2, None),
        ("SELECT * FROM $planets INNER JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats ON sats.planetId = $planets.id", 7, 22, None),
        ("SELECT * FROM $planets LEFT JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats ON sats.planetId = $planets.id", 9, 22, None),
        ("SELECT * FROM (SELECT * FROM $planets LEFT JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats ON sats.planetId = $planets.id) as satellites", 9, 22, None),
        ("SELECT pids FROM (SELECT * FROM $planets LEFT JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats ON sats.planetId = $planets.id) as satellites", 9, 1, None),
        ("SELECT pid FROM (SELECT * FROM $planets LEFT JOIN (SELECT ARRAY_AGG(id) AS pids, planetId FROM $satellites GROUP BY planetId) AS sats ON sats.planetId = $planets.id) as satellites CROSS JOIN UNNEST(pids) AS pid", 177, 1, None),
        ("SELECT * FROM (SELECT LENGTH(ARRAY_AGG(DISTINCT planetId)) AS L FROM $satellites GROUP BY planetId) AS I WHERE L == 1;", 7, 1, None),

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

        # Edge Case with Empty Joins
        ("SELECT * FROM $planets LEFT JOIN (SELECT id FROM $satellites WHERE planetId < 0) AS S ON $planets.id = S.id", 9, 21, None),
        # Handling NULL Comparisons in WHERE Clause
        ("SELECT * FROM $planets WHERE id IS NOT NULL AND id < NULL", 0, 20, None),
        # Edge Case with Temporal Filtering and Subqueries
        ("SELECT * FROM (SELECT * FROM $planets FOR '2024-10-01' WHERE id > 5) AS S WHERE id < 10", 4, 20, None),
        # Function Filtering with Aggregate Functions
        ("SELECT MAX(density), COUNT(*) FROM $planets WHERE LENGTH(name) > 4 GROUP BY orbitalVelocity", 8, 2, None),
        # Complex Nested Subqueries
        ("SELECT * FROM (SELECT name FROM (SELECT name FROM $planets WHERE LENGTH(name) > 3) AS T1) AS T2", 9, 1, None),
        # Test for Zero-Length String Comparison
        ("SELECT * FROM $satellites WHERE name = ''", 0, 8, None),
        # Cross Joining with Non-Matching Conditions
        ("SELECT P.name, S.name FROM $planets AS P CROSS JOIN $satellites AS S WHERE P.id = S.id AND P.name LIKE '%X%'", 0, 2, None),
        # Edge Case with LIKE and NULL Handling
        ("SELECT * FROM $planets WHERE name NOT LIKE '%a%' OR name IS NULL", 5, 20, None),
        # Complex GROUP BY with Multiple Expressions
        ("SELECT COUNT(*), LENGTH(name), ROUND(density, 2) FROM $planets GROUP BY LENGTH(name), ROUND(density, 2)", 9, 3, None),
        # Distinct Filtering with Aggregate Functions
        ("SELECT DISTINCT MAX(density) FROM $planets WHERE orbitalInclination > 1 GROUP BY numberOfMoons", 6, 1, None),
        # Edge Case with Temporal Filters Using Special Date Functions
        ("SELECT * FROM $planets FOR '2024-10-01' WHERE DATE(NOW()) > '2024-10-01'", 9, 20, None),
        # Ordering with NULLs First and Last
        ("SELECT * FROM $planets ORDER BY lengthOfDay NULLS LAST", 9, 20, None),
        # Multi-Level Subquery with Different Alias Names
        ("SELECT * FROM (SELECT * FROM (SELECT * FROM $planets WHERE id > 5) AS SQ1) AS SQ2 WHERE id < 10", 4, 20, None),
        # Edge Case Testing Subscripts on Arrays with NULL Values
        ("SELECT name[0] FROM $planets WHERE id IS NULL", 0, 1, None),
        # Testing Intervals with Arithmetic Expressions
        ("SELECT * FROM $planets WHERE TIMESTAMP '2024-10-01' + INTERVAL '2' DAY > CURRENT_TIME", 0, 20, None),
        # Edge Case with JSON-Like Filtering
        ("SELECT * FROM $astronauts WHERE birth_place->>'city' = 'New York'", 0, 19, None),
        # Ordering on Computed Columns with Aliases
        ("SELECT name, LENGTH(name) AS len FROM $planets ORDER BY len DESC", 9, 2, None),
        # Aggregate Functions with HAVING Clause
        ("SELECT name, COUNT(*) AS count FROM $satellites GROUP BY name HAVING count > 1", 0, 2, None),

        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY '%apoll%'", 0, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions ILIKE ANY '%apoll%'", 34, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('%apoll%')", 0, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions ILIKE ANY ('%apoll%')", 34, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('%Apoll%')", 34, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions ILIKE ANY ('%Apoll%')", 34, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('%Apoll%', 'mission')", 34, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions ILIKE ANY ('%Apoll%', 'mission')", 34, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT LIKE ANY '%apoll%'", 334, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT ILIKE ANY '%apoll%'", 300, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT LIKE ANY ('%apoll%')", 334, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT ILIKE ANY ('%apoll%')", 300, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT LIKE ANY ('%Apoll%')", 300, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT ILIKE ANY ('%Apoll%')", 300, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT LIKE ANY ('%Apoll%', 'mission')", 300, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT ILIKE ANY ('%Apoll%', 'mission')", 300, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('Apoll%', 'Gemini%', 'Mercury%')", 37, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT LIKE ANY ('Apoll%', 'Gemini%', 'Mercury%')", 297, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ()", 0, 2, SqlError),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT LIKE ANY ()", 0, 2, SqlError),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('%Apoll%', null)", 34, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT LIKE ANY ('%Apoll%', null)", 300, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('%aPoll%')", 0, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions ILIKE ANY ('%aPoll%')", 34, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('Apollo 11')", 3, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions NOT LIKE ANY ('Apollo 11')", 331, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('Apollo_%')", 34, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('Apo__o%')", 34, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('%Apoll%', 123)", 34, 2, IncorrectTypeError),
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('%pattern1%', '%pattern2%', '%pattern3%', '%pattern4%', '%pattern5%', '%pattern6%', '%pattern7%', '%pattern8%', '%pattern9%', '%pattern10%', '%pattern11%', '%pattern12%', '%pattern13%', '%pattern14%', '%pattern15%', '%pattern16%', '%pattern17%', '%pattern18%', '%pattern19%', '%pattern20%', '%pattern21%', '%pattern22%', '%pattern23%', '%pattern24%', '%pattern25%', '%pattern26%', '%pattern27%', '%pattern28%', '%pattern29%', '%pattern30%', '%pattern31%', '%pattern32%', '%pattern33%', '%pattern34%', '%pattern35%', '%pattern36%', '%pattern37%', '%pattern38%', '%pattern39%', '%pattern40%', '%pattern41%', '%pattern42%', '%pattern43%', '%pattern44%', '%pattern45%', '%pattern46%', '%pattern47%', '%pattern48%', '%pattern49%', '%pattern50%');", 0, 2, None),

        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY '%armstrong%'", 0, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name ILIKE ANY '%armstrong%'", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%arms%')", 0, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name ILIKE ANY ('%arms%')", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%Armstrong%')", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name ILIKE ANY ('%Arms%')", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%Armstrong%', 'mission')", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name ILIKE ANY ('%Armstrong%', 'mission')", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT LIKE ANY '%armstrong%'", 357, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT ILIKE ANY '%armstrong%'", 356, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT LIKE ANY ('%armstrong%')", 357, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT ILIKE ANY ('%armstrong%')", 356, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT LIKE ANY ('%Armstrong%')", 356, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT ILIKE ANY ('%Armstrong%')", 356, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT LIKE ANY ('%Armstrong%', 'mission')", 356, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT ILIKE ANY ('%Armstrong%', 'mission')", 356, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%Armstrong%', '%Aldrin%', '%Collins%')", 4, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT LIKE ANY ('%Armstrong%', '%Aldrin%', '%Collins%')", 353, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ()", 0, 2, SqlError),
        ("SELECT name, missions FROM $astronauts WHERE name NOT LIKE ANY ()", 0, 2, SqlError),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%Armstrong%', null)", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT LIKE ANY ('%Armstrong%', null)", 356, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%aRmstrong%')", 0, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name ILIKE ANY ('%aRmstrong%')", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('Neil A. Armstrong')", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name NOT LIKE ANY ('Neil A. Armstrong')", 356, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%__Armstrong%')", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%Arm__rong%')", 1, 2, None),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%Armstrong%', 123)", 1, 2, IncorrectTypeError),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%pattern1%', '%pattern2%', '%pattern3%', '%pattern4%', '%pattern5%', '%pattern6%', '%pattern7%', '%pattern8%', '%pattern9%', '%pattern10%', '%pattern11%', '%pattern12%', '%pattern13%', '%pattern14%', '%pattern15%', '%pattern16%', '%pattern17%', '%pattern18%', '%pattern19%', '%pattern20%', '%pattern21%', '%pattern22%', '%pattern23%', '%pattern24%', '%pattern25%', '%pattern26%', '%pattern27%', '%pattern28%', '%pattern29%', '%pattern30%', '%pattern31%', '%pattern32%', '%pattern33%', '%pattern34%', '%pattern35%', '%pattern36%', '%pattern37%', '%pattern38%', '%pattern39%', '%pattern40%', '%pattern41%', '%pattern42%', '%pattern43%', '%pattern44%', '%pattern45%', '%pattern46%', '%pattern47%', '%pattern48%', '%pattern49%', '%pattern50%');", 0, 2, None),

        ("SELECT max(current_time), name FROM $satellites group by name", 177, 2, None),
        ("SELECT max(1), name FROM $satellites group by name", 177, 2, None),
        ("SELECT max(1) FROM $satellites", 1, 1, None),
        ("SELECT max('a'), name FROM $satellites group by name", 177, 2, None),
        ("SELECT max('a') FROM $satellites", 1, 1, None),
        ("SELECT min(current_time), name FROM $satellites group by name", 177, 2, None),
        ("SELECT min(1), name FROM $satellites group by name", 177, 2, None),
        ("SELECT min(1) FROM $satellites", 1, 1, None),
        ("SELECT min('a'), name FROM $satellites group by name", 177, 2, None),
        ("SELECT min('a') FROM $satellites", 1, 1, None),
        ("SELECT count(current_time), name FROM $satellites group by name", 177, 2, None),
        ("SELECT count(1), name FROM $satellites group by name", 177, 2, None),
        ("SELECT count(1) FROM $satellites", 1, 1, None),
        ("SELECT count('a'), name FROM $satellites group by name", 177, 2, None),
        ("SELECT count('a') FROM $satellites", 1, 1, None),
        ("SELECT avg(1), name FROM $satellites group by name", 177, 2, None),
        ("SELECT avg(1) FROM $satellites", 1, 1, None),
        ("SELECT surface_pressure FROM $planets WHERE IFNOTNULL(surface_pressure, 0.0) == 0.0", 5, 1, None),
        ("SELECT username FROM testdata.flat.ten_files WHERE SQRT(followers) = 10 ORDER BY followers DESC LIMIT 10", 1, 1, None),
        ("SELECT username FROM testdata.flat.ten_files WHERE SQRT(followers) = 15 ORDER BY followers DESC LIMIT 10", 0, 1, None),
        
        ("SELECT HUMANIZE(1000)", 1, 1, None),
        ("SELECT HUMANIZE(COUNT(*)) FROM $planets", 1, 1, None),
        ("SELECT HUMANIZE(gravity) FROM $planets", 9, 1, None), 

        ("SELECT * EXCEPT(id) FROM $planets", 9, 19, None),
        ("SELECT * EXCEPT(id, name) FROM $planets", 9, 18, None),
        ("SELECT * EXCEPT(missing) FROM $planets", 9, 1, ColumnNotFoundError),
        ("SELECT * EXCEPT(id, missing) FROM $planets", 9, 1, ColumnNotFoundError),
        ("SELECT * EXCEPT(missing, id) FROM $planets", 9, 1, ColumnNotFoundError),
        ("SELECT * EXCEPT(name, missing, id) FROM $planets", 9, 1, ColumnNotFoundError),
        ("SELECT * EXCEPT(nmae, pid) FROM $planets", 9, 1, ColumnNotFoundError),
        ("SELECT * EXCEPT (id, name, gravity) FROM $planets WHERE id > 3", 6, 17, None),
        ("SELECT DISTINCT * EXCEPT (id, name, gravity) FROM testdata.planets", 9, 17, None),
        ("SELECT * FROM (SELECT * EXCEPT (id) FROM $planets) AS A", 9, 19, None),
        ("SELECT * EXCEPT (id) FROM (SELECT * FROM $planets) AS A", 9, 19, None),
        ("SELECT * EXCEPT (id) FROM (SELECT id AS pid, name FROM $planets) AS A", None, None, ColumnNotFoundError),
        ("SELECT * EXCEPT (pid) FROM (SELECT id AS pid, name FROM $planets) AS A", 9, 1, None),

        ("SELECT MIN(id), MAX(id), MIN(id + 1), MAX(id + 1), MIN(id + 2), MAX(id + 2), MIN(id * 2) FROM $planets", 1, 7, None),
        ("SELECT SUM(id), SUM(id + 1), SUM(id + 2) FROM $planets", 1, 3, None),
        ("SELECT SUM(id), SUM(id + 1), SUM(id + 2), COUNT(id) FROM $planets", 1, 4, None),
        ("SELECT MIN(id), MIN(id + 1), MIN(id + 2), COUNT(id) FROM $planets", 1, 4, None),
        ("SELECT MIN(id), MAX(id + 1), SUM(id + 2), COUNT(id) FROM $planets", 1, 4, None),

        # ****************************************************************************************

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
        ("SELECT * FROM (SELECT * FROM $planets ORDER BY id DESC LIMIT 5) AS SQ WHERE id > 7", 2, 20, None),
        # ORDER OF JOIN CONDITION
        ("SELECT * FROM $planets INNER JOIN $satellites ON $satellites.planetId = $planets.id", 177, 28, None),
        # ORDER BY QUALIFIED IDENTIFIER
        ("SELECT * FROM $planets LEFT JOIN $satellites ON $satellites.planetId = $planets.id ORDER BY $planets.name", 179, 28, None),
        # NAMED SUBQUERIES
        ("SELECT P.name FROM ( SELECT * FROM $planets ) AS P", 9, 1, None),
        # UNNEST
        ("SELECT * FROM testdata.partitioned.unnest_test FOR '2000-01-01' CROSS JOIN UNNEST (values) AS value ", 11, 3, None),
        # FRAME HANDLING
        ("SELECT * FROM testdata.partitioned.framed FOR '2021-03-28'", 100000, 1, None),
        ("SELECT * FROM testdata.partitioned.framed FOR '2021-03-29'", 100000, 1, None),
        ("SELECT * FROM testdata.partitioned.framed FOR DATES BETWEEN '2021-03-28' AND '2021-03-29'", 200000, 1, None),
        ("SELECT * FROM testdata.partitioned.framed FOR DATES BETWEEN '2021-03-29' AND '2021-03-30'", 100000, 1, None),
        ("SELECT * FROM testdata.partitioned.framed FOR DATES BETWEEN '2021-03-28' AND '2021-03-30'", 200000, 1, None),
        # PAGING OF DATASETS AFTER A GROUP BY [#179]
        ("SELECT * FROM (SELECT COUNT(*), column_1 FROM FAKE(5000,2) AS FK GROUP BY column_1 ORDER BY COUNT(*)) AS SQ LIMIT 5", 5, 2, None),
        # FILTER CREATION FOR 3 OR MORE ANDED PREDICATES FAILS [#182]
        ("SELECT * FROM $astronauts WHERE name LIKE '%o%' AND `year` > 1900 AND gender ILIKE '%ale%' AND group IN (1,2,3,4,5,6)", 41, 19, None),
        # LIKE-ING NULL
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username LIKE 'BBC%'", 3, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username ILIKE 'BBC%'", 3, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username NOT LIKE 'BBC%'", 21, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE NOT username LIKE 'BBC%'", 21, 5, None),
        ("SELECT * FROM testdata.partitioned.nulls FOR '2000-01-01' WHERE username NOT ILIKE 'BBC%'", 21, 5, None),
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
        ("SELECT * FROM (SELECT id from $planets AS PO) AS ONE LEFT JOIN (SELECT id from $planets AS PT) AS TWO ON id = id", 9, 2, AmbiguousIdentifierError),
        ("SELECT * FROM (SELECT id FROM $planets AS PONE) AS ONE LEFT JOIN (SELECT id FROM $planets AS PTWO) AS TWO ON ONE.id = TWO.id;", 9, 2, None),
        # JOIN on UNNEST [#382]
        ("SELECT name FROM $planets INNER JOIN UNNEST(('Earth', 'X')) AS n on name = n ", 1, 1, None),
        ("SELECT name FROM $planets INNER JOIN UNNEST(('Earth', 'Mars')) AS n on name = n", 2, 1, None),
        # SELECT <literal> [#409]
        ("SELECT DATE FROM (SELECT '1980-10-20' AS DATE) AS SQ", 1, 1, None),
        ("SELECT NUMBER FROM (SELECT 1.0 AS NUMBER) AS SQ", 1, 1, None),
        ("SELECT VARCHAR FROM (SELECT 'varchar' AS VARCHAR) AS SQ", 1, 1, None),
        ("SELECT BOOLEAN FROM (SELECT False AS BOOLEAN) AS SQ", 1, 1, None),
        # EXPLAIN has two heads (found looking a [#408])
        ("EXPLAIN SELECT * FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id)", 3, 3, None),
        # ALIAS issues [#408]
        ("SELECT $planets.* FROM $planets INNER JOIN (SELECT id FROM $planets AS IP) AS b USING (id)", 9, 20, None),
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
        ("SELECT P0.id, P1.ID, P2.ID FROM $planets AS P0 JOIN (SELECT id AS ID, name FROM $planets AS ppp) AS P1 ON P0.name = P1.name JOIN (SELECT id, name AS ID FROM $planets AS pppp) AS P2 ON P0.name = P2.name", 9, 3, ColumnNotFoundError),
        ("SELECT P0.id, P1.ID, P2.ID FROM $planets AS P0 JOIN (SELECT id AS ID, name FROM $planets AS ppp) AS P1 ON P0.name = P1.name JOIN (SELECT id, name AS ID FROM $planets AS pppp) AS P2 ON P0.name = P2.ID", 9, 3, None),
        ("SELECT P0.id, P1.ID FROM $planets AS P0 INNER JOIN (SELECT id, name AS ID FROM $planets) AS P1 ON P0.name = P1.name", 9, 2, ColumnNotFoundError),
        ("SELECT P0.id, P1.ID FROM $planets AS P0 INNER JOIN (SELECT id, name AS ID FROM $planets) AS P1 ON P0.name = P1.ID", 9, 2, None),
        ("SELECT P0.id, P1.ID FROM $planets AS P0 INNER JOIN (SELECT name, id AS ID FROM $planets) AS P1 USING (name)", 9, 2, None),
        ("SELECT P0.id, P1.ID FROM $planets AS P0 LEFT JOIN (SELECT id, name AS ID FROM $planets) AS P1 ON P0.name = P1.ID", 9, 2, None),
        # [#475] a variation of #471
        ("SELECT P0.id, P1.ID, P2.ID FROM $planets AS P0 JOIN (SELECT CONCAT_WS(' ', list(id)) AS ID, MAX(name) AS n FROM $planets AS Q1 GROUP BY gravity) AS P1 ON P0.name = P1.n JOIN (SELECT CONCAT_WS(' ', list(id)) AS ID, MAX(name) AS n FROM $planets AS Q2 GROUP BY gravity) AS P2 ON P0.name = P2.n", 8, 3, None),
        # no issue number - but these two caused a headache
        # FUNCTION (AGG)
        ("SELECT CONCAT(LIST(name)) FROM $planets GROUP BY gravity", 8, 1, None),
        # AGG (FUNCTION)
        ("SELECT SUM(IIF(year < 1970, 1, 0)), MAX(year) FROM $astronauts", 1, 2, None),
        # [#527] variables referenced in subqueries
        ("SET @v = 1; SELECT * FROM (SELECT @v) AS S;", 1, 1, None),
        # [#561] HASH JOIN with an empty table
        ("SELECT * FROM $planets LEFT JOIN (SELECT planetId as id FROM $satellites WHERE id < 0) USING (id)", None, None, UnnamedSubqueryError),  
        ("SELECT * FROM $planets LEFT JOIN (SELECT planetId as id FROM $satellites WHERE id < 0) AS S USING (id)", 9, 20, None),  
        # [#646] Incorrectly placed temporal clauses
        ("SELECT * FROM $planets WHERE 1 = 1 FOR TODAY;", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets GROUP BY name FOR TODAY;", 9, 1, InvalidTemporalRangeFilterError),
        # [#518] SELECT * and GROUP BY can't be used together
        ("SELECT * FROM $planets GROUP BY name", 9, 1, UnsupportedSyntaxError),
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
        ("SELECT * FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_id = -1", 0, 13, None),
        # 912 - optimized boolean evals were ignored
        ("SELECT * FROM $planets WHERE 1 = PI()", 0, 20, None),
        ("SELECT * FROM $planets WHERE PI() = 1", 0, 20, None),
        ("SET disable_optimizer = true; SELECT * FROM $planets WHERE 1 = PI()", 0, 20, None),
        ("SET disable_optimizer = true; SELECT * FROM $planets WHERE PI() = 1", 0, 20, None),
        ("SELECT * FROM $planets WHERE 3.141592653589793238462643383279502 = PI()", 9, 20, None),
        ("SELECT * FROM $planets WHERE PI() = 3.141592653589793238462643383279502", 9, 20, None),
        ("SET disable_optimizer = true; SELECT * FROM $planets WHERE 3.141592653589793238462643383279502 = PI()", 9, 20, None),
        ("SET disable_optimizer = true; SELECT * FROM $planets WHERE PI() = 3.141592653589793238462643383279502", 9, 20, None),
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
        ("SELECT name['n'] FROM $planets", None, None, IncorrectTypeError),
        ("SELECT id['n'] FROM $planets", None, None, IncorrectTypeError),
        # [1008] fuzzy search fails on ints
        ("SELECT * FROM $planets JOIN $planets ON id = 12;", None, None, AmbiguousDatasetError),
        ("SELECT * FROM $planets JOIN $planets ON 12 = id;", None, None, AmbiguousDatasetError),
        # [1006] dots in filenames
        ("SELECT * FROM 'testdata/flat/multi/00.01.jsonl'", 1, 4, None),
        # [1015] predicate pushdowns
        ("SELECT * FROM $planets WHERE rotationPeriod = lengthOfDay", 3, 20, None),
        ("SELECT * FROM 'testdata.planets' WITH(NO_PARTITION) WHERE rotationPeriod = lengthOfDay", 3, 20, None),
        ("SELECT * FROM 'testdata/planets/planets.parquet' WITH(NO_PARTITION) WHERE rotationPeriod = lengthOfDay", 3, 20, None),
        # memoization flaws
        ("SELECT LEFT('APPLE', 1), LEFT('APPLE', 1) || 'B'", 1, 2, None),
        ("SELECT LEFT('APPLE', 1) || 'B', LEFT('APPLE', 1)", 1, 2, None),
        ("SELECT LEFT('APPLE', 1) || LEFT('APPLE', 1)", 1, 1, None),
        # 1153 temporal extract from cross joins
        ("SELECT p.name, s.name FROM $planets as p, $satellites as s WHERE p.id = s.planetId", 177, 2, None),
        # Can't qualify fields used in subscripts
        ("SELECT d.birth_place['town'] FROM $astronauts AS d", 357, 1, None),
        # Null columns can't be inverted
        ("SELECT NOT NULL", 1, 1, None),
        # Columns in CAST statements appear to not be bound correctly
        ("SELECT SUM(CASE WHEN gm > 10 THEN 1 ELSE 0 END) AS gm_big_count FROM $satellites", 1, 1, None),
        # COUNT(*) in non aggregated joins
        ("SELECT COUNT(*), COUNT_DISTINCT(id) FROM $planets;", 1, 2, None),
        # NUMPY typles not handled by sqlalchemy
        ("SELECT P_1.* FROM sqlite.planets AS P_1 CROSS JOIN $satellites AS P_2 WHERE P_1.id = P_2.planetId AND P_1.name LIKE '%a%' AND lengthOfDay > 0", 91, 20, None),
        # 1370, issues coercing DATE and TIMESTAMPS
        ("SELECT * FROM $planets WHERE TIMESTAMP '2023-01-01' = DATE '2023-01-01'", 9, 20, None),
        ("SELECT * FROM $planets WHERE 1 = 1.0", 9, 20, None),
        ("SELECT * FROM $planets WHERE DATE '2023-01-01' + INTERVAL '1' MONTH is not null", 9, 20, None),
        ("SELECT * FROM $planets WHERE TIMESTAMP '2023-01-01' + INTERVAL '1' MONTH is not null", 9, 20, None),
        ("SELECT DATE '2023-01-01' + INTERVAL '1' MONTH FROM $planets", 9, 1, None),
        ("SELECT TIMESTAMP '2023-01-01' + INTERVAL '1' MONTH FROM $planets", 9, 1, None),
        ("SELECT * FROM $planets WHERE DATE '2023-01-01' + INTERVAL '1' MONTH < current_time", 9, 20, None),
        ("SELECT * FROM $planets WHERE TIMESTAMP '2023-01-01' + INTERVAL '1' MONTH < current_time", 9, 20, None),
        # 1380
        ("SELECT * FROM $planets INNER JOIN (SELECT * FROM UNNEST((1, 2, 3)) AS id) AS PID USING(id)", 3, 20, None),
        ("SELECT * FROM $planets INNER JOIN UNNEST((1, 2, 3)) AS id USING(id)", 3, 20, None),
        ("SELECT * FROM UNNEST((1, 2, 3)) AS id INNER JOIN $planets USING(id)", 3, 20, None),
        # 1320
        ("SELECT * FROM $planets WHERE LENGTH(name) BETWEEN 4 AND 6", 6, 20, None),
        # 1438
        ("SELECT * FROM $planets, $satellites, $astronauts", None, None, UnsupportedSyntaxError),
        # 1448
        ("SELECT COUNT(*), planetId FROM $satellites", None, None, SqlError),
        # 1462 - unhandled exception when working with empty datasets
        ("SELECT * FROM testdata.partitioned.empty FOR '2000-01-01'", None, None, EmptyDatasetError),
        # 1474
        ("SELECT *, id FROM $planets;", None, None, SqlError),
        ("SELECT id, * FROM $planets;", None, None, SqlError),
        # 1487
        ("SELECT * FROM $planets AS p INNER JOIN $satellites AS s ON p.id = s.planet_id WHERE p.name = 'Jupiter' AND s.radius = 1.0", 25, 28, None),
        ("SELECT * FROM $planets AS p INNER JOIN $satellites AS s ON p.id = s.planet_id WHERE p.name = 'Jupiter'", 67, 28, None),
        ("SELECT * FROM $planets AS p INNER JOIN $planets AS s ON p.id = s.id WHERE p.name = 'Jupiter' AND p.id = 1.0", 0, 40, None),
        ("SELECT * FROM sqlite.planets AS p INNER JOIN sqlite.planets AS s ON p.id = s.id WHERE p.name RLIKE 'Jupiter' AND s.id = 1.0", 0, 40, None),
        ("SELECT * FROM sqlite.planets AS p INNER JOIN sqlite.planets AS s ON p.id = s.id WHERE p.name RLIKE 'Jupiter' AND s.name RLIKE 'Jupiter'", 1, 40, None),
        # 1587
        ("SELECT name, Mission_Status, Mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission_names INNER JOIN $missions ON Mission = mission_names WHERE mission_names = 'Apollo 11'", 3, 3, None),
        ("SELECT name, Mission_Status, Mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission_names INNER JOIN $missions ON Mission = mission_names WHERE Mission = 'Apollo 11'", 3, 3, None),
        # 1607
        ("SELECT birth_place['town'], birth_place['state'] FROM $astronauts;", 357, 2, None),
        ("SELECT birth_place->'town', birth_place->'state' FROM $astronauts;", 357, 2, None),
        ("SELECT birth_place->>'town', birth_place->>'state' FROM $astronauts;", 357, 2, None),
        # 1622
        ("SELECT * FROM (SELECT p.Price AS pri, s.escapeVelocity FROM $missions AS p INNER JOIN $planets AS s ON p.Price = s.escapeVelocity) AS SV WHERE IFNULL(null, pri) = pri", 5, 2, None),
        # 1696
        ("SELECT name FROM (SELECT * FROM $planets LIMIT 5) AS S WHERE name != 'Mars'", 4, 1, None),
        # 1753
        ("SELECT TOP 5 * FROM $planets", None, None, UnsupportedSyntaxError),
        # 1801 CROSS JOIN UNNEST after WHERE
        ("SELECT alma, birth_date FROM $astronauts CROSS JOIN UNNEST(alma_mater) as alma WHERE birth_date > '1950-05-22'", 415, 2, None),
        ("SELECT alma, birth_date FROM $astronauts CROSS JOIN UNNEST(alma_mater) as alma", 681, 2, None),
        # date pushdowns for parquet
        ("SELECT Location FROM testdata.missions WHERE Lauched_at BETWEEN '1950-01-01' AND '1975-01-01'", 1311, 1, None),
        # 1837
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(missions)", None, None, UnnamedColumnError),
        ("SELECT * FROM $astronauts INNER JOIN UNNEST(missions) ON name = name", None, None, UnnamedColumnError),
        # 1841
        ("SELECT * FROM $astronauts, jsonb_object_keys(birth_place) as keys", None, None, UnsupportedSyntaxError),
        # 1848
        ("SELECT name is null from (SELECT name from $planets where id = 90) as s", 0, 1, None),
        ("SELECT * from (SELECT name from $planets where id = 90) as s WHERE name is null", 0, 1, None),
        ("SELECT * from (SELECT * from $planets where id = 90) as s WHERE name is not true", 0, 20, IncorrectTypeError),
        # 1849
        ("SELECT name FROM $planets FOR '1600-01-01' UNION SELECT name FROM $satellites", 183, 1, None),
        # 1850
        ("SELECT IFNULL(alma_mater, null) FROM $astronauts", 357, 1, None),
        ("SELECT IFNULL(alma_mater, []) FROM $astronauts", 357, 1, None),
        # 1854
        ("SELECT s,e FROM generate_series('2024-01-01', '2025-01-01', '1mo') as s, generate_series('2024-01-01', '2025-01-01', '1mo') as e", 169, 2, None),
        ("SELECT * from $planets, $satellites", 1593, 28, None),
        # 1865
        ("SELECT COUNT(*) FROM testdata.missions WHERE Lauched_at < '1970-01-01'", 1, 1, None),
        # 1875 - can't replicate error with test data, these are similar cases
        ("SELECT * FROM $astronauts WHERE IFNULL(birth_place->'state', 'home') == 'CA'", 25, 19, None),
        ("SELECT * FROM $astronauts WHERE IFNULL(GET(birth_place,'state'), 'home') == 'CA'", 25, 19, None),
        # 1880
        ("SELECT name, mission FROM (SELECT name, missions FROM $astronauts) as nauts CROSS JOIN UNNEST (nauts.missions) AS mission WHERE mission != 'Apollo 11'", 843, 2, None),
        ("SELECT name, mission FROM (SELECT name, missions FROM $astronauts) as nauts CROSS JOIN UNNEST (nauts.missions) AS mission WHERE mission > 'Apollo 11'", 837, 2, None),
        ("SELECT name, mission FROM (SELECT name, missions FROM $astronauts) as nauts CROSS JOIN UNNEST (nauts.missions) AS mission WHERE mission >= 'Apollo 11'", 840, 2, None),
        ("SELECT name, mission FROM (SELECT name, missions FROM $astronauts) as nauts CROSS JOIN UNNEST (nauts.missions) AS mission WHERE mission LIKE 'Apollo 11'", 3, 2, None),
        ("SELECT name, mission FROM (SELECT name, missions FROM $astronauts) as nauts CROSS JOIN UNNEST (nauts.missions) AS mission WHERE mission = 'Apollo 11'", 3, 2, None),
        ("SELECT name, mission FROM (SELECT name, missions FROM $astronauts) as nauts CROSS JOIN UNNEST (nauts.missions) AS mission WHERE mission LIKE 'Apollo%11'", 3, 2, None),
        ("SELECT name, mission FROM (SELECT name, missions FROM $astronauts) as nauts CROSS JOIN UNNEST (nauts.missions) AS mission WHERE mission not in ('Apollo 11')", 843, 2, None),
        ("SELECT name, mission FROM (SELECT name, missions FROM $astronauts) as nauts CROSS JOIN UNNEST (nauts.missions) AS mission WHERE NOT mission LIKE 'Apollo 11'", 843, 2, None),
        ("SELECT name, mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission WHERE mission != 'Apollo 11'", 843, 2, None),
        ("SELECT name, mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission WHERE mission > 'Apollo 11'", 837, 2, None),
        ("SELECT name, mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission WHERE mission >= 'Apollo 11'", 840, 2, None),
        ("SELECT name, mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission WHERE mission LIKE 'Apollo 11'", 3, 2, None),
        ("SELECT name, mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission WHERE mission = 'Apollo 11'", 3, 2, None),
        ("SELECT name, mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission WHERE mission LIKE 'Apollo%11'", 3, 2, None),
        ("SELECT name, mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission WHERE mission not in ('Apollo 11')", 843, 2, None),
        ("SELECT name, mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission WHERE NOT mission LIKE 'Apollo 11'", 843, 2, None),
        # 1881 - found when working on 1880 but different enough for a new bug
#        ("SELECT name, mission FROM (SELECT name, missions FROM $astronauts) as nauts CROSS JOIN UNNEST (nauts.missions) AS mission WHERE VARCHAR(mission) = 'Apollo 11'", 3, 2, None),
#        ("SELECT name, mission FROM $astronauts CROSS JOIN UNNEST (missions) AS mission WHERE LEFT(mission, 2) = 'Apollo 11'", 0, 0, None),
        # 1887
        ("SELECT * FROM (SELECT * FROM $satellites LEFT JOIN (SELECT id AS pid, mass FROM $planets) AS p ON $satellites.planetId = p.pid) AS mapped WHERE mass > 1", 170, 10, None),
        ("SELECT * FROM (SELECT planetId, mass FROM $satellites LEFT JOIN $planets AS p ON $satellites.planetId = p.id) AS mapped WHERE mass > 1", 170, 2, None),
        ("SELECT * FROM $satellites LEFT JOIN $planets AS p ON $satellites.planetId = p.id WHERE mass > 1", 170, 28, None),
        ("SELECT * FROM (SELECT p.id, mass FROM (SELECT * FROM $satellites) AS s LEFT JOIN $planets AS p ON s.planetId = p.id) AS mapped WHERE mass > 1", 170, 2, None),
        ("SELECT * FROM (SELECT * FROM $satellites) AS s LEFT JOIN (SELECT id as pid, mass FROM $planets) AS p ON s.planetId = p.pid WHERE mass > 1", 170, 10, None),
        ("SELECT * FROM $satellites LEFT JOIN (SELECT * FROM (SELECT * FROM $planets) AS p) AS planets ON $satellites.planetId = planets.id WHERE mass > 1", 170, 28, None),
        ("SELECT * FROM (SELECT * FROM (SELECT p.id, mass FROM $satellites LEFT JOIN $planets AS p ON $satellites.planetId = p.id) AS joined) AS mapped WHERE mass > 1", 170, 2, None),
        # 1977
        ("SELECT s, e FROM GENERATE_SERIES('2024-01-01', '2025-01-01', '1mth') AS s CROSS JOIN GENERATE_SERIES('2024-01-01', '2025-01-01', '1mth') AS e WHERE s = e + INTERVAL '1' MONTH", 12, 2, None),
        ("SELECT s, e FROM GENERATE_SERIES('2024-01-01', '2025-01-01', '1mth') AS s CROSS JOIN GENERATE_SERIES('2024-01-01', '2025-01-01', '1mth') AS e WHERE s + INTERVAL '1' MONTH = e", 12, 2, None),
        ("SELECT s, e FROM GENERATE_SERIES('2024-01-01', '2025-01-01', '1mth') AS s CROSS JOIN GENERATE_SERIES('2024-01-01', '2025-01-01', '1mth') AS e WHERE s = e - INTERVAL '1' MONTH", 12, 2, None),
        ("SELECT s, e FROM GENERATE_SERIES('2024-01-01', '2025-01-01', '1mth') AS s CROSS JOIN GENERATE_SERIES('2024-01-01', '2025-01-01', '1mth') AS e WHERE s - INTERVAL '1' MONTH = e", 12, 2, None),
        # 1981
        ("SELECT name FROM $planets WHERE VARCHAR(surface_pressure) = 'nan'", 0, 1, None),
        # 2002
        ("SELECT * FROM testdata.flat.hosts WHERE address | '54.0.0.0/8'", 27, 2, None),
        ("SELECT * FROM testdata.flat.hosts WHERE address | '20.0.0.0/9'", 76, 2, None),
        ("SELECT * FROM testdata.flat.hosts WHERE address | '20.1.0.0/9'", 0, 2, None),
        ("SELECT * FROM testdata.flat.hosts WHERE address | '20.112.0.0/16'", 26, 2, None),
        ("SELECT * FROM testdata.flat.hosts WHERE address | '127.0.0.0/24'", 1, 2, None),
        # 2019 
        ("SELECT name, mass, density, rotationPeriod, lengthOfDay, perihelion, aphelion, orbitalVelocity, orbitalEccentricity, obliquityToOrbit, surfacePressure, numberOfMoons FROM testdata.planets WHERE orbitalVelocity <> 2787170570 AND NOT orbitalVelocity BETWEEN 2191745.934 AND 402288.158", 9, 12, None),
        ("SELECT DISTINCT id, gm, density, magnitude FROM testdata.satellites WHERE radius < 1286258.869 AND NOT id > 2730526.873 AND id IS NULL ORDER BY radius DESC", 0, 4, None),
        ("SELECT Company, Price, Mission FROM testdata.missions WHERE Price <= 4279346967 AND NOT Price BETWEEN 137294968 AND 2336093823 ORDER BY Company DESC LIMIT 9 ", 9, 3, None),

        ("SELECT * FROM (SELECT * FROM (SELECT * FROM $satellites LEFT JOIN $planets AS p ON $satellites.planetId = p.id) AS joined) AS mapped WHERE mass > 1", 170, 28, AmbiguousIdentifierError),
        ("SELECT * FROM (SELECT * FROM $satellites LEFT JOIN (SELECT * FROM $planets) AS p ON $satellites.planetId = p.id) AS mapped WHERE mass > 1", 170, 28, AmbiguousIdentifierError),
        ("SELECT * FROM (SELECT * FROM $satellites LEFT JOIN $planets AS p ON $satellites.planetId = p.id) AS mapped WHERE mass > 1", 170, 28, AmbiguousIdentifierError),
        # 2042
        ("SELECT DISTINCT Company FROM launches", 62, 1, None),
        ("SELECT Company FROM launches", 4630, 1, None),
        ("SELECT * FROM launches", 4630, 3, None),
        ("SELECT DISTINCT Company FROM launches ORDER BY Company", 62, 1, None),
        ("SELECT DISTINCT Mission FROM launches", 4556, 1, None),
        # 2050
        ("SELECT RANDOM_STRING() FROM $planets", 9, 1, None),
        ("SELECT RANDOM_STRING(24) FROM $planets", 9, 1, None),
        # 2051
        ("SELECT CASE WHEN surfacePressure = 0 THEN -1 WHEN surfacePressure IS NULL THEN 0 ELSE -2 END FROM $planets", 9, 1, None),
        ("SELECT CASE WHEN surfacePressure = 0 THEN -1 ELSE -2 END FROM $planets", 9, 1, None),
        # 2054
        ("SELECT DISTINCT sides FROM (SELECT * FROM $planets AS plans LEFT JOIN (SELECT ARRAY_AGG(id) as sids, planetId FROM $satellites GROUP BY planetId) AS sats ON plans.id = planetId) AS plansats CROSS JOIN UNNEST (sids) as sides", 177, 1, None),
        ("SELECT DISTINCT sides FROM (SELECT * FROM $planets AS plans LEFT JOIN (SELECT ARRAY_AGG(name) as sids, planetId FROM $satellites GROUP BY planetId) AS sats ON plans.id = planetId) AS plansats CROSS JOIN UNNEST (sids) as sides", 177, 1, None),
        ("SELECT DISTINCT sides FROM (SELECT * FROM $planets AS plans LEFT JOIN (SELECT ARRAY_AGG(gm) as sids, planetId FROM $satellites GROUP BY planetId) AS sats ON plans.id = planetId) AS plansats CROSS JOIN UNNEST (sids) as sides", 102, 1, None),
        ("SELECT DISTINCT sides FROM (SELECT * FROM $planets AS plans LEFT JOIN (SELECT ARRAY_AGG(birth_date)  as sids, group FROM $astronauts GROUP BY group) AS sats ON plans.id = group) AS plansats CROSS JOIN UNNEST (sids) as sides", 125, 1, None),
        ("SELECT DISTINCT sides FROM (SELECT * FROM $planets AS plans LEFT JOIN (SELECT ARRAY_AGG(birth_place) as sids, group FROM $astronauts GROUP BY group) AS sats ON plans.id = group) AS plansats CROSS JOIN UNNEST (sids) as sides", 110, 1, None),
        # 2059
        ("SELECT g FROM generate_series(10) as g CROSS JOIN UNNEST (g) as g1", 0, 0, TypeError),
#        ("SELECT DISTINCT l FROM (SELECT split('a b c d e f g h i j', ' ') as letters) as plet CROSS JOIN UNNEST (letters) as l", 10, 1, None),
        # 2112
        ("SELECT id FROM $planets WHERE surface_pressure / surface_pressure is null", 5, 1, None),
        #2144
        ("SELECT town, LENGTH(NULLIF(town, 'Inglewood')) FROM (SELECT birth_place->'town' AS town FROM $astronauts) AS T", 357, 2, None),
        ("SELECT town, LENGTH(NULLIF(town, b'Inglewood')) FROM (SELECT birth_place->>'town' AS town FROM $astronauts) AS T", 357, 2, None),
        ("SELECT town, LENGTH(NULLIF(town, 'Inglewood')) FROM (SELECT birth_place->>'town' AS town FROM $astronauts) AS T", None, None, IncompatibleTypesError),
        # 2159
        ("SELECT * FROM (SELECT 1 * surface_pressure as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT surface_pressure * 1 as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT 0 * surface_pressure as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT surface_pressure * 0 as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT 0 + surface_pressure as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT surface_pressure + 0 as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT surface_pressure - 0 as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT surface_pressure / 1 as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT TRUE AND (surface_pressure = 0) as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT FALSE AND (surface_pressure = 0) as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 0, 2, None),
        ("SELECT * FROM (SELECT TRUE OR (surface_pressure = 0) as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 0, 2, None),
        ("SELECT * FROM (SELECT FALSE OR (surface_pressure = 0) as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT (surface_pressure = 0) AND TRUE as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT (surface_pressure = 0) AND FALSE as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 0, 2, None),
        ("SELECT * FROM (SELECT (surface_pressure = 0) OR TRUE as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 0, 2, None),
        ("SELECT * FROM (SELECT (surface_pressure = 0) OR FALSE as opt, surface_pressure FROM $planets) AS sub WHERE opt IS NULL", 4, 2, None),
        ("SELECT * FROM (SELECT name LIKE '%' as opt, name FROM $planets) AS sub WHERE opt IS TRUE", 9 , 2, None),
        ("SELECT * FROM $planets WHERE (surface_pressure * 1 IS NULL) OR (surface_pressure + 0 IS NULL)", 4, 20, None),
        ("SELECT * FROM $planets WHERE (surface_pressure / 1 IS NULL) AND (TRUE OR surface_pressure IS NULL)", 4, 20, None),
        ("SELECT * FROM $planets WHERE ((FALSE AND (surface_pressure * 1) != 0) IS NULL) OR (surface_pressure IS NULL)", 4, 20, None),
        ("SELECT * FROM $planets WHERE ((surface_pressure = 0) AND TRUE) IS NULL", 4, 20, None),
        ("SELECT * FROM $planets WHERE ((surface_pressure = 0) OR FALSE) IS NULL", 4, 20, None),
        ("SELECT COUNT(surface_pressure - 0) AS count_opt FROM $planets WHERE surface_pressure IS NULL", 1, 1, None),
        ("SELECT name || '' AS opt FROM $planets", 9, 1, None),
        ("SELECT name LIKE '%' AS opt FROM $planets", 9, 1, None),
        ("SELECT name LIKE '%a%' AS opt FROM $planets", 9, 1, None),
#        ("SELECT surface_pressure * 1 + surface_pressure * 0 AS opt FROM $planets", 4, 1, None),
        ("SELECT (TRUE AND (surface_pressure != 0)) OR FALSE AS opt FROM $planets", 9, 1, None),
        ("SELECT (surface_pressure / 1) * (surface_pressure - 0) AS opt FROM $planets", 9, 1, None),
        # 2180
        ("SELECT * FROM $planets ORDER BY (id)", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY (id) ASC", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY (id) DESC", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY name, (id)", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY name ASC, (id)", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY name DESC, (id)", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY (name), (id)", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY (name) ASC, (id) DESC", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY (name) DESC, (id)", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY (id), name", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY (id) ASC, name", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY (id) DESC, name", 9, 20, None),
        # 2340
        ("SELECT * FROM $satellites WHERE magnitude != 573602.533 ORDER BY magnitude DESC", 171, 8, None),
        ("SELECT * FROM iceberg.satellites WHERE magnitude != 573602.533 ORDER BY magnitude DESC", 171, 8, None),
        ("SELECT * FROM sqlite.satellites WHERE magnitude != 573602.533 ORDER BY magnitude DESC", 171, 8, None),
        ("SELECT * FROM $satellites WHERE magnitude < 573602.533 ORDER BY magnitude DESC", 171, 8, None),
        ("SELECT * FROM iceberg.satellites WHERE magnitude < 573602.533 ORDER BY magnitude DESC", 171, 8, None),
        ("SELECT * FROM sqlite.satellites WHERE magnitude < 573602.533 ORDER BY magnitude DESC", 171, 8, None),
]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery(statement:str, rows:int, columns:int, exception: Optional[Exception]):
    """
    Test an battery of statements
    """
    from tests.tools import set_up_iceberg
    from opteryx.connectors import IcebergConnector
    iceberg = set_up_iceberg()
    opteryx.register_store("iceberg", connector=IcebergConnector, catalog=iceberg)

    #    opteryx.register_store("tests", DiskConnector)
    #    opteryx.register_store("mabellabs", AwsS3Connector)
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
    # Running in the IDE we do some formatting - it's not functional but helps when reading the outputs.

    import shutil
    import time

    from tests.tools import trunc_printable

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 15
    passed:int = 0
    failed:int = 0
    nl:str = "\n"
    failures = []

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SHAPE TESTS")
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
            
            print(opteryx.query(statement))
            raise err

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
