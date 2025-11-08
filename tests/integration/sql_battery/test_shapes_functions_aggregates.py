"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
 >  Shape Checking
    Results Checking
    Compare to DuckDB

This file tests: Functions and aggregates

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
    UnsupportedSyntaxError,
    VariableNotFoundError,
)
from opteryx.managers.schemes.mabel_partitions import UnsupportedSegementationError
from opteryx.utils.formatter import format_sql
from opteryx.connectors import IcebergConnector

# fmt:off
# fmt:off
STATEMENTS = [
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

        # Function Filtering with Aggregate Functions
        ("SELECT MAX(density), COUNT(*) FROM $planets WHERE LENGTH(name) > 4 GROUP BY orbitalVelocity", 8, 2, None),
        # Edge Case with Temporal Filters Using Special Date Functions
        ("SELECT * FROM $planets FOR '2024-10-01' WHERE DATE(NOW()) > '2024-10-01'", 9, 20, None),
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
        ("SELECT name, missions FROM $astronauts WHERE missions LIKE ANY ('%Apoll%', 123)", 34, 2, IncompatibleTypesError),
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
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%Armstrong%', 123)", 1, 2, IncompatibleTypesError),
        ("SELECT name, missions FROM $astronauts WHERE name LIKE ANY ('%pattern1%', '%pattern2%', '%pattern3%', '%pattern4%', '%pattern5%', '%pattern6%', '%pattern7%', '%pattern8%', '%pattern9%', '%pattern10%', '%pattern11%', '%pattern12%', '%pattern13%', '%pattern14%', '%pattern15%', '%pattern16%', '%pattern17%', '%pattern18%', '%pattern19%', '%pattern20%', '%pattern21%', '%pattern22%', '%pattern23%', '%pattern24%', '%pattern25%', '%pattern26%', '%pattern27%', '%pattern28%', '%pattern29%', '%pattern30%', '%pattern31%', '%pattern32%', '%pattern33%', '%pattern34%', '%pattern35%', '%pattern36%', '%pattern37%', '%pattern38%', '%pattern39%', '%pattern40%', '%pattern41%', '%pattern42%', '%pattern43%', '%pattern44%', '%pattern45%', '%pattern46%', '%pattern47%', '%pattern48%', '%pattern49%', '%pattern50%');", 0, 2, None),

        ("SELECT name FROM $planets WHERE name LIKE '%e%' OR name LIKE '%i%'", 4, 1, None),
        ("SELECT name FROM $planets WHERE name ILIKE '%e%' OR name ILIKE '%i%'", 5, 1, None),
        ("SELECT name FROM $planets WHERE name LIKE '%e%' OR name LIKE '%i%' OR name LIKE '%a%'", 8, 1, None),
        ("SELECT name FROM $planets WHERE name LIKE '%e%' OR name ILIKE '%i%'", 4, 1, None),
        ("SELECT name FROM $planets WHERE name LIKE '%e%' OR diameter > 50000 OR name LIKE '%i%'", 6, 1, None),
        ("SELECT name FROM $planets WHERE (name LIKE '%e%' OR name ILIKE '%i%') AND mass > 1e-24", 4, 1, None),
        ("SELECT name FROM $planets WHERE (name LIKE '%u%' OR name LIKE '%a%') AND (mass > 1e-24 OR diameter < 30000)", 9, 1, None),
        ("SELECT name FROM $planets WHERE ((name LIKE '%m%' OR name LIKE '%v%') OR (name ILIKE '%u%' OR name ILIKE '%o%'))", 7, 1, None),
        ("SELECT name FROM $planets WHERE ((name LIKE '%m%' OR name ILIKE '%o%') OR (name ILIKE '%u%' OR name LIKE '%v%'))", 7, 1, None),
        ("SELECT name FROM $planets WHERE name LIKE '%m%' OR name ILIKE '%o%' OR name ILIKE '%u%' OR name LIKE '%v%'", 7, 1, None),
        ("SELECT name FROM $planets WHERE name LIKE '%m%' OR (name ILIKE '%o%' OR name ILIKE '%u%') OR name LIKE '%v%'", 7, 1, None),
        ("SELECT name FROM $planets WHERE ((name ILIKE '%m%' OR name ILIKE '%v%') AND (name LIKE '%u%' OR name LIKE '%o%'))", 2, 1, None),
        ("SELECT name FROM $planets WHERE name LIKE '%e%' OR name IN ('Earth', 'Mars', 'Jupiter')", 6, 1, None),
        ("SELECT name FROM $planets WHERE name LIKE '%r%' AND NOT name LIKE '%e%'", 4, 1, None),
        ("SELECT COUNT(*), name FROM $planets WHERE name LIKE '%a%' OR name ILIKE '%o%' GROUP BY name HAVING COUNT(*) > 0", 5, 2, None),

        ("SELECT max(current_timestamp), name FROM $satellites group by name", 177, 2, None),
        ("SELECT max(1), name FROM $satellites group by name", 177, 2, None),
        ("SELECT max(1) FROM $satellites", 1, 1, None),
        ("SELECT max('a'), name FROM $satellites group by name", 177, 2, None),
        ("SELECT max('a') FROM $satellites", 1, 1, None),
        ("SELECT min(current_timestamp), name FROM $satellites group by name", 177, 2, None),
        ("SELECT min(1), name FROM $satellites group by name", 177, 2, None),
        ("SELECT min(1) FROM $satellites", 1, 1, None),
        ("SELECT min('a'), name FROM $satellites group by name", 177, 2, None),
        ("SELECT min('a') FROM $satellites", 1, 1, None),
        ("SELECT count(current_timestamp), name FROM $satellites group by name", 177, 2, None),
        ("SELECT count(1), name FROM $satellites group by name", 177, 2, None),
        ("SELECT count(1) FROM $satellites", 1, 1, None),
        ("SELECT count('a'), name FROM $satellites group by name", 177, 2, None),
        ("SELECT count('a') FROM $satellites", 1, 1, None),
        ("SELECT avg(1), name FROM $satellites group by name", 177, 2, None),
        ("SELECT avg(1) FROM $satellites", 1, 1, None),
        ("SELECT surface_pressure FROM $planets WHERE IFNOTNULL(surface_pressure, 0.0) == 0.0", 5, 1, None),
        ("SELECT username FROM testdata.flat.ten_files WHERE SQRT(followers) = 10 ORDER BY followers DESC LIMIT 10", 1, 1, None),
        ("SELECT username FROM testdata.flat.ten_files WHERE SQRT(followers) = 15 ORDER BY followers DESC LIMIT 10", 0, 1, None),
        ("SELECT Company, Rocket, MIN(Price), MAX(Price) FROM $missions GROUP BY ALL", 429, 4, None),
        
        ("SELECT HUMANIZE(1000)", 1, 1, None),
        ("SELECT HUMANIZE(COUNT(*)) FROM $planets", 1, 1, None),
        ("SELECT HUMANIZE(gravity) FROM $planets", 9, 1, None),
        ("SELECT * FROM $satellites WHERE id > 1_00", 77, 8, None),
        ("SELECT * FROM $satellites WHERE radius > 10.0_0", 77, 8, None),
        ("SELECT * FROM $satellites WHERE radius > 10.0_0 and id > 1_00", 42, 8, None),

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

        ("SELECT DISTINCT surfacePressure FROM $planets", 6, 1, None), # DOUBLE (with nulls)
        ("SELECT DISTINCT mass FROM $planets", 9, 1, None), # DOUBLE
        ("SELECT DISTINCT name FROM $planets", 9, 1, None), # VARCHAR
        ("SELECT DISTINCT id FROM $planets", 9, 1, None), # INTEGER
        ("SELECT DISTINCT gravity FROM $planets", 8, 1, None), # DECIMAL
        ("SELECT DISTINCT alma_mater AS AM FROM $astronauts", 281, 1, None), # LIST<VARCHAR>
        ("SELECT DISTINCT birth_place FROM $astronauts", 272, 1, None), # JSONB/STRUCT
        ("SELECT DISTINCT death_date AS AM FROM $astronauts", 39, 1, None), # TIMESTAMP
        ("SELECT DISTINCT id > 1 AS AM FROM $planets", 2, 1, None), # BOOLEAN
        ("SELECT DISTINCT surfacePressure > 1 AS AM FROM $planets", 3, 1, None), # BOOLEAN (with nulls)

        ("SELECT CAST(p.name AS ARRAY<VARCHAR>) FROM $satellites as s LEFT JOIN $planets as p ON s.id = p.id WHERE s.id > 10", 167, 1, None),
        ("SELECT CAST(p.id AS ARRAY<VARCHAR>) FROM $satellites as s LEFT JOIN $planets as p ON s.id = p.id WHERE s.id > 10", 167, 1, None),
        ("SELECT CAST(p.mass AS ARRAY<VARCHAR>) FROM $satellites as s LEFT JOIN $planets as p ON s.id = p.id WHERE s.id > 10", 167, 1, None),
        ("SELECT CAST(p.name AS ARRAY<INTEGER>) FROM $satellites as s LEFT JOIN $planets as p ON s.id = p.id WHERE s.id > 10", 167, 1, None),
        ("SELECT CAST(p.id AS ARRAY<INTEGER>) FROM $satellites as s LEFT JOIN $planets as p ON s.id = p.id WHERE s.id > 10", 167, 1, None),
        ("SELECT CAST(p.mass AS ARRAY<INTEGER>) FROM $satellites as s LEFT JOIN $planets as p ON s.id = p.id WHERE s.id > 10", 167, 1, None),

        ("SELECT * FROM testdata.flat.struct_array WHERE data[0]->'id' = 1", 1, 2, None),
        
        ("SELECT * FROM $planets WHERE REPLACE(name, 'e', 'a') = 'Vanus'", 1, 20, None),
        ("SELECT * FROM $planets WHERE INITCAP(REVERSE(name)) = 'Htrae'", 1, 20, None),

        # ****************************************************************************************

        # These are queries which have been found to return the wrong result or not run correctly
        # FILTERING ON FUNCTIONS
        ("SELECT DATE(birth_date) FROM $astronauts FOR TODAY WHERE DATE(birth_date) < '1930-01-01'", 14, 1, None),
        # FUNCTION (AGG)
        ("SELECT CONCAT(ARRAY_AGG(name)) FROM $planets GROUP BY gravity", 8, 1, None),
        # AGG (FUNCTION)
        ("SELECT SUM(IIF(year < 1970, 1, 0)), MAX(year) FROM $astronauts", 1, 2, None),
        # [#527] variables referenced in subqueries
        ("SET @v = 1; SELECT * FROM (SELECT @v) AS S;", 1, 1, None),
        # [#561] HASH JOIN with an empty table
        ("SELECT * FROM $planets LEFT JOIN (SELECT planetId as id FROM $satellites WHERE id < 0) USING (id)", 9, 20, None),
        ("SELECT * FROM $planets LEFT JOIN (SELECT planetId as id FROM $satellites WHERE id < 0) AS S USING (id)", 9, 20, None),
        # [#646] Incorrectly placed temporal clauses
        ("SELECT * FROM $planets WHERE 1 = 1 FOR TODAY;", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets GROUP BY name FOR TODAY;", 9, 1, InvalidTemporalRangeFilterError),
        # [#518] SELECT * and GROUP BY can't be used together
        ("SELECT * FROM $planets GROUP BY name", 9, 1, UnsupportedSyntaxError),
        # found testing
        ("SELECT user_name FROM testdata.flat.formats.arrow WITH(NO_PARTITION) WHERE user_name = 'Niran'", 1, 1, None),
        # 769
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
        # COUNT(*) in non aggregated joins
        ("SELECT COUNT(*), COUNT_DISTINCT(id) FROM $planets;", 1, 2, None),
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

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} FUNCTIONS_AGGREGATES SHAPE TESTS")
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
