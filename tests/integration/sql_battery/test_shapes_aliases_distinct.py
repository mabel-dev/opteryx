"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
 >  Shape Checking
    Results Checking
    Compare to DuckDB

This file tests: Aliases and DISTINCT operations

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

        ("SELECT missions FROM $astronauts WHERE ARRAY_CONTAINS(missions, 'Apollo 8')", 3, 1, None),
        ("SELECT missions FROM $astronauts WHERE ARRAY_CONTAINS_ANY(missions, ('Apollo 8', 'Apollo 13'))", 5, 1, None),
        ("SELECT missions FROM $astronauts WHERE ARRAY_CONTAINS_ALL(missions, ('Apollo 8', 'Gemini 7'))", 2, 1, None),
        ("SELECT missions FROM $astronauts WHERE ARRAY_CONTAINS_ALL(missions, ('Gemini 7', 'Apollo 8'))", 2, 1, None),
        ("SELECT missions FROM $astronauts WHERE missions @> ('Apollo 8', 'Apollo 13')", 5, 1, None),
        ("SELECT * FROM $astronauts WHERE missions @>> ('Apollo 11', 'Gemini 12')", 1, 19, None),
        ("SELECT * FROM $astronauts WHERE missions @>> ('Gemini 7', 'Apollo 8')", 2, 19, None),

        ("SELECT * FROM $astronauts WHERE 'Apollo 11' = any(missions)", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE 'C' > any(alma_mater)", 19, 19, None),
        ("SELECT * FROM $astronauts WHERE 'X' < any(alma_mater)", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' != any(missions)", 334, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' != all(missions)", 331, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' = all(missions)", 0, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' = any(missions) AND True", 3, 19, None),
        ("SELECT * FROM $astronauts WHERE 'X' > any(alma_mater) OR 'Z' > any(alma_mater)", 357, 19, None),
        ("SELECT * FROM $astronauts WHERE name != 'Brian' AND 'B' < any(alma_mater)", 353, 19, None),
        ("SELECT * FROM $astronauts WHERE name != 'Brian' OR 'Apollo 11' != any(missions)", 357, 19, None),
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' != all(missions) AND name != 'Brian'", 331, 19, None),
        ("SELECT * FROM $astronauts WHERE name != 'Brian' AND 'Apollo 11' = all(missions)", 0, 19, None),
        ("SELECT * FROM $astronauts WHERE 'X' >= any(alma_mater)", 357, 19, None),
        ("SELECT * FROM $astronauts WHERE 'B' <= any(alma_mater)", 353, 19, None),

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
        ("SET disable_optimizer = true; EXPLAIN SELECT * FROM $satellites WHERE id = 8 AND id = 7", 2, 3, None),
        ("SET disable_optimizer = false; EXPLAIN SELECT * FROM $satellites WHERE id = 8 AND id = 7", 2, 3, None),
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
        ("SELECT $planets.*, $satellites.planetId FROM $planets INNER JOIN $satellites USING (id)", 9, 21, None),
        ("SELECT $planets.*, $satellites.planetId FROM $planets INNER JOIN $satellites USING (id) ORDER BY $planets.id", 9, 21, None),
        ("SELECT $planets.*, $satellites.planetId FROM $planets INNER JOIN $satellites USING (id) WHERE $planets.name LIKE '%r%'", 6, 21, None),
        ("SELECT $planets.id, $satellites.* FROM $planets INNER JOIN $satellites USING (id)", 6, 21, SqlError),
        ("SELECT $planets.*, $satellites.planetId FROM $planets INNER JOIN $satellites USING (id) WHERE $satellites.planetId = 4", 2, 21, None),

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

        ("SELECT * FROM $missions WHERE COSINE_SIMILARITY(Location, 'LC-18A, Cape Canaveral AFS, Florida, USA') > 0.7", 666, 8, None),

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
        ("SELECT ABS(mass) FROM $planets", 9, 1, None),
        ("SELECT SIGN(mass) FROM $planets", 9, 1, None),
        ("SELECT reverse(name) From $planets", 9, 1, None),
        ("SELECT title(reverse(name)) From $planets", 9, 1, None),
        ("SELECT SOUNDEX(name) From $planets", 9, 1, None),

        ("SELECT APPROXIMATE_MEDIAN(radius) AS AM FROM $satellites GROUP BY planetId HAVING APPROXIMATE_MEDIAN(radius) > 5;", 5, 1, None),
        ("SELECT APPROXIMATE_MEDIAN(radius) AS AM FROM $satellites GROUP BY planetId HAVING AM > 5;", 5, 1, None),
        ("SELECT COUNT(planetId) FROM $satellites", 1, 1, None),
        ("SELECT COUNT_DISTINCT(planetId) FROM $satellites", 1, 1, None),
        ("SELECT ARRAY_AGG(name), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT ONE(name), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT ANY_VALUE(name), planetId FROM $satellites GROUP BY planetId", 7, 2, None),
        ("SELECT MAX(planetId) FROM $satellites", 1, 1, None),
        ("SELECT AVG(planetId) FROM $satellites", 1, 1, None),
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
        ("SELECT CONCAT(ARRAY_AGG(name)) FROM $planets GROUP BY gravity", 8, 1, None),
        ("SELECT CONCAT(missions) FROM $astronauts", 357, 1, None),
        ("SELECT CONCAT(('1', '2', '3'))", 1, 1, None),
        ("SELECT CONCAT(('1', '2', '3')) FROM $planets", 9, 1, None),
        ("SELECT CONCAT_WS(', ', ARRAY_AGG(name)) FROM $planets GROUP BY gravity", 8, 1, None),
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
        ("SELECT CONCAT_WS(', ', ARRAY_AGG(mass)) as MASSES FROM $planets GROUP BY gravity", 8, 1, None),
        ("SELECT GREATEST(ARRAY_AGG(name)) as NAMES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT GREATEST(ARRAY_AGG(gm)) as MASSES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT LEAST(ARRAY_AGG(name)) as NAMES FROM $satellites GROUP BY planetId", 7, 1, None),
        ("SELECT LEAST(ARRAY_AGG(gm)) as MASSES FROM $satellites GROUP BY planetId", 7, 1, None),
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
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' != ANY(missions)", 334, 19, None),
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
        ("SELECT SPLIT(name, ' ')[0] AS names FROM $astronauts", 357, 1, None),
        ("SELECT SPLIT(name, ' ')[-1] AS names FROM $astronauts", 357, 1, None),

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
        ("SELECT name FROM $astronauts WHERE GET(VARCHAR(birth_place), 'state') = birth_place['state']", 357, 1, None),

        ("SELECT * FROM $missions WHERE MATCH (Location) AGAINST ('Florida USA')", 911, 8, None),
        ("SELECT * FROM $missions WHERE MATCH (Location) AGAINST ('Russia, Kapustin')", 112, 8, None),

        ("SELECT * FROM testdata.partitioned.hourly FOR '2024-01-01 01:00'", 1, 2, None),
        ("SELECT * FROM testdata.partitioned.hourly FOR '2024-01-01'", 2, 2, None),
        ("SELECT * EXCEPT (id, name, gravity) FROM $planets", 9, 17, None),

        # bespoke casters
        ("SELECT * FROM $satellites WHERE CAST(CAST(id AS VARCHAR) AS INTEGER) == id", 177, 8, None),
        ("SELECT * FROM $satellites WHERE CAST(CAST(id AS BLOB) AS INTEGER) == id", 177, 8, None),
        ("SELECT * FROM $satellites WHERE CAST(CAST(id * -1 AS VARCHAR) AS INTEGER) == id * -1", 177, 8, None),
        ("SELECT * FROM $satellites WHERE CAST(CAST(id * -1 AS BLOB) AS INTEGER) == id * -1", 177, 8, None),
        ("SELECT * FROM $satellites WHERE CAST(CAST(gm AS VARCHAR) AS DOUBLE) == gm", 177, 8, None),
        ("SELECT * FROM $satellites WHERE CAST(CAST(gm AS BLOB) AS DOUBLE) == gm", 177, 8, None),

        # 10-way join
        ("SELECT p1.name AS planet1_name, p2.name AS planet2_name, p3.name AS planet3_name, p4.name AS planet4_name, p5.name AS planet5_name, p6.name AS planet6_name, p7.name AS planet7_name, p8.name AS planet8_name, p9.name AS planet9_name, p10.name AS planet10_name, p1.diameter AS planet1_diameter, p2.gravity AS planet2_gravity, p3.orbitalPeriod AS planet3_orbitalPeriod, p4.numberOfMoons AS planet4_numberOfMoons, p5.meanTemperature AS planet5_meanTemperature FROM $planets p1 JOIN $planets p2 ON p1.id = p2.id JOIN $planets p3 ON p1.id = p3.id JOIN $planets p4 ON p1.id = p4.id JOIN $planets p5 ON p1.id = p5.id JOIN $planets p6 ON p1.id = p6.id JOIN $planets p7 ON p1.id = p7.id JOIN $planets p8 ON p1.id = p8.id JOIN $planets p9 ON p1.id = p9.id JOIN $planets p10 ON p1.id = p10.id WHERE p1.diameter > 10000 ORDER BY p1.name, p2.name, p3.name, p4.name, p5.name;", 6, 15, None),

        ("SELECT mission, ARRAY_AGG(name) FROM $missions INNER JOIN (SELECT * FROM $astronauts CROSS JOIN UNNEST(missions) AS mission) AS astronauts ON Mission = mission GROUP BY mission", 16, 2, None),
        ("SELECT alma_matered FROM (SELECT alma_mater FROM $astronauts CROSS JOIN $satellites) AS bulked CROSS JOIN UNNEST(alma_mater) AS alma_matered", 120537, 1, None),

        # virtual dataset doesn't exist
        ("SELECT * FROM $RomanGods", None, None, DatasetNotFoundError),
        # disk dataset doesn't exist
        ("SELECT * FROM non.existent", None, None, DatasetNotFoundError),
        # column doesn't exist
        ("SELECT awesomeness_factor FROM $planets;", None, None, ColumnNotFoundError),
        ("SELECT * FROM $planets WHERE awesomeness_factor > 'Mega';", None, None, ColumnNotFoundError),
        # https://trino.io/docs/current/functions/aggregate.html#filtering-during-aggregation
        ("SELECT ARRAY_AGG(name) FILTER (WHERE name IS NOT NULL) FROM $planets;", None, None, UnsupportedSyntaxError),
        # Can't IN an INDENTIFIER
        ("SELECT * FROM $astronauts WHERE 'Apollo 11' IN missions", None, None, SqlError),
        # Distinct Filtering with Aggregate Functions
        ("SELECT DISTINCT MAX(density) FROM $planets WHERE orbitalInclination > 1 GROUP BY numberOfMoons", 6, 1, None),
        # Multi-Level Subquery with Different Alias Names
        ("SELECT * FROM (SELECT * FROM (SELECT * FROM $planets WHERE id > 5) AS SQ1) AS SQ2 WHERE id < 10", 4, 20, None),
        # Ordering on Computed Columns with Aliases
        ("SELECT name, LENGTH(name) AS len FROM $planets ORDER BY len DESC", 9, 2, None),
        # DISTINCT on null values [#285]
        ("SELECT DISTINCT name FROM (VALUES (null),(null),('apple')) AS booleans (name)", 2, 1, None),
        # empty aggregates with other columns, loose the other columns [#281]
        ("SELECT name, COUNT(*) FROM $astronauts WHERE name = 'Jim' GROUP BY name", 0, 2, None),
        # ALIAS issues [#408]
        ("SELECT $planets.* FROM $planets INNER JOIN (SELECT id FROM $planets AS IP) AS b USING (id)", 9, 20, None),
        # DOUBLE QUOTED STRING [#399]
        ("SELECT birth_place['town'] FROM $astronauts WHERE birth_place['town'] = \"Rome\"", 1, 1, None),
        # COUNT incorrect
        ("SELECT * FROM (SELECT COUNT(*) AS bodies FROM $planets) AS space WHERE space.bodies > 5", 1, 1, None),
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

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} ALIASES_DISTINCT SHAPE TESTS")
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
