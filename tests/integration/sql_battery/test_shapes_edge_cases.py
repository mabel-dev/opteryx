"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
 >  Shape Checking
    Results Checking
    Compare to DuckDB

This file tests: Edge cases, errors, and regressions

This tests that the shape of the response is as expected: the right number of columns,
the right number of rows and, if appropriate, the right exception is thrown.
"""
import pytest
import os
import sys

#import opteryx

from typing import Optional

sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
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
        # Invalid temporal ranges
        ("SELECT * FROM $planets FOR 2022-01-01", 9, 20, None),
        ("SELECT * FROM $planets FOR DATES IN 2022", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES BETWEEN 2022-01-01 AND TODAY", 9, 20, None),
        ("SELECT * FROM $planets FOR DATES BETWEEN today AND yesterday", None, None, InvalidTemporalRangeFilterError),
        ("SELECT * FROM $planets FOR DATES IN '2022-01-01' AND '2022-01-02'", None, None, InvalidTemporalRangeFilterError),
        # MONTH has a bug
        ("SELECT DATEDIFF('months', birth_date, '2022-07-07') FROM $astronauts", None, None, FunctionExecutionError),
        ("SELECT DATEDIFF('months', birth_date, '2022-07-07') FROM $astronauts", None, None, FunctionExecutionError),
        ("SELECT DATEDIFF(MONTH, birth_date, '2022-07-07') FROM $astronauts", None, None, ColumnNotFoundError),
        ("SELECT DATEDIFF(MONTHS, birth_date, '2022-07-07') FROM $astronauts", None, None, ColumnNotFoundError),
        # TEMPORAL QUERIES aren't part of the AST
        ("SELECT * FROM CUSTOMERS FOR SYSTEM_TIME ('2022-01-01', '2022-12-31')", None, None, InvalidTemporalRangeFilterError),
        # can't cast to a list
        ("SELECT CAST('abc' AS LIST)", None, None, SqlError),
        ("SELECT TRY_CAST('abc' AS LIST)", None, None, SqlError),

        ("SELECT dict FROM testdata.flat.struct", 6, 1, None),

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
        ("SELECT * FROM $planets AS P ANTI JOIN $satellites AS S ON S.id = P.id;", 0, 20, None),
        ("SELECT * FROM $planets AS P RIGHT ANTI JOIN $satellites AS S ON S.id = P.id;", 168, 8, UnsupportedSyntaxError),
        ("SELECT * FROM $planets AS P LEFT SEMI JOIN $satellites AS S ON S.id = P.id;", 9, 20, None),
        ("SELECT * FROM $planets AS P SEMI JOIN $satellites AS S ON S.id = P.id;", 9, 20, None),
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
        ("SELECT * FROM $planets AS P SEMI JOIN $satellites AS S ON P.id = S.id AND P.name = 'Earth';", 1, 20, None),
        ("SELECT * FROM $planets AS P SEMI JOIN $satellites AS S ON P.name = S.name;", 0, 20, None),
        ("SELECT * FROM $planets AS P SEMI JOIN $satellites AS S ON P.id = S.id WHERE P.name IS NOT NULL;", 9, 20, None),
        ("SELECT * FROM $planets SEMI JOIN $satellites USING(id) WHERE id IS NULL;", 0, 20, None),
        ("SELECT * FROM $planets AS P SEMI JOIN (SELECT DISTINCT id FROM $satellites) AS S ON P.id = S.id;", 9, 20, None),
        ("SELECT * FROM $satellites AS S SEMI JOIN $planets AS P ON S.id = P.id;", 9, 8, None),
        ("SELECT * FROM $planets AS P SEMI JOIN (SELECT id FROM $satellites WHERE FALSE) AS S ON P.id = S.id;", 0, 20, None),
        ("SELECT * FROM $satellites AS P ANTI JOIN $planets AS S ON P.id = S.id;", 168, 8, None),
        ("SELECT * FROM $planets AS P ANTI JOIN $satellites AS S ON P.name = S.name;", 9, 20, None),
        ("SELECT * FROM $planets ANTI JOIN $satellites USING(id);", 0, 20, None),
        ("SELECT * FROM $satellites ANTI JOIN $planets USING(id);", 168, 8, None),
        ("SELECT * FROM $planets AS P ANTI JOIN (SELECT DISTINCT id FROM $satellites) AS S ON P.id = S.id;", 0, 20, None),
        ("SELECT * FROM (SELECT 42 AS id) AS X SEMI JOIN $satellites AS S USING(id);", 1, 1, None),
        ("SELECT * FROM (SELECT 42 AS id) AS X ANTI JOIN $planets AS S USING(id);", 1, 1, None),
        ("SELECT * FROM (SELECT 42 AS id) AS X ANTI JOIN $satellites AS S USING(id);", 0, 1, None),
        ("SELECT * FROM (SELECT 42 AS id) AS X SEMI JOIN $planets AS S USING(id);", 0, 1, None),
        ("SELECT * FROM $planets AS P ANTI JOIN (SELECT id FROM $satellites WHERE FALSE) AS S ON P.id = S.id;", 9, 20, None),
        ("SELECT * FROM $planets ANTI JOIN $satellites USING(id) WHERE name IS NOT NULL;", 0, 20, None),
        ("SELECT * FROM $planets AS P ANTI JOIN $satellites AS S ON P.id = S.id WHERE P.name != 'Earth';", 0, 20, None),

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

        # Handling NULL Comparisons in WHERE Clause
        ("SELECT * FROM $planets WHERE id IS NOT NULL AND id < NULL", 0, 20, None),
        # Test for Zero-Length String Comparison
        ("SELECT * FROM $satellites WHERE name = ''", 0, 8, None),
        # Edge Case with LIKE and NULL Handling
        ("SELECT * FROM $planets WHERE name NOT LIKE '%a%' OR name IS NULL", 5, 20, None),
        # Ordering with NULLs First and Last
        ("SELECT * FROM $planets ORDER BY lengthOfDay NULLS LAST", 9, 20, None),
        # Edge Case Testing Subscripts on Arrays with NULL Values
        ("SELECT name[0] FROM $planets WHERE id IS NULL", 0, 1, None),
        # Edge Case with JSON-Like Filtering
        ("SELECT * FROM $astronauts WHERE birth_place->>'city' = 'New York'", 0, 19, None),
        # ZERO RECORD RESULT SETS
        ("SELECT * FROM $planets WHERE id = -1 ORDER BY id", 0, 20, None),
        ("SELECT * FROM $planets WHERE id = -1 LIMIT 10", 0, 20, None),
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
        ("SELECT P0.id, P1.ID, P2.ID FROM $planets AS P0 JOIN (SELECT CONCAT_WS(' ', ARRAY_AGG(id)) AS ID, MAX(name) AS n FROM $planets AS Q1 GROUP BY gravity) AS P1 ON P0.name = P1.n JOIN (SELECT CONCAT_WS(' ', ARRAY_AGG(id)) AS ID, MAX(name) AS n FROM $planets AS Q2 GROUP BY gravity) AS P2 ON P0.name = P2.n", 8, 3, None),
        # no issue number - but these two caused a headache
        # Null columns can't be inverted
        ("SELECT NOT NULL", 1, 1, None),
        # Columns in CAST statements appear to not be bound correctly
        ("SELECT SUM(CASE WHEN gm > 10 THEN 1 ELSE 0 END) AS gm_big_count FROM $satellites", 1, 1, None),
        # NUMPY typles not handled by sqlalchemy
        ("SELECT P_1.* FROM sqlite.planets AS P_1 CROSS JOIN $satellites AS P_2 WHERE P_1.id = P_2.planetId AND P_1.name LIKE '%a%' AND lengthOfDay > 0", 91, 20, None),
        # 1370, issues coercing DATE and TIMESTAMPS
        ("SELECT * FROM $planets WHERE TIMESTAMP '2023-01-01' = DATE '2023-01-01'", 9, 20, None),
        ("SELECT * FROM $planets WHERE 1 = 1.0", 9, 20, None),
        ("SELECT * FROM $planets WHERE DATE '2023-01-01' + INTERVAL '1' MONTH is not null", 9, 20, None),
        ("SELECT * FROM $planets WHERE TIMESTAMP '2023-01-01' + INTERVAL '1' MONTH is not null", 9, 20, None),
        ("SELECT DATE '2023-01-01' + INTERVAL '1' MONTH FROM $planets", 9, 1, None),
        ("SELECT TIMESTAMP '2023-01-01' + INTERVAL '1' MONTH FROM $planets", 9, 1, None),
        ("SELECT * FROM $planets WHERE DATE '2023-01-01' + INTERVAL '1' MONTH < current_timestamp", 9, 20, None),
        ("SELECT * FROM $planets WHERE TIMESTAMP '2023-01-01' + INTERVAL '1' MONTH < current_timestamp", 9, 20, None),
        # 1380
        ("SELECT * FROM $planets INNER JOIN (SELECT * FROM UNNEST((1, 2, 3)) AS id) AS PID USING(id)", 3, 20, None),
        ("SELECT * FROM $planets INNER JOIN (SELECT * FROM UNNEST((1, 2, 3)) AS id) AS S USING(id)", 3, 20, None),
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
        ("SELECT g FROM generate_series(10) as g CROSS JOIN UNNEST (g) as g1", 0, 0, IncorrectTypeError),
#        ("SELECT DISTINCT l FROM (SELECT split('a b c d e f g h i j', ' ') as letters) as plet CROSS JOIN UNNEST (letters) as l", 10, 1, None),
        # 2112
        ("SELECT id FROM $planets WHERE surface_pressure / surface_pressure is null", 5, 1, None),
        # 2144
        ("SELECT town, LENGTH(NULLIF(town, 'Inglewood')) FROM (SELECT birth_place->'town' AS town FROM $astronauts) AS T", 357, 2, None),
        ("SELECT town, LENGTH(NULLIF(town, b'Inglewood')) FROM (SELECT birth_place->>'town' AS town FROM $astronauts) AS T", 357, 2, None),
        ("SELECT town, LENGTH(NULLIF(town, 'Inglewood')) FROM (SELECT birth_place->>'town' AS town FROM $astronauts) AS T", 357, 2, None),
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
        ("SELECT * FROM iceberg.opteryx.satellites WHERE magnitude != 573602.533 ORDER BY magnitude DESC", 171, 8, None),
        ("SELECT * FROM sqlite.satellites WHERE magnitude != 573602.533 ORDER BY magnitude DESC", 171, 8, None),
        ("SELECT * FROM $satellites WHERE magnitude < 573602.533 ORDER BY magnitude DESC", 171, 8, None),
        ("SELECT * FROM iceberg.opteryx.satellites WHERE magnitude < 573602.533 ORDER BY magnitude DESC", 171, 8, None),
        ("SELECT * FROM sqlite.satellites WHERE magnitude < 573602.533 ORDER BY magnitude DESC", 171, 8, None),
        # 2489
        ("SELECT name FROM $planets where length(md5(name)) == 32", 9, 1, None),
        ("SELECT name FROM $planets WHERE case when name is null then '' else name end == 'Earth'", 1, 1, None),
        # 2514
        ("SELECT * FROM (SELECT '{\"name\": \"John\"}' AS details) AS t WHERE IFNULL(details->'name', '') == 'John'", 1, 1, None),
        # 2523
        ("SELECT * FROM (SELECT name, id FROM $planets AS A UNION SELECT name, id FROM $planets AS B) AS C WHERE name = 'Earth'", 1, 2, None),
        ("SELECT * FROM (SELECT name, id FROM $planets AS A UNION ALL SELECT name, id FROM $planets AS B) AS C WHERE name = 'Earth'", 2, 2, None),
        # 2547
        ("SELECT * FROM $planets WHERE name ILIKE '%art%h%'", 1, 20, None),
        # 2483
        ("SELECT * FROM $planets LEFT JOIN testdata.flat.null_lists", None, None, UnsupportedSyntaxError),
        # 2588
        ("SELECT CAST(M AS INTEGER) FROM (SELECT CAST(mass AS VARCHAR) AS M FROM $planets) AS A", None, None, FunctionExecutionError),
        # 2592
        ("SELECT AVG(mass), COUNT(*), name FROM (SELECT name, mass FROM $planets WHERE mass < 0 GROUP BY name, mass) AS A group by name", 0, 3, None),
        # 2614
        ("SELECT name FROM $planets WHERE IFNULL(surface_pressure, 0) == 0", 5, 1, None),
        ("SELECT name FROM $planets WHERE IFNULL(surface_pressure, 1) == 1", 5, 1, None),
        ("SELECT name FROM $planets WHERE IFNULL(surface_pressure, 2) == 2", 4, 1, None),
        ("SELECT IFNULL(missions, []) FROM $astronauts", 357, 1, None),
        ("SELECT IFNULL(missions, ['Training']) FROM $astronauts", 357, 1, None),
        ("SELECT IFNULL(missions, []) FROM $astronauts WHERE missions IS NULL", 23, 1, None),
        # 2649
        ("SELECT name FROM $planets WHERE UNIXTIME('2020-01-01T00:00:00') = 1577836800;", 9, 1, None),
        ("SELECT name FROM $planets WHERE UNIXTIME('2020-01-01T00:00:00'::TIMESTAMP) = 1577836800;", 9, 1, None),
        ("SELECT name FROM (SELECT name, UNIXTIME('1970-01-01'::DATE) AS ts FROM $planets) AS A WHERE ts = 0;", 9, 1, None),
        ("SELECT name FROM $astronauts WHERE UNIXTIME(birth_date) = UNIXTIME('1961-11-05'::DATE);", 2, 1, None),
        ("SELECT name FROM $planets WHERE '2020-01-01T00:00:00'::TIMESTAMP = FROM_UNIXTIME(1577836800);", 9, 1, None),
        # 2754
        ("SELECT name FROM $astronauts WHERE CONCAT(missions) ILIKE '%Apo%'", 34, 1, None),
        ("SELECT name FROM $astronauts WHERE CONCAT(missions) LIKE '%Apo%'", 34, 1, None),
        # 2781
        ("SELECT id, (COUNT(*) + COUNT(*)) AS c FROM $planets GROUP BY id", 9, 2, None),
        ("SELECT id, (COUNT(*) + COUNT(*) + COUNT(*)) AS c FROM $planets GROUP BY id", 9, 2, None),
        ("SELECT id, (COUNT(*) * 2) + COUNT(*) AS c FROM $planets GROUP BY id", 9, 2, None),
        ("SELECT id, COUNT(*) AS c, c + c AS doubled FROM $planets GROUP BY id", 9, 3, None),
        ("SELECT id, COUNT(*) / LN(COUNT(*)) AS c FROM $planets GROUP BY id", 9, 2, None),
        ("SELECT (COUNT(*) + COUNT(*)) AS c FROM $planets", 1, 1, None),
        ("SELECT (COUNT(*) + COUNT(*) + COUNT(*)) AS c FROM $planets", 1, 1, None),
        ("SELECT (COUNT(*) * 2) + COUNT(*) AS c FROM $planets", 1, 1, None),
        ("SELECT COUNT(*) / LN(COUNT(*)) AS c FROM $planets", 1, 1, None),
        # 2786
        ("SELECT * EXCEPT(id) FROM $planets ORDER BY name", 9, 19, None),
        ("SELECT * EXCEPT(id, density) FROM $planets ORDER BY name", 9, 18, None),
        ("SELECT * EXCEPT(name) FROM $planets ORDER BY name", 9, 19, UnsupportedSyntaxError),
        ("SELECT * EXCEPT(id, name) FROM $planets ORDER BY name", 9, 19, UnsupportedSyntaxError),
        # 2818
        ("SELECT COUNT_DISTINCT(perihelion) FROM testdata.planets WHERE diameter >= 378092", 1, 1, None),
        ("SELECT COUNT_DISTINCT(perihelion) FROM testdata.planets WHERE diameter >= 378092 GROUP BY name", 0, 1, None),

        # Additional edge cases - UNION/EXCEPT/INTERSECT
        ("SELECT id FROM $planets WHERE id < 3 UNION SELECT id FROM $planets WHERE id > 7", 4, 1, AmbiguousDatasetError),
        ("SELECT id FROM $planets WHERE id < 5 UNION ALL SELECT id FROM $planets WHERE id < 3", 6, 1, AmbiguousDatasetError),
        ("SELECT id FROM $planets EXCEPT SELECT id FROM $satellites WHERE id < 5", 5, 1, UnsupportedSyntaxError),
        ("SELECT id FROM $planets INTERSECT SELECT id FROM $satellites", 9, 1, UnsupportedSyntaxError),

        # Complex nested subqueries
        ("SELECT * FROM (SELECT * FROM (SELECT * FROM $planets) AS s1) AS s2", 9, 20, None),
        ("SELECT COUNT(*) FROM (SELECT id FROM $planets WHERE id IN (SELECT planetId FROM $satellites)) AS subq", 1, 1, UnsupportedSyntaxError),

        # CROSS JOIN edge cases
        ("SELECT COUNT(*) FROM $planets CROSS JOIN $no_table", 1, 1, None),
        ("SELECT p.id FROM $planets p CROSS JOIN (SELECT 1 AS one) AS t", 9, 1, None),

        # Edge cases with HAVING
        ("SELECT planetId, COUNT(*) FROM $satellites GROUP BY planetId HAVING COUNT(*) > 1", 6, 2, None),
        ("SELECT planetId FROM $satellites GROUP BY planetId HAVING COUNT(*) = 1", 1, 1, ColumnReferencedBeforeEvaluationError),
        ("SELECT planetId FROM $satellites GROUP BY planetId HAVING MAX(id) > 100", 1, 1, ColumnNotFoundError),

        # Window function edge cases (if supported)
        # ("SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM $planets", 9, 2, None),
        # ("SELECT id, RANK() OVER (ORDER BY id) FROM $planets", 9, 2, None),

        # Complex CASE expressions
        ("SELECT CASE WHEN id < 5 THEN 'small' WHEN id < 8 THEN 'medium' ELSE 'large' END FROM $planets", 9, 1, None),
        ("SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM $planets", 9, 1, None),
        ("SELECT CASE WHEN id IS NULL THEN 'null' WHEN id < 0 THEN 'negative' ELSE 'positive' END FROM $planets", 9, 1, None),

        # LIMIT with expressions
        ("SELECT * FROM $planets LIMIT 1 + 1", 2, 20, TypeError),
        ("SELECT * FROM $planets LIMIT 10 - 5", 5, 20, TypeError),

        # Edge cases with string functions
        ("SELECT * FROM $planets WHERE LENGTH(name) > 5", 5, 20, None),
        ("SELECT * FROM $planets WHERE UPPER(name) = 'EARTH'", 1, 20, None),
        ("SELECT * FROM $planets WHERE LOWER(name) LIKE 'mars'", 1, 20, None),

        # Temporal query edge cases
        ("SELECT * FROM $planets FOR DATES BETWEEN '2020-01-01' AND '2025-12-31'", 9, 20, None),
        ("SELECT * FROM $planets FOR '2023-06-15'", 9, 20, None),

        # Complex JOIN conditions
        ("SELECT p.id FROM $planets p INNER JOIN $satellites s ON p.id = s.planetId AND p.id < 5", 3, 1, None),
        ("SELECT COUNT(*) FROM $planets p LEFT JOIN $satellites s ON p.id = s.planetId WHERE s.id IS NULL", 1, 1, None),
        ("SELECT COUNT(*) FROM $satellites s RIGHT JOIN $planets p ON s.planetId = p.id WHERE s.id IS NOT NULL", 1, 1, None),

        # Self-join edge cases
        ("SELECT p1.id FROM $planets p1 JOIN $planets p2 ON p1.id = p2.id", 9, 1, None),
        ("SELECT COUNT(*) FROM $planets p1, $planets p2 WHERE p1.id != p2.id", 1, 1, None),

        # Multiple aggregations
        ("SELECT COUNT(*), SUM(id), AVG(id), MIN(id), MAX(id) FROM $planets", 1, 5, None),
        ("SELECT COUNT(DISTINCT id), COUNT(*) FROM $planets", 1, 2, None),

        # Empty result handling
        ("SELECT * FROM $planets WHERE FALSE", 0, 20, None),
        ("SELECT * FROM $planets WHERE 1 = 0", 0, 20, None),
        ("SELECT * FROM $planets WHERE id > 1000", 0, 20, None),

        # Extreme LIMIT values
        ("SELECT * FROM $planets LIMIT 999999", 9, 20, None),

        # Complex ORDER BY
        ("SELECT * FROM $planets ORDER BY id ASC, name DESC", 9, 20, None),
        ("SELECT id, name FROM $planets ORDER BY 1, 2", 9, 2, UnsupportedSyntaxError),

        # Aggregate with no GROUP BY
        ("SELECT COUNT(*), 'constant' FROM $planets", 1, 2, None),
        ("SELECT MAX(id), MIN(id), AVG(id) FROM $planets", 1, 3, None),

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
    opteryx.register_store(
        "iceberg",
        connector=IcebergConnector,
        catalog=iceberg,
        remove_prefix=True,
    )

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

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} EDGE_CASES SHAPE TESTS")
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
            if not isinstance(err, AssertionError):
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

    # Exit with appropriate code to signal success/failure to parent process
    if failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)
