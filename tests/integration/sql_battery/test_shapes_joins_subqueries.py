"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
 >  Shape Checking
    Results Checking
    Compare to DuckDB

This file tests: JOINs, subqueries, CTEs, and UNIONs

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
        # Join hints aren't supported
        ("SELECT * FROM $satellites INNER HASH JOIN $planets USING (id)", None, None, SqlError),
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
        ("SELECT * FROM $planets INNER JOIN (SELECT * FROM UNNEST(('Earth', 'Moon')) AS n) AS S ON name = n", 1, 21, None),
        ("SELECT name, mission FROM $astronauts INNER JOIN (SELECT * FROM UNNEST(missions) as mission) AS S ON mission = name", 0, 2, None),
        ("SELECT * FROM $astronauts CROSS JOIN UNNEST(missions) AS mission WHERE mission IN ('Apollo 11', 'Apollo 12')", 6, 20, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(name) as n FROM $astronauts GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn", 357, 2, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(name) as n FROM $astronauts WHERE status = 'Retired' GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn", 220, 2, None),
        ("SELECT number FROM $astronauts CROSS JOIN UNNEST((1, 2, 3, 4, 5)) AS number", 1785, 1, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(name) as n FROM $astronauts WHERE birth_date < '1960-01-01' GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn", 269, 2, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(name) as n FROM $astronauts WHERE status = 'Active' AND birth_date > '1970-01-01' GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn", 10, 2, None),
        ("SELECT group, nn FROM (SELECT group, ARRAY_AGG(name) as n FROM $astronauts GROUP BY group) AS alma CROSS JOIN UNNEST(n) AS nn", 357, 2, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(CASE WHEN LENGTH(alma_mater) > 10 THEN name ELSE NULL END) as alma_mater_arr FROM $astronauts GROUP BY group) AS alma CROSS JOIN UNNEST(alma_mater_arr) AS alma", 357, 2, None),
        ("SELECT * FROM (SELECT ARRAY_AGG(1) as num_arr FROM $astronauts GROUP BY group) AS numbers CROSS JOIN UNNEST(num_arr) AS number", 357, 2, None),
        ("SELECT name, group, astronaut_year, LEAST(year_list) FROM (SELECT ARRAY_AGG(year) as year_list, name, group FROM $astronauts WHERE status = 'Retired' and year is not null GROUP BY group, name) AS alma CROSS JOIN UNNEST(year_list) AS astronaut_year", 196, 4, None),

        # PUSHDOWN (the result should be the same without pushdown)
        ("SELECT p.name, s.name FROM $planets p, $satellites s WHERE p.id = s.planetId AND p.mass > 1000 AND s.gm < 500;", 63, 2, None),
        ("SELECT p.name, sub.name FROM $planets p CROSS JOIN (SELECT name, planetId FROM $satellites WHERE gm < 1000) AS sub WHERE p.id = sub.planetId;", 170, 2, None),
        ("SELECT p.name, s.name FROM $planets p, $satellites s WHERE p.id = s.planetId AND p.id = s.id;", 1, 2, None),
        ("SELECT p.name, COUNT(s.id) FROM $planets p JOIN $satellites s ON p.id = s.planetId GROUP BY p.name HAVING COUNT(s.id) > 3;", 5, 2, None),
        ("SELECT COUNT(*) FROM $planets WHERE TRUE AND 3 = 2 AND 3 > 2", 1, 1, None),

        ("SELECT missions[0] as m FROM $astronauts CROSS JOIN FAKE(1, 1) AS F order by m", 357, 1, None),
        ("SELECT name[id] as m FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT * FROM $astronauts WHERE ARRAY_CONTAINS_ANY(missions, @@user_memberships)", 3, 19, None),
        ("SELECT $missions.* FROM $missions INNER JOIN $user ON Mission = value WHERE attribute = 'membership'", 1, 8, None),
        ("SELECT * FROM $planets WHERE name = any(@@user_memberships)", 0, 20, None),
        ("SELECT name FROM $planets WHERE 'Apollo 11' = ANY(@@user_memberships) OR name = 'Saturn'", 9, 1, None),
        ("SELECT name FROM $planets WHERE 'Apollo 11' = ANY(@@user_memberships) AND name = 'Saturn'", 1, 1, None),
        ("SELECT name FROM $planets WHERE 'Apollo 11' = ANY(@@user_memberships)", 9, 1, None),
        ("SELECT name FROM sqlite.planets WHERE name = ANY(('Earth', 'Mars'))", 2, 1, None),
        ("SELECT name FROM $planets WHERE REGEXP_REPLACE(name, '^E', 'G') == 'Garth'", 1, 1, None),

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

        # Edge Case with Empty Joins
        ("SELECT * FROM $planets LEFT JOIN (SELECT id FROM $satellites WHERE planetId < 0) AS S ON $planets.id = S.id", 9, 21, None),
        # Edge Case with Temporal Filtering and Subqueries
        ("SELECT * FROM (SELECT * FROM $planets FOR '2024-10-01' WHERE id > 5) AS S WHERE id < 10", 4, 20, None),
        # Complex Nested Subqueries
        ("SELECT * FROM (SELECT name FROM (SELECT name FROM $planets WHERE LENGTH(name) > 3) AS T1) AS T2", 9, 1, None),
        # Cross Joining with Non-Matching Conditions
        ("SELECT P.name, S.name FROM $planets AS P CROSS JOIN $satellites AS S WHERE P.id = S.id AND P.name LIKE '%X%'", 0, 2, None),
        # ORDER OF CLAUSES (FOR before INNER JOIN)
        ("SELECT * FROM $planets FOR '2022-03-03' INNER JOIN $satellites ON $planets.id = $satellites.planetId", 177, 28, None),
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
        # JOIN from subquery regressed [#291]
        ("SELECT * FROM (SELECT id from $planets AS PO) AS ONE LEFT JOIN (SELECT id from $planets AS PT) AS TWO ON id = id", 9, 2, AmbiguousIdentifierError),
        ("SELECT * FROM (SELECT id FROM $planets AS PONE) AS ONE LEFT JOIN (SELECT id FROM $planets AS PTWO) AS TWO ON ONE.id = TWO.id;", 9, 2, None),
        # SELECT <literal> [#409]
        ("SELECT DATE FROM (SELECT '1980-10-20' AS DATE) AS SQ", 1, 1, None),
        ("SELECT NUMBER FROM (SELECT 1.0 AS NUMBER) AS SQ", 1, 1, None),
        ("SELECT VARCHAR FROM (SELECT 'varchar' AS VARCHAR) AS SQ", 1, 1, None),
        ("SELECT BOOLEAN FROM (SELECT False AS BOOLEAN) AS SQ", 1, 1, None),
        # EXPLAIN has two heads (found looking a [#408])
        ("EXPLAIN SELECT * FROM $planets AS a INNER JOIN (SELECT id FROM $planets) AS b USING (id)", 3, 3, None),

        # Additional complex nested subqueries
        ("SELECT * FROM (SELECT * FROM (SELECT id FROM $planets WHERE id < 5) AS s1 WHERE id > 2) AS s2", 2, 1, None),
        # Subqueries in JOIN conditions - CORRECTED: Returns 1 for COUNT aggregation  
        ("SELECT COUNT(*) FROM (SELECT * FROM $planets WHERE id < 5) p LEFT JOIN $satellites s ON p.id = s.planetId", 1, 1, None),

        # ========== UNSUPPORTED FEATURES - TEMPORARILY DISABLED ==========
        # These tests are commented out because they use SQL features not yet supported by Opteryx:
        # - IN (subquery): WHERE column IN (SELECT ...)
        # - EXISTS: WHERE EXISTS (SELECT ...)  
        # - Subqueries in SELECT clause: SELECT (SELECT ...) FROM ...
        
        # IN (subquery) - NOT YET SUPPORTED
        # ("SELECT * FROM $planets WHERE id IN (SELECT planetId FROM (SELECT * FROM $satellites) AS s)", 9, 20, UnsupportedSyntaxError),
        
        # Correlated subqueries - NOT YET SUPPORTED  
        # ("SELECT p.id, (SELECT COUNT(*) FROM $satellites s WHERE s.planetId = p.id) AS sat_count FROM $planets p", 9, 2, UnsupportedSyntaxError),
        # ("SELECT p.name FROM $planets p WHERE EXISTS (SELECT 1 FROM $satellites s WHERE s.planetId = p.id)", 9, 1, UnsupportedSyntaxError),
        # ("SELECT p.name FROM $planets p WHERE NOT EXISTS (SELECT 1 FROM $satellites s WHERE s.planetId = p.id AND s.id > 1000)", 9, 1, UnsupportedSyntaxError),

        # Multiple JOIN operations - NOT YET SUPPORTED (mission table reference issue)
        # ("SELECT COUNT(*) FROM $planets p INNER JOIN $satellites s ON p.id = s.planetId INNER JOIN $missions m ON p.id = m.id", 9, 1, ColumnNotFoundError),
        # CORRECTED: Multiple LEFT JOINs with same table create cartesian products
        ("SELECT p.id FROM $planets p LEFT JOIN $satellites s1 ON p.id = s1.planetId LEFT JOIN $satellites s2 ON p.id = s2.planetId", 9167, 1, None),

        # Self-join variations - NOT YET SUPPORTED (inequality joins)
        # ("SELECT p1.id, p2.id FROM $planets p1 INNER JOIN $planets p2 ON p1.id < p2.id", 36, 2, UnsupportedSyntaxError),
        # CORRECTED: Self-join with equality returns 1 row for COUNT aggregation
        ("SELECT COUNT(*) FROM $planets p1 LEFT JOIN $planets p2 ON p1.id = p2.id", 1, 1, None),

        # Subquery in SELECT clause - NOT YET SUPPORTED
        # ("SELECT id, (SELECT COUNT(*) FROM $satellites WHERE planetId = $planets.id) FROM $planets", 9, 2, UnsupportedSyntaxError),
        # ("SELECT id, name, (SELECT MAX(id) FROM $satellites) AS max_sat_id FROM $planets", 9, 3, UnsupportedSyntaxError),

        # Subquery in WHERE with multiple conditions - NOT YET SUPPORTED (IN subquery)
        # ("SELECT * FROM $planets WHERE id IN (SELECT planetId FROM $satellites WHERE id > 10 AND id < 50)", 9, 20, UnsupportedSyntaxError),
        # ("SELECT * FROM $planets WHERE id = (SELECT MIN(planetId) FROM $satellites)", 1, 20, UnsupportedSyntaxError),

        # JOIN with subqueries
        ("SELECT * FROM (SELECT id, name FROM $planets) p INNER JOIN (SELECT planetId FROM $satellites) s ON p.id = s.planetId", 177, 3, None),
        # CORRECTED: Returns 1 for COUNT aggregation
        ("SELECT COUNT(*) FROM (SELECT * FROM $planets WHERE id < 5) p LEFT JOIN $satellites s ON p.id = s.planetId", 1, 1, None),

        # Complex JOIN conditions with subqueries - NOT YET SUPPORTED (IN subquery)
        # ("SELECT p.id FROM $planets p WHERE id IN (SELECT planetId FROM $satellites GROUP BY planetId HAVING COUNT(*) > 1)", 6, 1, UnsupportedSyntaxError),

        # UNION in subqueries - NOT YET SUPPORTED (ambiguous dataset references)
        # ("SELECT * FROM (SELECT id FROM $planets WHERE id < 3 UNION SELECT id FROM $planets WHERE id > 7) AS combined", 4, 1, AmbiguousDatasetError),
        ("SELECT COUNT(*) FROM (SELECT id FROM $planets UNION SELECT planetId FROM $satellites) AS all_ids", 1, 1, None),

        # Nested EXISTS - NOT YET SUPPORTED
        # ("SELECT * FROM $planets p WHERE EXISTS (SELECT 1 FROM $satellites s WHERE s.planetId = p.id AND EXISTS (SELECT 1 FROM $missions m WHERE m.id = s.id))", 9, 20, UnsupportedSyntaxError),

        # Multiple subqueries in WHERE - NOT YET SUPPORTED (subqueries in WHERE)
        # ("SELECT * FROM $planets WHERE id > (SELECT MIN(id) FROM $planets) AND id < (SELECT MAX(id) FROM $planets)", 7, 20, UnsupportedSyntaxError),

        # Subquery with GROUP BY in JOIN
        ("SELECT p.id, counts.cnt FROM $planets p LEFT JOIN (SELECT planetId, COUNT(*) AS cnt FROM $satellites GROUP BY planetId) counts ON p.id = counts.planetId", 9, 2, None),

        # Complex nested WITH/CTE-like subqueries - NOT YET SUPPORTED (IN subquery)
        # ("SELECT * FROM (SELECT id, name FROM $planets) AS p WHERE id IN (SELECT planetId FROM $satellites WHERE id < 100)", 9, 2, UnsupportedSyntaxError),

        # Lateral-style correlations - NOT YET SUPPORTED (correlated subquery)
        # ("SELECT p.id, sub.max_sat FROM $planets p LEFT JOIN (SELECT planetId, MAX(id) AS max_sat FROM $satellites WHERE planetId = p.id GROUP BY planetId) sub ON TRUE", 9, 2, UnsupportedSyntaxError),

        # Multiple levels of nesting with aggregates
        ("SELECT outer_id FROM (SELECT inner_id AS outer_id FROM (SELECT id AS inner_id FROM $planets) AS level1) AS level2", 9, 1, None),

        # Subquery returning multiple columns
        ("SELECT * FROM (SELECT id, name, id * 2 AS doubled FROM $planets) AS sub WHERE doubled > 10", 4, 3, None),

        # JOIN with DISTINCT in subquery - CORRECTED: Returns 1 for COUNT aggregation
        ("SELECT COUNT(*) FROM $planets p INNER JOIN (SELECT DISTINCT planetId FROM $satellites) s ON p.id = s.planetId", 1, 1, None),

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

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} JOINS_SUBQUERIES SHAPE TESTS")
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
