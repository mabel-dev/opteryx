"""
Tests to help confirm conformity to SQL-92
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx

# fmt:off
STATEMENTS = [
        
    ("SELECT +5", "E011-01"),
    ("SELECT -5", "E011-01"),
    ("SELECT 5", "E011-01"),
    ("SELECT +7.8", "E011-02"),
    ("SELECT -7.8", "E011-02"),
    ("SELECT +.2", "E011-02"),
    ("SELECT +.2E+2", "E011-02"),
    ("SELECT +.2E-2", "E011-02"),
    ("SELECT +.2E2", "E011-02"),
    ("SELECT +2", "E011-02"),
    ("SELECT +2.", "E011-02"),
    ("SELECT +2.2", "E011-02"),
    ("SELECT +2.2E+2", "E011-02"),
    ("SELECT +2.2E-2", "E011-02"),
    ("SELECT +2.2E2", "E011-02"),
    ("SELECT +2.E+2", "E011-02"),
    ("SELECT +2.E-2", "E011-02"),
    ("SELECT +2.E2", "E011-02"),
    ("SELECT +2E+2", "E011-02"),
    ("SELECT +2E-2", "E011-02"),
    ("SELECT +2E2", "E011-02"),
    ("SELECT -.2", "E011-02"),
    ("SELECT -.2E+2", "E011-02"),
    ("SELECT -.2E-2", "E011-02"),
    ("SELECT -.2E2", "E011-02"),
    ("SELECT -2", "E011-02"),
    ("SELECT -2.", "E011-02"),
    ("SELECT -2.2", "E011-02"),
    ("SELECT -2.2E+2", "E011-02"),
    ("SELECT -2.2E-2", "E011-02"),
    ("SELECT -2.2E2", "E011-02"),
    ("SELECT -2.E+2", "E011-02"),
    ("SELECT -2.E-2", "E011-02"),
    ("SELECT -2.E2", "E011-02"),
    ("SELECT -2E+2", "E011-02"),
    ("SELECT -2E-2", "E011-02"),
    ("SELECT -2E2", "E011-02"),
    ("SELECT .2", "E011-02"),
    ("SELECT .2E+2", "E011-02"),
    ("SELECT .2E-2", "E011-02"),
    ("SELECT .2E2", "E011-02"),
    ("SELECT 2", "E011-02"),
    ("SELECT 2.", "E011-02"),
    ("SELECT 2.2", "E011-02"),
    ("SELECT 2.2E+2", "E011-02"),
    ("SELECT 2.2E-2", "E011-02"),
    ("SELECT 2.2E2", "E011-02"),
    ("SELECT 2.E+2", "E011-02"),
    ("SELECT 2.E-2", "E011-02"),
    ("SELECT 2.E2", "E011-02"),
    ("SELECT 2E+2", "E011-02"),
    ("SELECT 2E-2", "E011-02"),
    ("SELECT 2E2", "E011-02"),
    ("SELECT 5 +3", "E011-04"),
    ("SELECT 5 -3", "E011-04"),
    ("SELECT 3.4 + 1.2", "E011-04"),
    ("SELECT 3.4 - 1.2", "E011-04"),
    ("SELECT 3 < 5", "E011-05"),
    ("SELECT 3 <= 5", "E011-05"),
    ("SELECT 3 <> 5", "E011-05"),
    ("SELECT 3 = 5", "E011-05"),
    ("SELECT 3 > 5", "E011-05"),
    ("SELECT 3 >= 5", "E011-05"),
    ("SELECT 3.7 < 5.2", "E011-05"),
    ("SELECT 3.7 <= 5.2", "E011-05"),
    ("SELECT 3.7 <> 5.2", "E011-05"),
    ("SELECT 3.7 = 5.2", "E011-05"),
    ("SELECT 3.7 > 5.2", "E011-05"),
    ("SELECT 3.7 >= 5.2", "E011-05"),
    ("SELECT ( 1 )", "E011-05"),
    ("SELECT ( 5.5 )", "E011-05"),
    ("SELECT 3 < 1.2", "E011-06"),
    ("SELECT 3 < 5", "E011-06"),
    ("SELECT 3 <= 1.2", "E011-06"),
    ("SELECT 3 <= 5", "E011-06"),
    ("SELECT 3 <> 1.2", "E011-06"),
    ("SELECT 3 <> 5", "E011-06"),
    ("SELECT 3 = 1.2", "E011-06"),
    ("SELECT 3 = 5", "E011-06"),
    ("SELECT 3 > 1.2", "E011-06"),
    ("SELECT 3 > 5", "E011-06"),
    ("SELECT 3 >= 1.2", "E011-06"),
    ("SELECT 3 >= 5", "E011-06"),
    ("SELECT 3.7 < 1.2", "E011-06"),
    ("SELECT 3.7 < 5", "E011-06"),
    ("SELECT 3.7 <= 1.2", "E011-06"),
    ("SELECT 3.7 <= 5", "E011-06"),
    ("SELECT 3.7 <> 1.2", "E011-06"),
    ("SELECT 3.7 <> 5", "E011-06"),
    ("SELECT 3.7 = 1.2", "E011-06"),
    ("SELECT 3.7 = 5", "E011-06"),
    ("SELECT 3.7 > 1.2", "E011-06"),
    ("SELECT 3.7 > 5", "E011-06"),
    ("SELECT 3.7 >= 1.2", "E011-06"),
    ("SELECT 3.7 >= 5", "E011-06"),
    ("SELECT ''", "E021-03"),
    ("SELECT 'a'", "E021-03"),
    ("SELECT 'abc'", "E021-03"),
#    ("SELECT CHARACTER_LENGTH ( 'foo' )", "E021-04"),
#    ("SELECT CHARACTER_LENGTH ( 'foo' USING CHARACTERS )", "E021-04"),
#    ("SELECT CHARACTER_LENGTH ( 'foo' USING OCTETS )", "E021-04"),
#    ("SELECT CHAR_LENGTH ( 'foo' )", "E021-04"),
#    ("SELECT CHAR_LENGTH ( 'foo' USING CHARACTERS )", "E021-04"),
#    ("SELECT CHAR_LENGTH ( 'foo' USING OCTETS )", "E021-04"),
#    ("SELECT OCTET_LENGTH ( 'foo' )", "E021-05"),
    ("SELECT SUBSTRING ( 'foo' FROM 1 )", "E021-06"),
    ("SELECT SUBSTRING ( 'foo' FROM 1 FOR 2 )", "E021-06"),
#    ("SELECT SUBSTRING ( 'foo' FROM 1 FOR 2 USING CHARACTERS )", "E021-06"),
#    ("SELECT SUBSTRING ( 'foo' FROM 1 FOR 2 USING OCTETS )", "E021-06"),
#    ("SELECT SUBSTRING ( 'foo' FROM 1 USING CHARACTERS )", "E021-06"),
#    ("SELECT SUBSTRING ( 'foo' FROM 1 USING OCTETS )", "E021-06"),
    ("SELECT 'foo' || 'bar'", "E021-07"),
    ("SELECT LOWER ( 'foo' )", "E021-08"),
    ("SELECT UPPER ( 'foo' )", "E021-08"),
    ("SELECT TRIM ( 'foo' )", "E021-09"),
    ("SELECT TRIM ( 'foo' FROM 'foo' )", "E021-09"),
    ("SELECT TRIM ( BOTH 'foo' FROM 'foo' )", "E021-09"),
#    ("SELECT TRIM ( BOTH FROM 'foo' )", "E021-09"),
#    ("SELECT TRIM ( FROM 'foo' )", "E021-09"),
    ("SELECT TRIM ( LEADING 'foo' FROM 'foo' )", "E021-09"),
#    ("SELECT TRIM ( LEADING FROM 'foo' )", "E021-09"),
    ("SELECT TRIM ( TRAILING 'foo' FROM 'foo' )", "E021-09"),
#    ("SELECT TRIM ( TRAILING FROM 'foo' )", "E021-09"),
    ("SELECT POSITION ( 'foo' IN 'bar' )", "E021-11"),
#    ("SELECT POSITION ( 'foo' IN 'bar' USING CHARACTERS )", "E021-11"),
#    ("SELECT POSITION ( 'foo' IN 'bar' USING OCTETS )", "E021-11"),
    ("SELECT 'foo' < 'bar'", "E021-12"),
    ("SELECT 'foo' <= 'bar'", "E021-12"),
    ("SELECT 'foo' <> 'bar'", "E021-12"),
    ("SELECT 'foo' = 'bar'", "E021-12"),
    ("SELECT 'foo' > 'bar'", "E021-12"),
    ("SELECT 'foo' >= 'bar'", "E021-12"),
    ("SELECT NULL", "E131"),
    ("SELECT 1 -- hello", "E161"),
    ("SELECT AVG(i) FROM GENERATE_SERIES(1, 10) as i", "E091-01"),
    ("SELECT COUNT(i) FROM GENERATE_SERIES(1, 10) as i", "E091-02"),
    ("SELECT MAX(i) FROM GENERATE_SERIES(1, 10) as i", "E091-03"),
    ("SELECT MIN(i) FROM GENERATE_SERIES(1, 10) as i", "E091-04"),
    ("SELECT SUM(i) FROM GENERATE_SERIES(1, 10) as i", "E091-05"),
    ("SELECT * FROM $planets ORDER BY mass;", "E121-02"),
    ("SELECT DATE '2016-03-26'", "F051-01"),
#    ("SELECT TIME '01:02:03'", "F051-02"),
    ("SELECT TIMESTAMP '2016-03-26 01:02:03'", "F051-03"),
    ("SELECT DATE '2016-03-26' < DATE '2016-03-26'", "F051-04"),
    ("SELECT DATE '2016-03-26' <= DATE '2016-03-26'", "F051-04"),
    ("SELECT DATE '2016-03-26' <> DATE '2016-03-26'", "F051-04"),
    ("SELECT DATE '2016-03-26' = DATE '2016-03-26'", "F051-04"),
    ("SELECT DATE '2016-03-26' > DATE '2016-03-26'", "F051-04"),
    ("SELECT DATE '2016-03-26' >= DATE '2016-03-26'", "F051-04"),
#    ("SELECT TIME '01:02:03' < TIME '01:02:03'", "F051-04"),
#    ("SELECT TIME '01:02:03' <= TIME '01:02:03'", "F051-04"),
#    ("SELECT TIME '01:02:03' <> TIME '01:02:03'", "F051-04"),
#    ("SELECT TIME '01:02:03' = TIME '01:02:03'", "F051-04"),
#    ("SELECT TIME '01:02:03' > TIME '01:02:03'", "F051-04"),
#    ("SELECT TIME '01:02:03' >= TIME '01:02:03'", "F051-04"),
    ("SELECT TIMESTAMP '2016-03-26 01:02:03' < TIMESTAMP '2016-03-26 01:02:03'", "F051-04"),
    ("SELECT TIMESTAMP '2016-03-26 01:02:03' <= TIMESTAMP '2016-03-26 01:02:03'", "F051-04"),
    ("SELECT TIMESTAMP '2016-03-26 01:02:03' <> TIMESTAMP '2016-03-26 01:02:03'", "F051-04"),
    ("SELECT TIMESTAMP '2016-03-26 01:02:03' = TIMESTAMP '2016-03-26 01:02:03'", "F051-04"),
    ("SELECT TIMESTAMP '2016-03-26 01:02:03' > TIMESTAMP '2016-03-26 01:02:03'", "F051-04"),
    ("SELECT TIMESTAMP '2016-03-26 01:02:03' >= TIMESTAMP '2016-03-26 01:02:03'", "F051-04"),
    ("SELECT CAST ( '2016-03-26' AS DATE )", "F051-05"),
#    ("SELECT CAST ( '01:02:03' AS TIME )", "F051-05"),
    ("SELECT CAST ( '2016-03-26 01:02:03' AS TIMESTAMP WITHOUT TIME ZONE )", "F051-05"),
    ("SELECT CAST ( CAST ( '2016-03-26' AS DATE ) AS VARCHAR )", "F051-05"),
#    ("SELECT CAST ( CAST ( '01:02:03' AS TIME ) AS VARCHAR )", "F051-05"),
    ("SELECT CAST ( CAST ( '2016-03-26 01:02:03' AS TIMESTAMP WITHOUT TIME ZONE ) AS VARCHAR )", "F051-05"),
#    ("SELECT CAST ( CAST ( '01:02:03' AS TIME ) AS TIME )", "F051-05"),
#    ("SELECT CAST ( CAST ( '01:02:03' AS TIME ) AS TIMESTAMP )", "F051-05"),
#    ("SELECT CAST ( CAST ( '01:02:03' AS TIME ) AS VARCHAR )", "F051-05"),
#    ("SELECT CAST ( CAST ( '2016-03-26 01:02:03' AS TIMESTAMP WITHOUT TIME ZONE ) AS DATE )", "F051-05"),
#    ("SELECT CAST ( CAST ( '2016-03-26 01:02:03' AS TIMESTAMP WITHOUT TIME ZONE ) AS TIME )", "F051-05"),
    ("SELECT CAST ( CAST ( '2016-03-26 01:02:03' AS TIMESTAMP WITHOUT TIME ZONE ) AS TIMESTAMP )", "F051-05"),
#    ("SELECT CAST ( CAST ( '2016-03-26 01:02:03' AS TIMESTAMP WITHOUT TIME ZONE ) AS VARCHAR )", "F051-05"),
    ("SELECT CURRENT_DATE", "F051-06"),
    ("SELECT CURRENT_TIME", "F051-07"),
#    ("SELECT CURRENT_TIME ( 0 )", "F051-07"),
    ("SELECT CURRENT_TIMESTAMP", "F051-08"),
#    ("SELECT CURRENT_TIMESTAMP ( 0 )", "F051-08"),
    ("SELECT CASE 0 WHEN 2 THEN 1 ELSE 1 END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN 1 ELSE NULL END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN 1 END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN NULL ELSE 1 END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN NULL ELSE NULL END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN NULL END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN 1 ELSE 1 END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN 1 ELSE NULL END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN 1 END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN NULL ELSE 1 END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN NULL ELSE NULL END", "F261-01"),
    ("SELECT CASE 0 WHEN 2 THEN NULL END", "F261-01"),
    ("SELECT CASE WHEN 0 = 1 THEN 1 ELSE 1 END", "F261-02"),
    ("SELECT CASE WHEN 0 = 1 THEN 1 ELSE NULL END", "F261-02"),
    ("SELECT CASE WHEN 0 = 1 THEN 1 END", "F261-02"),
    ("SELECT CASE WHEN 0 = 1 THEN NULL ELSE 1 END", "F261-02"),
    ("SELECT CASE WHEN 0 = 1 THEN NULL ELSE NULL END", "F261-02"),
    ("SELECT CASE WHEN 0 = 1 THEN NULL END", "F261-02"),
    ("SELECT NULLIF ( 1 , 1 )", "F261-03"),
    ("SELECT COALESCE ( 1 , 1 )", "F261-04"),


]
# fmt:on


@pytest.mark.parametrize("statement, feature", STATEMENTS)
def test_sql92(statement, feature):
    """
    Test an battery of statements
    """
    opteryx.query_to_arrow(statement)


if __name__ == "__main__":  # pragma: no cover
    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil

    from opteryx.utils.formatter import format_sql
    from tests import trunc_printable

    width = shutil.get_terminal_size((80, 20))[0]

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SQL92 TESTS")
    for index, (statement, feature) in enumerate(STATEMENTS):
        detail = f"\033[0;35m{feature}\033[0m {format_sql(statement)}"
        detail = trunc_printable(detail, width - 8)
        print(
            f"\033[0;36m{(index + 1):04}\033[0m {detail.ljust(width)}",
            end="",
        )
        test_sql92(statement, feature)
        print("✅")

    print("--- ✅ \033[0;32mdone\033[0m")
