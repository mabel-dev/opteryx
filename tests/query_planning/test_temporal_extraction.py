"""

"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime

import pytest

from opteryx.components.sql_rewriter.sql_rewriter import clean_statement
from opteryx.components.sql_rewriter.sql_rewriter import remove_comments
from opteryx.components.sql_rewriter.temporal_extraction import extract_temporal_filters


TODAY = datetime.datetime.utcnow().date()
YESTERDAY = datetime.datetime.utcnow().date() - datetime.timedelta(days=1)

# fmt:off
STATEMENTS = [
        # simple single table cases with no FOR clause
        ("SELECT * FROM $planets;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets WITH(NO_CACHE)", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets\n;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets WHERE name = ?", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets GROUP BY name", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets ORDER BY name", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets HAVING name = ?", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets LIMIT 1", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets OFFSET 3", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets\nWHERE name = ?;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM (SELECT * FROM $planets)", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM (SELECT * FROM (SELECT * FROM $planets))", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets AS P WHERE name = ?", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM (SELECT * FROM $planets AS P)", [('$planets', TODAY, TODAY)]),

        # simple single table cases with FOR 'date' clause
        ("SELECT * FROM $planets FOR TODAY;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets\nFOR\nTODAY\n;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY WITH(NO_CACHE)", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets\nFOR TODAY;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets\tFOR TODAY;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets -- FOR YESTERDAY\nFOR TODAY;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY WHERE name = ?", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY GROUP BY name", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY ORDER BY name", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY HAVING name = ?", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY LIMIT 1", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY OFFSET 3", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY\nWHERE name = ?;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY\nWHERE\nname = ?;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY WHERE\nname = ?;", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM (SELECT * FROM $planets FOR TODAY)", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM (SELECT * FROM (SELECT * FROM $planets FOR TODAY))", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR TODAY AS P WHERE name = ?", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM (SELECT * FROM $planets FOR TODAY AS P)", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets WHERE name = 'FOR YESTERDAY';", [('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets WHERE name = 'SELECT * FROM $planets FOR TODAY;';", [('$planets', TODAY, TODAY)]),

        # simple single table cases with FOR BETWEEN clause
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY;", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets\nFOR DATES\nBETWEEN YESTERDAY\tAND TODAY\n;", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY WITH(NO_CACHE)", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets\nFOR DATES BETWEEN YESTERDAY AND TODAY;", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets\tFOR DATES BETWEEN YESTERDAY AND TODAY;", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets -- FOR YESTERDAY\nFOR DATES BETWEEN YESTERDAY AND TODAY;", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY WHERE name = ?", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY GROUP BY name", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY ORDER BY name", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY HAVING name = ?", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY LIMIT 1", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY OFFSET 3", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY\nWHERE name = ?;", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY\nWHERE\nname = ?;", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY WHERE\nname = ?;", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM (SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY)", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM (SELECT * FROM (SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY))", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY AS P WHERE name = ?", [('$planets', YESTERDAY, TODAY)]),
        ("SELECT * FROM (SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY AS P)", [('$planets', YESTERDAY, TODAY)]),

        # multiple relation references
        ("SELECT * FROM $planets INNER JOIN $planets ON (id);", [('$planets', TODAY, TODAY), ('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets INNER\nJOIN $planets ON (id);", [('$planets', TODAY, TODAY), ('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets LEFT JOIN $planets ON (id);", [('$planets', TODAY, TODAY), ('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets LEFT OUTER JOIN $planets ON (id);", [('$planets', TODAY, TODAY), ('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets CROSS JOIN $planets ON (id);", [('$planets', TODAY, TODAY), ('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR YESTERDAY INNER JOIN $planets ON (id);", [('$planets', YESTERDAY, YESTERDAY), ('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets INNER JOIN $planets FOR YESTERDAY ON (id);", [('$planets', TODAY, TODAY), ('$planets', YESTERDAY, YESTERDAY)]),
        ("SELECT * FROM $planets FOR YESTERDAY INNER JOIN (SELECT * FROM $planets FOR TODAY) ON (id);", [('$planets', YESTERDAY, YESTERDAY), ('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets FOR YESTERDAY WHERE id IN (SELECT * FROM $planets FOR YESTERDAY);", [('$planets', YESTERDAY, YESTERDAY), ('$planets', YESTERDAY, YESTERDAY)]),
        ("SELECT * FROM $planets FOR YESTERDAY WHERE id IN (SELECT * FROM $planets);", [('$planets', YESTERDAY, YESTERDAY), ('$planets', TODAY, TODAY)]),
        ("SELECT * FROM $planets WHERE id IN (SELECT * FROM $planets FOR YESTERDAY);", [('$planets', TODAY, TODAY), ('$planets', YESTERDAY, YESTERDAY)]),
        ("SELECT * FROM $planets WHERE id IN (SELECT * FROM $planets);", [('$planets', TODAY, TODAY), ('$planets', TODAY, TODAY)]),
    ]
# fmt:on


@pytest.mark.parametrize("statement, filters", STATEMENTS)
def test_temporal_extraction(statement, filters):
    """
    Test an battery of statements
    """

    clean = clean_statement(remove_comments(statement))
    _, extracted_filters = extract_temporal_filters(clean)

    assert filters == extracted_filters, f"{filters} != {extracted_filters}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(STATEMENTS)} TEMPORAL FILTER EXTRACTION TESTS")
    for statement, filters in STATEMENTS:
        print(statement)
        test_temporal_extraction(statement, filters)

    print("✅ okay")
