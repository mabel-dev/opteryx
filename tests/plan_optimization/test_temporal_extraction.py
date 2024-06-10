""" """

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime

import pytest

from opteryx.planner.sql_rewriter import extract_temporal_filters
from opteryx.utils.sql import clean_statement, remove_comments

# fmt:off
THIS_MORNING = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
TONIGHT = datetime.datetime.utcnow().replace(hour=23, minute=59, second=0, microsecond=0)
NOWISH = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
YESTERDAY = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)
# fmt:on

# fmt:off
STATEMENTS = [
        # simple single table cases with no FOR clause
        ("SELECT * FROM $planets;", [('$planets', None, None)]),
        ("SELECT * FROM $planets", [('$planets', None, None)]),
        ("SELECT * FROM $planets WITH(NO_CACHE)", [('$planets', None, None)]),
        ("SELECT * FROM $planets\n;", [('$planets', None, None)]),
        ("SELECT * FROM $planets WHERE name = ?", [('$planets', None, None)]),
        ("SELECT * FROM $planets GROUP BY name", [('$planets', None, None)]),
        ("SELECT * FROM $planets ORDER BY name", [('$planets', None, None)]),
        ("SELECT * FROM $planets HAVING name = ?", [('$planets', None, None)]),
        ("SELECT * FROM $planets LIMIT 1", [('$planets', None, None)]),
        ("SELECT * FROM $planets OFFSET 3", [('$planets', None, None)]),
        ("SELECT * FROM $planets\nWHERE name = ?;", [('$planets', None, None)]),
        ("SELECT * FROM (SELECT * FROM $planets)", [('$planets', None, None)]),
        ("SELECT * FROM (SELECT * FROM (SELECT * FROM $planets))", [('$planets', None, None)]),
        ("SELECT * FROM $planets AS P WHERE name = ?", [('$planets', None, None)]),
        ("SELECT * FROM (SELECT * FROM $planets AS P)", [('$planets', None, None)]),

        # simple single table cases with FOR 'date' clause
        ("SELECT * FROM $planets FOR TODAY;", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets\nFOR\nTODAY\n;", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY WITH(NO_CACHE)", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets\nFOR TODAY;", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets\tFOR TODAY;", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets -- FOR YESTERDAY\nFOR TODAY;", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY WHERE name = ?", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY GROUP BY name", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY ORDER BY name", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY HAVING name = ?", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY LIMIT 1", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY OFFSET 3", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY\nWHERE name = ?;", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY\nWHERE\nname = ?;", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY WHERE\nname = ?;", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM (SELECT * FROM $planets FOR TODAY)", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM (SELECT * FROM (SELECT * FROM $planets FOR TODAY))", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR TODAY AS P WHERE name = ?", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM (SELECT * FROM $planets FOR TODAY AS P)", [('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets WHERE name = 'FOR YESTERDAY';", [('$planets', None, None)]),
        ("SELECT * FROM $planets WHERE name = 'SELECT * FROM $planets FOR TODAY;';", [('$planets', None, None)]),

        # simple single table cases with FOR BETWEEN clause
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY;", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets\nFOR DATES\nBETWEEN YESTERDAY\tAND TODAY\n;", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY WITH(NO_CACHE)", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets\nFOR DATES BETWEEN YESTERDAY AND TODAY;", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets\tFOR DATES BETWEEN YESTERDAY AND TODAY;", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets -- FOR YESTERDAY\nFOR DATES BETWEEN YESTERDAY AND TODAY;", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY WHERE name = ?", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY GROUP BY name", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY ORDER BY name", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY HAVING name = ?", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY LIMIT 1", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY OFFSET 3", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY\nWHERE name = ?;", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY\nWHERE\nname = ?;", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY WHERE\nname = ?;", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM (SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY)", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM (SELECT * FROM (SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY))", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY AS P WHERE name = ?", [('$planets', YESTERDAY, NOWISH)]),
        ("SELECT * FROM (SELECT * FROM $planets FOR DATES BETWEEN YESTERDAY AND TODAY AS P)", [('$planets', YESTERDAY, NOWISH)]),

        # check this syntax is understood
        ("SELECT * FROM $planets FOR DATES IN THIS_MONTH;", [('$planets', THIS_MORNING.replace(day=1), NOWISH)]),
        ("SELECT * FROM $planets FOR DATES SINCE YESTERDAY;", [('$planets', YESTERDAY, NOWISH)]),

        # multiple relation references
        ("SELECT * FROM $planets INNER JOIN $planets ON (id);", [('$planets', None, None), ('$planets', None, None)]),
        ("SELECT * FROM $planets INNER\nJOIN $planets ON (id);", [('$planets', None, None), ('$planets', None, None)]),
        ("SELECT * FROM $planets LEFT JOIN $planets ON (id);", [('$planets', None, None), ('$planets', None, None)]),
        ("SELECT * FROM $planets LEFT OUTER JOIN $planets ON (id);", [('$planets', None, None), ('$planets', None, None)]),
        ("SELECT * FROM $planets CROSS JOIN $planets ON (id);", [('$planets', None, None), ('$planets', None, None)]),
        ("SELECT * FROM $planets FOR YESTERDAY INNER JOIN $planets ON (id);", [('$planets', YESTERDAY, YESTERDAY.replace(hour=23, minute=59)), ('$planets', None, None)]),
        ("SELECT * FROM $planets INNER JOIN $planets FOR YESTERDAY ON (id);", [('$planets', None, None), ('$planets', YESTERDAY, YESTERDAY.replace(hour=23, minute=59))]),
        ("SELECT * FROM $planets FOR YESTERDAY INNER JOIN (SELECT * FROM $planets FOR TODAY) ON (id);", [('$planets', YESTERDAY, YESTERDAY.replace(hour=23, minute=59)), ('$planets', THIS_MORNING, TONIGHT)]),
        ("SELECT * FROM $planets FOR YESTERDAY WHERE id IN (SELECT * FROM $planets FOR YESTERDAY);", [('$planets', YESTERDAY, YESTERDAY.replace(hour=23, minute=59)), ('$planets', YESTERDAY, YESTERDAY.replace(hour=23, minute=59))]),
        ("SELECT * FROM $planets FOR YESTERDAY WHERE id IN (SELECT * FROM $planets);", [('$planets', YESTERDAY, YESTERDAY.replace(hour=23, minute=59)), ('$planets', None, None)]),
        ("SELECT * FROM $planets WHERE id IN (SELECT * FROM $planets FOR YESTERDAY);", [('$planets', None, None), ('$planets', YESTERDAY, YESTERDAY.replace(hour=23, minute=59))]),
        ("SELECT * FROM $planets WHERE id IN (SELECT * FROM $planets);", [('$planets', None, None), ('$planets', None, None)]),
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
