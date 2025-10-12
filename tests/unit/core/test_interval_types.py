import os

os.environ.setdefault("DISABLE_HIGH_PRIORITY", "1")

import pyarrow
import pytest
from orso.types import OrsoTypes

import opteryx


@pytest.mark.parametrize(
    "query",
    [
        "SELECT INTERVAL '1' MONTH AS iv",
        "SELECT TIMESTAMP '2025-10-11 14:48:42.078300' - TIMESTAMP '2024-01-01' AS iv",
    ],
)
def test_interval_columns_report_correct_type(query):
    cursor = opteryx.connect().cursor()
    cursor.execute(query)

    assert cursor._schema.columns[0].type == OrsoTypes.INTERVAL

    arrow_result = opteryx.query_to_arrow(query)
    assert arrow_result.schema.field(0).type == pyarrow.month_day_nano_interval()
