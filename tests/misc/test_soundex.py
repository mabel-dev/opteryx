import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from opteryx.third_party.soundex import Soundex

# fmt:off
TESTS = [
    ('Test', 'T230'),
    ('Therkelsen', 'T624'),
    ('Troccoli', 'T624'),
    ('Zelenski', 'Z452'),
    ('Zielonka', 'Z452')
]
# fmt:on


@pytest.mark.parametrize("input, result", TESTS)
def test_soundex_battery(input, result):

    actual_result = Soundex(4)(input)
    assert actual_result == result, f"expected: '{result}', got: '{actual_result}'"


if __name__ == "__main__":  # pragma: no cover

    print(f"RUNNING BATTERY OF {len(TESTS)} TESTS")
    for str1, str2 in TESTS:
        test_soundex_battery(str1, str2)

    print("✅ okay")
