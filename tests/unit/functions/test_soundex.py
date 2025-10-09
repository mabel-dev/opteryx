import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.third_party.fuzzy import soundex

# fmt:off
TESTS = [
    ('Test', 'T230'),
    ('Therkelsen', 'T624'),
    ('Troccoli', 'T624'),
    ('Zelenski', 'Z452'),
    ('Zielonka', 'Z452'),
    ('Smith', 'S530'),
    ('Johnson', 'J525'),
    ('Williams', 'W452'),
    ('Jones', 'J520'),
    ('Brown', 'B650'),
    ('Davis', 'D120'),
    ('Miller', 'M460'),
    ('Wilson', 'W425'),
    ('Moore', 'M600'),
    ('Taylor', 'T460'),
    ('Anderson', 'A536'),
    ('Thomas', 'T520'),
    ('Jackson', 'J250'),
    ('White', 'W300'),
    ('Harris', 'H620'),
    ('Martin', 'M635'),
    ('Thompson', 'T512'),
    ('Garcia', 'G620'),
    ('Martinez', 'M635'),
    ('Robinson', 'R152'),
    ('Xi', 'X000'),
    ('Lee', 'L000'),
    ('Zz', 'Z200'),
    ('Kkk', 'K200'),
    ('Aa', 'A000'),
    ('Mmmmm', 'M500'),
    ('O\'Neil', 'O540'),
    ('Van der Sar', 'V536'),
    ('St. John', 'S325'),
    ('D\'Amico', 'D520'),
    ('McDonald', 'M235'),
    ('de la Cruz', 'D426'),
    ('O\'Connor', 'O256'),
    ('Von Trapp', 'V536'),
    ('Al', 'A400'),
    ('Bo', 'B000'),
    ('Cy', 'C000'),
    ('Du', 'D000'),
    ('Ek', 'E200'),
    ('', ''),
    ('Washington', 'W252'),
    ('Jefferson', 'J162'),
    ('Lincoln', 'L524'),
    ('Roosevelt', 'R214'),
    ('Kennedy', 'K530'),
    ('Reagan', 'R250'),
    ('Bush', 'B200'),
    ('Clinton', 'C453'),
    ('Obama', 'O150'),
    ('Trump', 'T651'),
    ('Biden', 'B350'),
    ('Harrison', 'H625'),
    ('Cleveland', 'C414'),
    ('McKinley', 'M254'),
    ('Coolidge', 'C432'),
    ('Hoover', 'H160'),
    ('Truman', 'T650'),
    ('Eisenhower', 'E256'),
    ('Nixon', 'N250'),
    ('Ford', 'F630'),
    ('Carter', 'C636'),
    ('Adams', 'A352'),
    ('Madison', 'M325'),
    ('Monroe', 'M560'),
    ('Jackson', 'J250'),
    ('Polk', 'P420'),
    ('Taylor', 'T460'),
    ('Fillmore', 'F456'),
    ('Pierce', 'P620'),
    ('Buchanan', 'B250'),
    ('Grant', 'G653'),
    ('Hayes', 'H200'),
    ('Garfield', 'G614'),
    ('Arthur', 'A636'),
    ('Taft', 'T130'),
    ('Harding', 'H635'),
]
# fmt:on


@pytest.mark.parametrize("input, result", TESTS)
def test_soundex_battery(input, result):
    actual_result = soundex(input)
    assert actual_result == result, f"for {input} - expected: '{result}', got: '{actual_result}'"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} TESTS")
    for str1, str2 in TESTS:
        try:
            test_soundex_battery(str1, str2)
            print("\033[38;2;26;185;67m.\033[0m", end="")
        except Exception as e:
            print(f"Test failed for {str1} and {str2} with error: {e}")

    print()
    print("✅ okay")
