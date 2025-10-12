import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest
import jellyfish

from opteryx.third_party.fuzzy import soundex

# Test cases for soundex algorithm - these are just the input names
# We'll compare against jellyfish (reference implementation) rather than hardcoded values
TEST_NAMES = [
    'Test',
    'Therkelsen',
    'Troccoli',
    'Zelenski',
    'Zielonka',
    'Smith',
    'Johnson',
    'Williams',
    'Jones',
    'Brown',
    'Davis',
    'Miller',
    'Wilson',
    'Moore',
    'Taylor',
    'Anderson',
    'Thomas',
    'Jackson',
    'White',
    'Harris',
    'Martin',
    'Thompson',
    'Garcia',
    'Martinez',
    'Robinson',
    'Xi',
    'Lee',
    'Zz',
    'Kkk',
    'Aa',
    'Mmmmm',
    'O\'Neil',
    'Van der Sar',
    'St. John',
    'D\'Amico',
    'McDonald',
    'de la Cruz',
    'O\'Connor',
    'Von Trapp',
    'Al',
    'Bo',
    'Cy',
    'Du',
    'Ek',
    '',
    'Washington',
    'Jefferson',
    'Lincoln',
    'Roosevelt',
    'Kennedy',
    'Reagan',
    'Bush',
    'Clinton',
    'Obama',
    'Trump',
    'Biden',
    'Harrison',
    'Cleveland',
    'McKinley',
    'Coolidge',
    'Hoover',
    'Truman',
    'Eisenhower',
    'Nixon',
    'Ford',
    'Carter',
    'Adams',
    'Madison',
    'Monroe',
    'Jackson',
    'Polk',
    'Taylor',
    'Fillmore',
    'Pierce',
    'Buchanan',
    'Grant',
    'Hayes',
    'Garfield',
    'Arthur',
    'Taft',
    'Harding',
    # additional edge cases: short names
    'A',
    'B',
    'I',
    'Z',
    # additional edge cases: names with hyphens
    'Smith-Jones',
    'Mary-Ann',
    'Jean-Luc',
    # additional edge cases: names with apostrophes  
    'O\'Reilly',
    'D\'Angelo',
    'L\'Enfant',
    # additional edge cases: double letters
    'Phillip',
    'Matthew',
    'Lloyds',
    'Becker',
    # additional edge cases: silent letters
    'Knight',
    'Wright',
    'Knuth',
    'Pneumonia',  # (name used as test)
    # additional edge cases: names starting with vowels
    'Ashcroft',
    'Ellsworth',
    'Ingram',
    'Underwood',
    # additional edge cases: repeated consonants
    'Bennett',
    'Garrett',
    'Harriett',
    'Jarrett',
    # additional edge cases: common international names
    'Singh',
    'Zhang',
    'Nguyen',
    'Schmidt',
    'Mueller',
    'Kowalski',
    # additional edge cases: Welsh names
    'Llewellyn',
    'Cadwallader',
    'Rhys',
    # additional edge cases: Irish names
    'McCarthy',
    'Gallagher',
    'Sullivan',
    # additional edge cases: Scottish names
    'MacGregor',
    'MacDonald',
    'Campbell',
    # additional edge cases: names with special patterns
    'Schwarzenegger',
    'Tchotchke',
    'Pfeiffer',
    'Czajkowski',
]


@pytest.mark.parametrize("input_name", TEST_NAMES)
def test_soundex_against_reference(input_name):
    """Test that our soundex implementation matches the jellyfish reference implementation."""
    expected = jellyfish.soundex(input_name)
    actual = soundex(input_name)
    assert actual == expected, f"for '{input_name}' - expected: '{expected}', got: '{actual}'"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TEST_NAMES)} TESTS")
    failed_count = 0
    
    for test_name in TEST_NAMES:
        try:
            test_soundex_against_reference(test_name)
            print("\033[38;2;26;185;67m.\033[0m", end="\n")
        except AssertionError as e:
            print(f"Test failed for {test_name} with error: {e}")
            failed_count += 1

    print()
    if failed_count == 0:
        print("✅ All tests passed!")
    else:
        print(f"❌ {failed_count} tests failed")
