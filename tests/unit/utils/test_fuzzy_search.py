import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.utils import suggest_alternative

# fmt:off
TESTS = [
        ("apple", ["snapple", "crackle", "pop"], "snapple"),
        ("app_le", ["apple", "crackle", "pop"], "apple"),
        ("apple", ["AppLe", "crackle", "pop"], "AppLe"),
        ("apple", ["car", "plane", "bus"], None),
        ("dog", ["cat", "doge", "pig", "duck"], "doge"),
        ("crackle", ["cracker", "crack", "snack"], "cracker"),
        ("carrot", ["cabbage", "celery", "cucumber", "carrot", "cantaloupe"], "carrot"),
        ("", ["hello", "world"], None),
        ("apple", ["apple", "crackle", "pop"], "apple"),
        ("appple", ["apple", "crackle", "pop"], "apple"),
        ("aaple", ["apple", "crackle", "pop"], "apple"),
        ("appl", ["apple", "crackle", "pop"], "apple"),
        ("aple", ["apple", "crackle", "pop"], "apple"),
        ("aple", ["aple", "crackle", "pop"], "aple"),
        ("ppl", ["apple", "crackle", "pop"], "apple"),
        ("a", ["apple", "crackle", "pop"], None),
        ("", ["apple", "crackle", "pop"], None),
        ("", ["", "crackle", "pop"], None),
        ("", [], None),
        ("apple", ["appl", "aple", "aplee", "aplle"], "appl"),  # first best match
        ("a_b_c_d", ["abcd", "a_b_cd", "a_b_c_d_e"], "abcd"),
        ("a_b_c_d_e", ["abcd", "a_b_cd", "a_b_c_d_e"], "a_b_c_d_e"),
        ("a-b+c_d", ["abcd", "a_b+cd", "a-b+c_d-e"], "abcd"),
        ("apple", ["banana", "orange", "pear"], None),
        ("apple", [], None),
        ("apple", ["appl", "aple", "aplee", "aplle", "apple"], "apple"),
        ("123", ["123", "321", "456", "12"], "123"),
        ("123", ["124", "456", "12"], "124"), # first best match
        ("123", ["124", "321", "456", "12"], "124"),  # first best match
        ("1234", ["124", "321", "456", "12"], "124"),
        ("apple", ["appl", "aple", "aplee", "aplle", "aplle"], "appl"), # first best match
        ("apple", ["aple", "appl", "aplee", "aplle", "aplee"], "aple"), # first best match
        ("apple", ["aplee", "applle"], "applle"),
        ("banana", ["apple", "pear", "grape"], None),
        ("snack", ["cracker", "crack", "snack"], "snack"),
        ("cucumberrr", ["cabbage", "celery", "cucumber", "carrot", "cantaloupe"], "cucumber"),
        ("pop", ["snapple", "crackle", "pop"], "pop"),
        ("grape", ["apple", "pear", "grape"], "grape"),
        ("lettuce", ["cabbage", "celery", "cucumber", "carrot", "cantaloupe"], None),
        ("world", ["hello", "world"], "world"),
        ("mango", ["apple", "pear", "grape"], None),
        ("blueberry", ["strawberry", "raspberry", "blackberry"], None),
        ("elephant", ["lion", "tiger", "bear"], None),
        ("cafe", ["coffee", "café", "caffeine"], "café"),
        ("gra_pe", ["apple", "grape", "grapefruit"], "grape"),
        ("lemonade", ["limeade", "lemonade", "orangeade"], "lemonade"),
        ("coconut", ["coconut", "cocoa", "coffee"], "coconut"),
        ("eleven", ["seven", "eleven", "twelve"], "eleven"),
        ("kiwi", ["apple", "banana", "kiwi"], "kiwi"),
        ("beet", ["beetroot", "carrot", "potato"], None),
        ("pineapple", ["pineapple", "apple", "banana"], "pineapple"),
        ("watermelon", ["melon", "watermelon", "grapefruit"], "watermelon"),
        ("chocolate", ["vanilla", "strawberry", "chocolate"], "chocolate"),
        ("peach", ["apple", "pear", "peach"], "peach"),
        ("brocolli", ["spinach", "kale", "broccoli"], "broccoli"),
        ("apple", ["Apple", "ApPle", "aPple", "aPPle"], "Apple"),
        ("app.le", ["apple", "apples", "ap.le", "appl.e"], "apple"),
        ("!orange", ["apple", "banana", "!orange", "orange!"], "!orange"),
        ("Lemonade", ["lemonade", "LEMOnade", "LEMONADE"], "lemonade"),
        ("Kiwi!", ["Kiwi", "kiwi!"], "Kiwi"),
        ("strawberry", ["Strawberries", "Strawberry"], "Strawberry"),
        ("mango", ["MANGO", "MangO", "MaNgo", "manGO"], "MANGO"),
        ("!coconut!", ["coconut", "CocoNut", "coconut!", "!Coconut"], "coconut"),
        ("watermelon", ["watermelon", "WaTerMeLon", "watermelons", "wateRmElon"], "watermelon"),
        ("grape!", ["GraPe", "GRAPE"], "GraPe"),
        ("_melon", ["watermelon", "_melon", "me_lon", "MELON_"], "_melon"),
        ("apple?", ["apple", "APPLE?", "applE", "APPLE"], "apple"),
        ("BaNaNa", ["banana", "BANANA", "banAna", "BaNAna"], "banana"),
        ("pEar!", ["pear", "PEAr", "Pear!", "pear"], "pear"),
        ("!chocolate!", ["Chocolate", "!ChOcOlate!", "chocolate", "CHOCOLATE"], "Chocolate"),
        ("apri_cot", ["apricot", "APR!COT", "ApriCOT", "Apricot"], "apricot"),
        ("a.b.c.d", ["abcd", "a.b.cd", "a.b.c.d.e"], "abcd"),
        ("a+b-c*d", ["abcd", "a+b-c*d", "a-b+c-d*e"], "abcd"),
        ("aBcDe", ["AbCdE", "aBCde", "abcde"], "AbCdE"),
        ("b-a+n+a+n+a", ["banana", "apple", "orange"], "banana"),
        ("12345", ["54321", "12345", "543210"], "12345"),
        ("123.45", ["543.21", "123.45", "543.210"], "123.45"),
        ("!@#$%", ["!@#$%", "!@#$%^", "!@#$%^&"], "!@#$%"),
        ("hello.world", ["helloworld", "hello.world", "hello-world"], "helloworld"),
        ("a!", ["a!", "a!!", "a!!!"], "a!"),
        ("grapefruit", ["grapefruit", "grape", "fruit"], "grapefruit"),
        ("apple", ["APPLE", "apple"], "APPLE"),
        ("apple", ["APple", "ApPle", "apPle", "APPle"], "APple"),
        ("banana", ["banana", "baNanA", "BANANA"], "banana"),
        ("orange", ["OrAnGe", "or_ange", "orange"], "OrAnGe"),
    ]
# fmt:on


@pytest.mark.parametrize("string, candidates, expected", TESTS)
def test_suggestor(string, candidates, expected):
    """
    We're running a string through a set of candidate matches and returning the item
    which is the best match (expected)
    """
    assert (
        suggest_alternative(string, candidates) == expected
    ), f"{string}, {candidates}, {expected} != {suggest_alternative(string, candidates)}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} FUZZY TESTS")
    for s, c, e in TESTS:
        print("\033[38;2;26;185;67m.\033[0m", end="")
        test_suggestor(s, c, e)
    print()
    print("✅ okay")
