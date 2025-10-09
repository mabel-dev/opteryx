import os
import sys
import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.list_ops import list_anyop_gt

def _test_gt_comparison(literal, test_value, expected_result, _type=pa.string()):
    """
    Helper to test comparison through list_anyop_gt
    """
    # Create a simple array with one list containing one item
    array = pa.array([[test_value]], type=pa.list_(_type))
    result = list(list_anyop_gt(literal, array))
    assert result == [expected_result], f"Expected {literal} > {test_value} to be {expected_result}, got {result[0]}"

def test_basic_gt_comparison():
    # Basic ASCII comparisons
    _test_gt_comparison("b", "a", 1)  # b > a -> True
    _test_gt_comparison("a", "b", 0)  # a > b -> False
    _test_gt_comparison("a", "a", 0)  # a > a -> False

def test_longer_string_gt_comparison():
    # Lexicographic longer strings
    _test_gt_comparison("Banana", "Bananas", 0)  # "Banana" < "Bananas"
    _test_gt_comparison("Bananas", "Banana", 1)  # "Bananas" > "Banana"
    _test_gt_comparison("banana", "Banana", 1)   # lowercase > uppercase
    _test_gt_comparison("Banana", "banana", 0)   # uppercase < lowercase
    _test_gt_comparison("abc", "ab", 1)          # "abc" > "ab"
    _test_gt_comparison("ab", "abc", 0)          # "ab" < "abc"
    _test_gt_comparison("z", "za", 0)            # "z" < "za"
    _test_gt_comparison("za", "z", 1)            # "za" > "z"

    _test_gt_comparison("Z", "MIT", 1)
    _test_gt_comparison("Z", "US Military Academy", 1)
    _test_gt_comparison("a", "MIT", 1)
    _test_gt_comparison("a", "US Military Academy", 1)
    _test_gt_comparison("A", "MIT", 0)
    _test_gt_comparison("A", "US Military Academy", 0)

def test_basic_gt_comparison_ints():
    # Integer comparisons
    _test_gt_comparison(2, 1, 1, pa.int64())  # 2 > 1 -> True
    _test_gt_comparison(1, 2, 0, pa.int64())  # 1 > 2 -> False
    _test_gt_comparison(1, 1, 0, pa.int64())  # 1 > 1 -> False
    _test_gt_comparison(-1, -2, 1, pa.int64())  # -1 > -2 -> True
    _test_gt_comparison(0, -1, 1, pa.int64())  # 0 > -1 -> True

def test_basic_gt_comparison_floats():
    # Float comparisons
    _test_gt_comparison(2.5, 1.5, 1, pa.float64())  # 2.5 > 1.5 -> True
    _test_gt_comparison(1.5, 2.5, 0, pa.float64())  # 1.5 > 2.5 -> False
    _test_gt_comparison(1.5, 1.5, 0, pa.float64())  # 1.5 > 1.5 -> False
    _test_gt_comparison(float('inf'), 1000.0, 1, pa.float64())  # inf > 1000.0 -> True
    _test_gt_comparison(1000.0, float('inf'), 0, pa.float64())  # 1000.0 > inf -> False
    _test_gt_comparison(float('nan'), 1.0, 0, pa.float64())  # nan > 1.0 -> False (special NaN handling)

def test_basic_gt_comparison_booleans():
    # Boolean comparisons
    _test_gt_comparison(True, False, 1, pa.bool_())  # True > False -> True
    _test_gt_comparison(False, True, 0, pa.bool_())  # False > True -> False
    _test_gt_comparison(True, True, 0, pa.bool_())  # True > True -> False
    _test_gt_comparison(False, False, 0, pa.bool_())  # False > False -> False

def test_basic_gt_comparison_dates():
    # Date comparisons
    import datetime
    
    _test_gt_comparison(datetime.date(2023, 1, 2), datetime.date(2023, 1, 1), 1, pa.date32())  # Later date > Earlier date -> True
    _test_gt_comparison(datetime.date(2023, 1, 1), datetime.date(2023, 1, 2), 0, pa.date32())  # Earlier date > Later date -> False
    _test_gt_comparison(datetime.date(2023, 1, 1), datetime.date(2023, 1, 1), 0, pa.date32())  # Same date > Same date -> False

def test_basic_gt_comparison_timestamps():
    # Timestamp comparisons
    import datetime
    
    _test_gt_comparison(
        datetime.datetime(2023, 1, 1, 12, 0, 1), 
        datetime.datetime(2023, 1, 1, 12, 0, 0), 
        1, 
        pa.timestamp('s')
    )  # Later time > Earlier time -> True
    
    _test_gt_comparison(
        datetime.datetime(2023, 1, 1, 12, 0, 0), 
        datetime.datetime(2023, 1, 1, 12, 0, 1), 
        0, 
        pa.timestamp('s')
    )  # Earlier time > Later time -> False
    
    _test_gt_comparison(
        datetime.datetime(2023, 1, 1, 12, 0, 0), 
        datetime.datetime(2023, 1, 1, 12, 0, 0), 
        0, 
        pa.timestamp('s')
    )  # Same time > Same time -> False

def test_basic_gt_comparison_decimals():
    # Decimal comparisons
    import decimal
    
    _test_gt_comparison(decimal.Decimal('2.5'), decimal.Decimal('1.5'), 1, pa.decimal128(5, 2))  # 2.5 > 1.5 -> True
    _test_gt_comparison(decimal.Decimal('1.5'), decimal.Decimal('2.5'), 0, pa.decimal128(5, 2))  # 1.5 > 2.5 -> False
    _test_gt_comparison(decimal.Decimal('1.5'), decimal.Decimal('1.5'), 0, pa.decimal128(5, 2))  # 1.5 > 1.5 -> False


def test_length_gt_comparison():
    # Strings equal up to the shorter length - longer wins
    _test_gt_comparison("abc", "ab", 1)  # abc > ab -> True
    _test_gt_comparison("ab", "abc", 0)  # ab > abc -> False

    
def test_gt_empty_strings():
    # Edge cases with empty strings
    _test_gt_comparison("", "a", 0)  # "" > a -> False
    _test_gt_comparison("a", "", 1)  # a > "" -> True
    _test_gt_comparison("", "", 0)  # "" > "" -> False
    
def test_gt_mixed_case():
    # Verify case sensitivity
    _test_gt_comparison("A", "a", 0)  # A > a -> False (ASCII 'A' < 'a')
    _test_gt_comparison("a", "A", 1)  # a > A -> True

def test_gt_boundary_values():
    # Test with null bytes and special characters
    _test_gt_comparison("b\0", "a\0", 1)  # b\0 > a\0 -> True
    _test_gt_comparison("a\1", "a\0", 1)  # a\1 > a\0 -> True

def test_gt_full_list_comparison():
    """Test ANY operator with multiple values in lists"""
    # Create a more complex test case
    test_data = [
        ["a", "b", "c"],      # "d" > ANY(["a", "b", "c"]) -> True
        ["x", "y", "z"],      # "d" > ANY(["x", "y", "z"]) -> False
        []                    # "d" > ANY([]) -> False (empty list)
    ]
    
    array = pa.array(test_data, type=pa.list_(pa.string()))
    result = list(list_anyop_gt("d", array))
    assert result == [1, 0, 0], f"Expected [1, 0, 0], got {result}"

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
