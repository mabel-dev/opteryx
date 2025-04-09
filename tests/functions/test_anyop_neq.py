import os
import sys
import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.list_ops.list_anyop_neq import list_anyop_neq

def _test_comparison(literal, test_value, expected_result, _type=pa.string()):
    array = pa.array([[test_value]], type=pa.list_(_type))
    result = list(list_anyop_neq(literal, array))
    assert result == [expected_result], f"Expected {literal} != {test_value} to be {bool(expected_result)}, got {bool(result[0])}"

def test_basic_comparison_strings():
    _test_comparison("b", "a", 1)  # b != a -> True
    _test_comparison("a", "b", 1)  # a != b -> True
    _test_comparison("a", "a", 0)  # a != a -> False

def test_basic_comparison_bytes():
    _test_comparison(b"b", b"a", 1, pa.binary())  # b != a -> True
    _test_comparison(b"a", b"b", 1, pa.binary())  # a != b -> True
    _test_comparison(b"a", b"a", 0, pa.binary())  # a != a -> False

def test_basic_comparison_ints():
    # Integer comparisons
    _test_comparison(2, 1, 1, pa.int64())  # 2 != 1 -> True
    _test_comparison(1, 2, 1, pa.int64())  # 1 != 2 -> True
    _test_comparison(1, 1, 0, pa.int64())  # 1 != 1 -> False
    _test_comparison(-1, -2, 1, pa.int64())  # -1 != -2 -> True
    _test_comparison(0, -1, 1, pa.int64())  # 0 != -1 -> True

def test_comparison_float_numbers():
    # Float comparisons
    _test_comparison(1.5, 1.5, 0, pa.float64())  # 1.5 != 1.5 -> False
    _test_comparison(1.5, 2.5, 1, pa.float64())  # 1.5 != 2.5 -> True
    _test_comparison(0.0, -0.0, 1, pa.float64())  # 0.0 != -0.0 -> True (is this right?)
#    _test_comparison(float('nan'), float('nan'), 1, pa.float64())  # NaN != NaN -> True

def test_comparison_longer_strings():
    _test_comparison("hello", "hello", 0)  # "hello" != "hello" -> False
    _test_comparison("hello", "world", 1)  # "hello" != "world" -> True
    _test_comparison("", "empty", 1)  # "" != "empty" -> True
    _test_comparison("case", "CASE", 1)  # "case" != "CASE" -> True
    _test_comparison("  spaces  ", "  spaces  ", 0)  # "  spaces  " != "  spaces  " -> False
    _test_comparison("a" * 100, "a" * 100, 0)  # Long identical strings -> False
    _test_comparison("a" * 100, "a" * 99 + "b", 1)  # Long different strings -> True

def test_comparison_with_int_lists():
    # Test with lists containing multiple values
    array = pa.array([[1, 2, 3]], type=pa.list_(pa.int64()))
    result = list(list_anyop_neq(1, array))
    assert result == [1], "Expected 1 != [1,2,3] to be True (as 2,3 are different)"
    
    array = pa.array([[1, 1, 1]], type=pa.list_(pa.int64()))
    result = list(list_anyop_neq(1, array))
    assert result == [0], "Expected 1 != [1,1,1] to be False (as all elements are 1)"
    
    # Test with empty list
    array = pa.array([[]], type=pa.list_(pa.int64()))
    result = list(list_anyop_neq(1, array))
    assert result == [0], "Expected 1 != [] to be False (empty list has no different values)"

def test_comparison_with_string_lists():
    # Test with lists containing multiple string values
    array = pa.array([["apple", "banana", "cherry"]], type=pa.list_(pa.string()))
    result = list(list_anyop_neq("apple", array))
    assert result == [1], "Expected 'apple' != ['apple','banana','cherry'] to be True (as 'banana','cherry' are different)"
    
    array = pa.array([["apple", "apple", "apple"]], type=pa.list_(pa.string()))
    result = list(list_anyop_neq("apple", array))
    assert result == [0], "Expected 'apple' != ['apple','apple','apple'] to be False (as all elements are 'apple')"
    
    # Test with mixed case strings
    array = pa.array([["Apple", "apple", "APPLE"]], type=pa.list_(pa.string()))
    result = list(list_anyop_neq("apple", array))
    assert result == [1], "Expected 'apple' != ['Apple','apple','APPLE'] to be True (case sensitivity)"
    
    # Test with varying string lengths
    array = pa.array([["a", "aa", "aaa"]], type=pa.list_(pa.string()))
    result = list(list_anyop_neq("a", array))
    assert result == [1], "Expected 'a' != ['a','aa','aaa'] to be True (as 'aa','aaa' are different)"
    
    # Test with empty string in list
    array = pa.array([["", "apple", ""]], type=pa.list_(pa.string()))
    result = list(list_anyop_neq("", array))
    assert result == [1], "Expected '' != ['','apple',''] to be True (as 'apple' is different)"
    
    # Test with special characters
    array = pa.array([["a\nb", "a b", "a\tb"]], type=pa.list_(pa.string()))
    result = list(list_anyop_neq("a b", array))
    assert result == [1], "Expected 'a b' != ['a\\nb','a b','a\\tb'] to be True (different whitespace)"

def test_comparison_with_binary_lists():
    # Test with lists containing binary values
    array = pa.array([[b"data", b"info", b"bytes"]], type=pa.list_(pa.binary()))
    result = list(list_anyop_neq(b"data", array))
    assert result == [1], "Expected b'data' != [b'data',b'info',b'bytes'] to be True (as others are different)"
    
    array = pa.array([[b"data", b"data", b"data"]], type=pa.list_(pa.binary()))
    result = list(list_anyop_neq(b"data", array))
    assert result == [0], "Expected b'data' != [b'data',b'data',b'data'] to be False (as all elements are b'data')"
    
    # Test with empty binary
    array = pa.array([[b""]], type=pa.list_(pa.binary()))
    result = list(list_anyop_neq(b"", array))
    assert result == [0], "Expected b'' != [b''] to be False (both are empty)"
    
    # Test with binary containing zeros and non-printable characters
    array = pa.array([[b"\x00\x01\x02", b"\x00\x01\x03"]], type=pa.list_(pa.binary()))
    result = list(list_anyop_neq(b"\x00\x01\x02", array))
    assert result == [1], "Expected comparison with binary data to work correctly"
    
    # Test with longer binary data
    large_binary = b"x" * 1000
    array = pa.array([[large_binary, large_binary + b"y"]], type=pa.list_(pa.binary()))
    result = list(list_anyop_neq(large_binary, array))
    assert result == [1], "Expected large binary != [large_binary, large_binary+'y'] to be True"
    
    # Test with UTF-8 content stored as binary
    array = pa.array([[b"\xf0\x9f\x98\x80", b"\xf0\x9f\x98\x81"]], type=pa.list_(pa.binary()))  # 😀, 😁 emojis
    result = list(list_anyop_neq(b"\xf0\x9f\x98\x80", array))
    assert result == [1], "Expected binary emoji comparison to work correctly"

def test_comparison_with_nulls():
    # Test with null values
    array = pa.array([[None]], type=pa.list_(pa.string()))
    result = list(list_anyop_neq("a", array))
    assert result == [1], "Expected 'a' != None to be True"
    
    array = pa.array([[None, "a", None]], type=pa.list_(pa.string()))
    result = list(list_anyop_neq("a", array))
    assert result == [1], "Expected 'a' != [None,'a',None] to be True (as None != 'a')"
    _test_comparison(-1, -2, 1, pa.int64())  # -1 != -2 -> True
    _test_comparison(0, -1, 1, pa.int64())  # 0 != -1 -> True

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests
    run_tests()
