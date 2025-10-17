#!/usr/bin/env python
"""
Quick test to verify SIMD delimiter search is working correctly
"""

import sys

from opteryx.compiled.structures import jsonl_decoder


def test_basic_numbers():
    """Test that numbers followed by various delimiters are parsed correctly"""
    
    # Test data with numbers followed by different delimiters
    test_cases = [
        # minimal and common
        (b'{"num":42}', 'num', '42'),
        (b'{"num": 42}', 'num', '42'),
        (b'{"num" :42}', 'num', '42'),
        (b'{ "num":42}', 'num', '42'),

        # multiple spaces
        (b'{"num":  42}', 'num', '42'),
        (b'{"num"  :   42}', 'num', '42'),
        (b'{  "num"  :  42  }', 'num', '42'),

        # tabs
        (b'{"num":\t42}', 'num', '42'),
        (b'{"num"\t:\t42}', 'num', '42'),
        (b'{\t"num"\t:\t42\t}', 'num', '42'),
        (b'{ \t "num" \t : \t 42 \t }', 'num', '42'),
        (b'{\t\t\t\t\t\t\t"num"\t\t\t\t\t\t\t\t:\t\t\t\t\t\t\t\t42\t\t\t\t\t\t}', 'num', '42'),
        (b'{\t \t "num" \t \t : \t \t 42 \t \t }', 'num', '42'),

        # carriage returns (valid JSON whitespace)
        (b'{"num":\r42}', 'num', '42'),
        (b'{"num"\r:\r42}', 'num', '42'),

        # between multiple keys (ensures you stop at the right delimiter)
        (b'{"num":42, "id":1}', 'num', '42'),
        (b'{"num":42 , "id":1}', 'num', '42'),
        (b'{"num":42\t, "id":1}', 'num', '42'),

        # trailing spaces before end of object
        (b'{"num":42 }', 'num', '42'),
        (b'{"num":42\t}', 'num', '42'),
    ]
    
    for i, (jsonl_line, key, expected_value) in enumerate(test_cases):
        print(f"Test {i+1}: {jsonl_line.decode()}")
        
        column_names = [key]
        if 'score' in key or 'score' in jsonl_line.decode():
            column_types = {key: 'float'}
        else:
            column_types = {key: 'int'}
        
        try:
            num_rows, num_cols, result = jsonl_decoder.fast_jsonl_decode_columnar(
                jsonl_line, column_names, column_types
            )
            
            value = result[key][0]
            print(f"  Expected: {expected_value}, Got: {value}")
            
            # Compare as strings for easier debugging
            assert str(value) == expected_value, f"Mismatch: expected {expected_value}, got {value}"
            print(f"  ✓ PASSED")
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            return False
    
    return True


def test_multiple_rows():
    """Test multiple rows with numbers to verify delimiter search works across rows"""
    
    data = b'''\
{"id": 1, "score": 95.5}
{"id": 2, "score": 87.3}
{"id": 3, "score": 92.1}
'''
    
    print("\nTest multiple rows:")
    column_names = ['id', 'score']
    column_types = {'id': 'int', 'score': 'float'}
    
    try:
        num_rows, num_cols, result = jsonl_decoder.fast_jsonl_decode_columnar(
            data, column_names, column_types
        )
        
        print(f"  Rows: {num_rows}, Cols: {num_cols}")
        print(f"  IDs: {result['id']}")
        print(f"  Scores: {result['score']}")
        
        assert result['id'] == [1, 2, 3], f"ID mismatch: {result['id']}"
        assert result['score'] == [95.5, 87.3, 92.1], f"Score mismatch: {result['score']}"
        print("  ✓ PASSED")
        return True
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_edge_cases():
    """Test edge cases like very large numbers, negative numbers, etc."""
    
    test_cases = [
        (b'{"num": -42}', 'num', '-42'),
        (b'{"num": 0}', 'num', '0'),
        (b'{"num": 9999999999}', 'num', '9999999999'),
        (b'{"flt": -3.14159}', 'flt', '-3.14159'),
        (b'{"flt": 0.0}', 'flt', '0.0'),
    ]
    
    print("\nTest edge cases:")
    for i, (jsonl_line, key, expected_value) in enumerate(test_cases):
        print(f"Test {i+1}: {jsonl_line.decode()}")
        
        column_names = [key]
        column_types = {key: 'float'} if 'flt' in key else {key: 'int'}
        
        try:
            num_rows, num_cols, result = jsonl_decoder.fast_jsonl_decode_columnar(
                jsonl_line, column_names, column_types
            )
            
            value = result[key][0]
            print(f"  Expected: {expected_value}, Got: {value}")
            
            # Compare as strings for easier debugging
            assert str(value) == expected_value, f"Mismatch: expected {expected_value}, got {value}"
            print(f"  ✓ PASSED")
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    return True


if __name__ == '__main__':
    print("=" * 60)
    print("Testing SIMD Delimiter Search in JSONL Decoder")
    print("=" * 60)
    
    all_passed = True
    
    all_passed &= test_basic_numbers()
    all_passed &= test_multiple_rows()
    all_passed &= test_edge_cases()
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ ALL TESTS PASSED")
        sys.exit(0)
    else:
        print("✗ SOME TESTS FAILED")
        sys.exit(1)
