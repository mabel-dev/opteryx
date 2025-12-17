"""Test script for ArrayVector.from_sequence"""
import sys
import os

# Ensure the module is in the path
sys.path.insert(0, os.path.dirname(__file__))

try:
    from opteryx.draken.interop.arrow import vector_from_sequence
    import pyarrow as pa
    
    print("Testing ArrayVector.from_sequence...")
    
    # Test 1: Simple nested lists
    print("\nTest 1: Simple nested lists of integers")
    data = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]
    vec = vector_from_sequence(data)
    print(f"  Input: {data}")
    print(f"  Vector type: {type(vec).__name__}")
    print(f"  Vector length: {vec.length}")
    
    arrow_result = vec.to_arrow()
    print(f"  Arrow type: {arrow_result.type}")
    print(f"  Round-trip result: {arrow_result.to_pylist()}")
    assert arrow_result.to_pylist() == data, "Round-trip failed!"
    print("  ✓ Test 1 passed")
    
    # Test 2: Nested lists with None
    print("\nTest 2: Nested lists with None")
    data = [[1, 2], None, [3, 4, 5]]
    vec = vector_from_sequence(data)
    print(f"  Input: {data}")
    print(f"  Vector type: {type(vec).__name__}")
    print(f"  Vector null_count: {vec.null_count}")
    
    arrow_result = vec.to_arrow()
    print(f"  Round-trip result: {arrow_result.to_pylist()}")
    assert arrow_result.to_pylist() == data, "Round-trip with None failed!"
    print("  ✓ Test 2 passed")
    
    # Test 3: Empty nested lists
    print("\nTest 3: Empty nested lists")
    data = [[], [1], [], [2, 3]]
    vec = vector_from_sequence(data)
    print(f"  Input: {data}")
    
    arrow_result = vec.to_arrow()
    print(f"  Round-trip result: {arrow_result.to_pylist()}")
    assert arrow_result.to_pylist() == data, "Round-trip with empty lists failed!"
    print("  ✓ Test 3 passed")
    
    # Test 4: Nested lists of strings
    print("\nTest 4: Nested lists of strings")
    data = [['a', 'b'], ['c', 'd', 'e'], ['f']]
    vec = vector_from_sequence(data)
    print(f"  Input: {data}")
    
    arrow_result = vec.to_arrow()
    result = arrow_result.to_pylist()
    # Convert bytes to strings if needed
    result = [[item.decode('utf-8') if isinstance(item, bytes) else item for item in row] if row else row for row in result]
    print(f"  Round-trip result: {result}")
    assert result == data, "Round-trip with strings failed!"
    print("  ✓ Test 4 passed")
    
    # Test 5: Nested lists of floats
    print("\nTest 5: Nested lists of floats")
    data = [[1.1, 2.2], [3.3], [4.4, 5.5, 6.6]]
    vec = vector_from_sequence(data)
    print(f"  Input: {data}")
    
    arrow_result = vec.to_arrow()
    print(f"  Round-trip result: {arrow_result.to_pylist()}")
    assert arrow_result.to_pylist() == data, "Round-trip with floats failed!"
    print("  ✓ Test 5 passed")
    
    print("\n✅ All tests passed!")
    
except ImportError as e:
    print(f"Import error: {e}")
    print("Note: You may need to rebuild the Cython extensions:")
    print("  python setup.py build_ext --inplace")
    sys.exit(1)
except Exception as e:
    print(f"\n❌ Test failed with error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
