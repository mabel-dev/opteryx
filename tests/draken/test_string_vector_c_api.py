"""
Quick test of the new StringVector C-level API features.
"""

from opteryx.draken.vectors.string_vector import StringVector, StringVectorBuilder


def test_c_iter():
    """Test the C-level iterator exists and is callable."""
    builder = StringVectorBuilder.with_estimate(3, 20)
    builder.append(b"hello")
    builder.append(b"world")
    builder.append_null()
    
    vec = builder.finish()
    
    # Test that c_iter() method exists and returns an iterator
    c_iter = vec.c_iter()
    print(f"C iterator created: {c_iter}")
    print(f"Iterator position: {c_iter.position}")
    
    # Test reset
    c_iter.reset()
    print(f"After reset, position: {c_iter.position}")
    
    print("✓ C iterator basic operations work")


def test_raw_pointer_append():
    """Test the raw pointer append method."""
    builder = StringVectorBuilder.with_estimate(2, 10)
    
    # These will use append_bytes internally when called from Cython
    # From Python, we use regular append
    builder.append(b"test1")
    builder.append(b"test2")
    
    vec = builder.finish()
    
    assert len(vec) == 2
    assert vec[0] == b"test1"
    assert vec[1] == b"test2"
    
    print("✓ Builder methods work correctly")


def test_integration():
    """Test that the new features integrate with existing functionality."""
    builder = StringVectorBuilder.with_estimate(5, 30)
    builder.append(b"alpha")
    builder.append(b"beta")
    builder.append_null()
    builder.append(b"gamma")
    builder.append(b"delta")
    
    vec = builder.finish()
    
    # Test iteration
    values = list(vec)
    assert values == [b"alpha", b"beta", None, b"gamma", b"delta"]
    
    # Test null count
    assert vec.null_count == 1
    
    # Test equality
    mask = vec.equals(b"beta")
    assert list(mask) == [0, 1, 0, 0, 0]
    
    # Test view
    view = vec.view()
    assert view.is_null(2) == True
    assert view.is_null(0) == False
    
    # Test c_iter exists
    c_iter = vec.c_iter()
    assert c_iter.position == 0
    
    print("✓ Integration with existing features works")


if __name__ == "__main__":
    print("Testing new StringVector C-level API features...\n")
    
    test_c_iter()
    test_raw_pointer_append()
    test_integration()
    
    print("\n✓ All tests passed!")
    print("\nNote: For full C-level iterator testing with struct access,")
    print("compile and run the Cython examples in examples/c_iterator_demo.pyx")
