#!/usr/bin/env python3
"""
Quick validation script - runs a subset of tests to verify disk_reader works.
"""

import hashlib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from opteryx.compiled.io.disk_reader import read_file, read_file_to_bytes


def test_real_files():
    """Quick test using real test data files."""
    print("Testing disk_reader with real files...")
    
    test_files = [
        "testdata/permissions.json",
        "testdata/flat/wordlist/english.txt",
        "testdata/flat/formats/jsonl/tweets.jsonl",
        "testdata/planets/planets.parquet",
    ]
    
    passed = 0
    for filepath in test_files:
        if not os.path.exists(filepath):
            print(f"  ⊘ {filepath} (not found)")
            continue
        
        # Read with Python
        with open(filepath, 'rb') as f:
            py_data = f.read()
        py_hash = hashlib.sha256(py_data).hexdigest()
        
        # Read with disk_reader
        cpp_data = bytes(read_file(filepath))
        cpp_hash = hashlib.sha256(cpp_data).hexdigest()
        
        # Verify
        if py_hash == cpp_hash and len(py_data) == len(cpp_data):
            print(f"  ✓ {filepath} ({len(cpp_data):,} bytes, hash match)")
            passed += 1
        else:
            print(f"  ✗ {filepath} (HASH MISMATCH!)")
            return False
    
    print(f"\n{passed}/{len(test_files)} tests passed")
    return passed > 0


def test_edge_cases():
    """Quick edge case tests."""
    print("\nTesting edge cases...")
    
    import tempfile
    
    # Empty file
    with tempfile.NamedTemporaryFile(delete=False) as f:
        temp_path = f.name
    try:
        data = read_file(temp_path)
        assert len(data) == 0
        print("  ✓ Empty file")
    finally:
        os.unlink(temp_path)
    
    # Small file
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        f.write(b"test data")
        temp_path = f.name
    try:
        data = bytes(read_file(temp_path))
        assert data == b"test data"
        print("  ✓ Small file")
    finally:
        os.unlink(temp_path)
    
    # Non-existent file
    try:
        read_file("/tmp/does_not_exist_xyz123.txt")
        print("  ✗ Should have raised FileNotFoundError")
        return False
    except FileNotFoundError:
        print("  ✓ FileNotFoundError raised correctly")
    
    return True


def test_io_hints():
    """Test I/O hints."""
    print("\nTesting I/O hints...")
    
    filepath = "testdata/permissions.json"
    if not os.path.exists(filepath):
        print("  ⊘ Test file not found")
        return True
    
    # Get expected hash
    with open(filepath, 'rb') as f:
        expected_hash = hashlib.sha256(f.read()).hexdigest()
    
    # Test different hint combinations
    hints = [
        (True, True, False, "sequential+willneed"),
        (True, True, True, "sequential+willneed+drop_after"),
        (False, False, False, "no hints"),
    ]
    
    for sequential, willneed, drop_after, desc in hints:
        data = bytes(read_file(filepath, sequential=sequential, 
                              willneed=willneed, drop_after=drop_after))
        actual_hash = hashlib.sha256(data).hexdigest()
        
        if actual_hash == expected_hash:
            print(f"  ✓ {desc}")
        else:
            print(f"  ✗ {desc} (hash mismatch)")
            return False
    
    return True


def main():
    print("=" * 60)
    print("disk_reader Quick Validation")
    print("=" * 60)
    print()
    
    try:
        from opteryx.compiled.io.disk_reader import read_file
        print("✓ Module imported successfully\n")
    except ImportError as e:
        print(f"✗ Failed to import: {e}\n")
        print("Please build the module first:")
        print("  python setup.py build_ext --inplace")
        return 1
    
    results = []
    results.append(test_real_files())
    results.append(test_edge_cases())
    results.append(test_io_hints())
    
    print("\n" + "=" * 60)
    if all(results):
        print("✓ All validation tests passed!")
        print("=" * 60)
        print("\nRun full test suite with:")
        print("  pytest tests/compiled/io/test_disk_reader.py -v")
        return 0
    else:
        print("✗ Some tests failed")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
