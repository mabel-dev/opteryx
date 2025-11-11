#!/usr/bin/env python3
"""
Test script for the compiled disk_reader module.

This script tests the high-performance disk reader with various scenarios:
- Small files
- Large files
- Binary files
- Text files
- Performance benchmarks
- Edge cases
"""

import os
import random
import sys
import tempfile
import time

sys.path.insert(1, os.path.join(sys.path[0], "../../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

def create_test_file(path, size_mb=1, content_type="random"):
    """Create a test file with specified size and content type."""
    size_bytes = size_mb * 1024 * 1024
    
    with open(path, 'wb') as f:
        if content_type == "random":
            # Random binary data
            chunk_size = 64 * 1024
            for _ in range(size_bytes // chunk_size):
                f.write(os.urandom(chunk_size))
            remaining = size_bytes % chunk_size
            if remaining:
                f.write(os.urandom(remaining))
        elif content_type == "text":
            # Repeating text pattern
            pattern = b"The quick brown fox jumps over the lazy dog.\n"
            repeats = size_bytes // len(pattern)
            for _ in range(repeats):
                f.write(pattern)
            remaining = size_bytes % len(pattern)
            if remaining:
                f.write(pattern[:remaining])
        elif content_type == "zeros":
            # All zeros (highly compressible)
            f.write(b'\x00' * size_bytes)
        elif content_type == "sequential":
            # Sequential bytes (0-255 repeating)
            chunk = bytes(range(256))
            repeats = size_bytes // len(chunk)
            for _ in range(repeats):
                f.write(chunk)
            remaining = size_bytes % len(chunk)
            if remaining:
                f.write(chunk[:remaining])
    
    return size_bytes


def test_basic_read():
    """Test basic file reading functionality."""
    print("\n=== Test 1: Basic File Reading ===")
    
    from opteryx.compiled.io.disk_reader import read_file
    from opteryx.compiled.io.disk_reader import read_file_to_bytes
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        test_data = b"Hello, World! This is a test file.\n" * 100
        f.write(test_data)
        temp_path = f.name
    
    try:
        # Test read_file (returns memoryview)
        mv = read_file(temp_path)
        assert len(mv) == len(test_data), f"Size mismatch: {len(mv)} != {len(test_data)}"
        assert bytes(mv) == test_data, "Content mismatch"
        print(f"✓ read_file: Read {len(mv)} bytes successfully")
        
        # Test read_file_to_bytes
        data = read_file_to_bytes(temp_path)
        assert len(data) == len(test_data), f"Size mismatch: {len(data)} != {len(test_data)}"
        assert data == test_data, "Content mismatch"
        print(f"✓ read_file_to_bytes: Read {len(data)} bytes successfully")
        
    finally:
        os.unlink(temp_path)


def test_edge_cases():
    """Test edge cases like empty files, non-existent files, etc."""
    print("\n=== Test 2: Edge Cases ===")
    
    from opteryx.compiled.io.disk_reader import read_file

    # Test empty file
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        temp_path = f.name
    
    try:
        data = read_file(temp_path)
        assert len(data) == 0, "Empty file should return empty data"
        print("✓ Empty file: OK")
    finally:
        os.unlink(temp_path)
    
    # Test non-existent file
    try:
        read_file("/tmp/this_file_should_not_exist_12345.txt")
        assert False, "Should raise FileNotFoundError"
    except FileNotFoundError:
        print("✓ Non-existent file: Correctly raises FileNotFoundError")


def test_different_content_types():
    """Test reading files with different content types."""
    print("\n=== Test 3: Different Content Types ===")
    
    from opteryx.compiled.io.disk_reader import read_file
    
    with tempfile.TemporaryDirectory() as tmpdir:
        content_types = [
            ("random", 1),
            ("text", 1),
            ("zeros", 1),
            ("sequential", 1),
        ]
        
        for content_type, size_mb in content_types:
            path = os.path.join(tmpdir, f"{content_type}.dat")
            expected_size = create_test_file(path, size_mb, content_type)
            
            data = read_file(path)
            assert len(data) == expected_size, f"Size mismatch for {content_type}"
            print(f"✓ {content_type}: Read {len(data):,} bytes")


def test_large_file():
    """Test reading a larger file."""
    print("\n=== Test 4: Large File (50 MB) ===")
    
    from opteryx.compiled.io.disk_reader import read_file
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        size_mb = 50
        expected_size = create_test_file(f.name, size_mb, "random")
        temp_path = f.name
    
    try:
        start = time.perf_counter()
        data = read_file(temp_path)
        elapsed = time.perf_counter() - start
        
        assert len(data) == expected_size, f"Size mismatch"
        throughput_mb = size_mb / elapsed
        print(f"✓ Read {len(data):,} bytes in {elapsed:.3f}s ({throughput_mb:.1f} MB/s)")
    finally:
        os.unlink(temp_path)


def test_io_hints():
    """Test different I/O hint combinations."""
    print("\n=== Test 5: I/O Hints ===")
    
    from opteryx.compiled.io.disk_reader import read_file
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        test_data = b"x" * (1024 * 1024)  # 1 MB
        f.write(test_data)
        temp_path = f.name
    
    try:
        # Test different hint combinations
        hints = [
            (True, True, False, "sequential + willneed"),
            (False, True, False, "random + willneed"),
            (True, False, False, "sequential only"),
            (True, True, True, "sequential + willneed + drop_after"),
        ]
        
        for sequential, willneed, drop_after, desc in hints:
            data = read_file(temp_path, sequential=sequential, 
                           willneed=willneed, drop_after=drop_after)
            assert len(data) == len(test_data), f"Size mismatch for {desc}"
            print(f"✓ {desc}: OK")
    finally:
        os.unlink(temp_path)


def benchmark_comparison():
    """Compare performance with standard Python file reading."""
    print("\n=== Benchmark: Comparison with Python ===")
    
    from opteryx.compiled.io.disk_reader import read_file_mmap
    from opteryx.compiled.io.disk_reader import unmap_memory
    
    sizes_mb = [1, 10, 50]
    
    with tempfile.TemporaryDirectory() as tmpdir:
        for size_mb in sizes_mb:
            path = os.path.join(tmpdir, f"bench_{size_mb}mb.dat")
            expected_size = create_test_file(path, size_mb, "random")
            
            # Warm up
            with open(path, 'rb') as f:
                _ = f.read()
            
            # Python built-in
            start = time.perf_counter()
            with open(path, 'rb') as f:
                py_data = f.read()
            py_time = time.perf_counter() - start
            
            # Compiled reader
            start = time.perf_counter()
            cpp_data = read_file_mmap(path)
            unmap_memory(cpp_data)
            cpp_time = time.perf_counter() - start

            
            assert len(py_data) == len(cpp_data) == expected_size
            
            speedup = py_time / cpp_time if cpp_time > 0 else 0
            py_throughput = size_mb / py_time
            cpp_throughput = size_mb / cpp_time
            
            print(f"\n{size_mb} MB file:")
            print(f"  Python:   {py_time:.4f}s ({py_throughput:.1f} MB/s)")
            print(f"  C++:      {cpp_time:.4f}s ({cpp_throughput:.1f} MB/s)")
            print(f"  Speedup:  {speedup:.2f}x")


def test_correctness():
    """Verify that data is read correctly by comparing with Python."""
    print("\n=== Test 6: Correctness Verification ===")
    
    from opteryx.compiled.io.disk_reader import read_file
    
    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(5):
            path = os.path.join(tmpdir, f"verify_{i}.dat")
            
            # Create file with known content
            content = os.urandom(random.randint(1000, 100000))
            with open(path, 'wb') as f:
                f.write(content)
            
            # Read with both methods
            with open(path, 'rb') as f:
                py_data = f.read()
            
            cpp_data = bytes(read_file(path))
            
            assert py_data == cpp_data, f"Data mismatch for file {i}"
            print(f"✓ File {i}: {len(content):,} bytes match")


def main():
    """Run all tests."""
    print("=" * 60)
    print("disk_reader Module Test Suite")
    print("=" * 60)
    
    try:
        from opteryx.compiled.io.disk_reader import read_file
        print("✓ Module imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import module: {e}")
        print("\nPlease build the module first:")
        print("  python setup.py build_ext --inplace")
        sys.exit(1)
    
    try:
        test_basic_read()
        test_edge_cases()
        test_different_content_types()
        test_large_file()
        test_io_hints()
        test_correctness()
        benchmark_comparison()
        
        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
