#!/usr/bin/env python3
"""
Quick reference examples for the disk_reader module.
"""

from opteryx.compiled.io.disk_reader import read_file
from opteryx.compiled.io.disk_reader import read_file_to_bytes


# Example 1: Basic file reading
def example_basic():
    """Read a file and get its contents."""
    data = read_file("temp.json")  # Returns memoryview
    print(f"File size: {len(data)} bytes")
    
    # Convert to bytes if needed
    data_bytes = bytes(data)
    print(f"First 50 chars: {data_bytes[:50]}")


# Example 2: Stream large files without cache pollution
def example_streaming():
    """Process multiple large files efficiently."""
    large_files = ["tmp/planets-gw0.duckdb", "tmp/planets-gw1.duckdb"]
    
    for filename in large_files:
        # Read and evict from cache to save memory
        data = read_file(filename, drop_after=True)
        print(f"{filename}: {len(data):,} bytes")


# Example 3: Zero-copy operations with memoryview
def example_zero_copy():
    """Efficiently slice data without copying."""
    data = read_file("temp.csv")
    
    # These operations don't copy the underlying data
    first_line = data[:data.tobytes().find(b'\n')]
    
    print(f"First line: {bytes(first_line)}")


# Example 4: Using read_file_to_bytes for convenience
def example_bytes():
    """Get bytes directly instead of memoryview."""
    data = read_file_to_bytes("temp.md")
    
    # Can use all bytes methods directly
    lines = data.split(b'\n')
    print(f"Number of lines: {len(lines)}")


# Example 5: I/O hints for optimal performance
def example_io_hints():
    """Control caching behavior for different scenarios."""
    
    # For large sequential reads (optimal)
    data = read_file("large_file.bin", sequential=True, willneed=True)
    
    # For random access patterns
    data = read_file("index_file.bin", sequential=False)
    
    # For one-time processing of huge files
    data = read_file("temporary_data.bin", drop_after=True)


if __name__ == "__main__":
    import sys
    
    print("disk_reader Quick Examples")
    print("=" * 60)
    
    try:
        print("\n1. Basic Reading:")
        example_basic()
        
        print("\n2. Streaming Large Files:")
        example_streaming()
        
        print("\n3. Zero-Copy Operations:")
        example_zero_copy()
        
        print("\n4. Bytes Convenience Method:")
        example_bytes()
        
        print("\n" + "=" * 60)
        print("✓ All examples completed successfully!")
        
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
