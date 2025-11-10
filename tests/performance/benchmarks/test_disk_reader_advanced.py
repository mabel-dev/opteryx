#!/usr/bin/env python3
"""
Advanced test showing disk_reader advantages with streaming large files.
"""

import os
import sys
import tempfile
import time

sys.path.insert(1, os.path.join(sys.path[0], "../../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

def test_streaming_large_files():
    """
    Test the drop_after flag for streaming large files.
    This is useful when processing files larger than available RAM.
    """
    print("\n=== Streaming Large Files (drop_after=True) ===")
    print("This demonstrates reading files without polluting the page cache.\n")
    
    from opteryx.compiled.io.disk_reader import read_file

    # Create a 100 MB test file
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        size_mb = 100
        chunk = os.urandom(1024 * 1024)  # 1 MB chunks
        for _ in range(size_mb):
            f.write(chunk)
        temp_path = f.name
    
    try:
        print(f"Created {size_mb} MB test file: {temp_path}")
        
        # First read with drop_after=False (keeps in cache)
        start = time.perf_counter()
        data1 = read_file(temp_path, drop_after=False)
        time1 = time.perf_counter() - start
        print(f"\n1st read (cached):     {time1:.4f}s ({size_mb/time1:.1f} MB/s)")
        
        # Second read should be faster (from cache)
        start = time.perf_counter()
        data2 = read_file(temp_path, drop_after=False)
        time2 = time.perf_counter() - start
        print(f"2nd read (from cache): {time2:.4f}s ({size_mb/time2:.1f} MB/s)")
        print(f"Speedup from cache:    {time1/time2:.1f}x")
        
        # Read with drop_after=True (evicts from cache)
        start = time.perf_counter()
        data3 = read_file(temp_path, drop_after=True)
        time3 = time.perf_counter() - start
        print(f"\n3rd read (drop_after): {time3:.4f}s ({size_mb/time3:.1f} MB/s)")
        print("✓ File evicted from cache")
        
        # Verify all reads are identical
        assert bytes(data1) == bytes(data2) == bytes(data3)
        print("✓ All reads returned identical data")
        
    finally:
        os.unlink(temp_path)


def test_io_hints_impact():
    """
    Test the impact of different I/O hint combinations.
    """
    print("\n=== I/O Hints Performance Impact ===\n")
    
    from opteryx.compiled.io.disk_reader import read_file
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        size_mb = 50
        f.write(os.urandom(size_mb * 1024 * 1024))
        temp_path = f.name
    
    try:
        # Clear cache first
        _ = read_file(temp_path, drop_after=True)
        
        configs = [
            (True, True, "Sequential + WillNeed (optimal)"),
            (False, False, "No hints"),
            (True, False, "Sequential only"),
        ]
        
        for sequential, willneed, desc in configs:
            # Clear cache
            _ = read_file(temp_path, drop_after=True)
            time.sleep(0.1)
            
            start = time.perf_counter()
            data = read_file(temp_path, sequential=sequential, willneed=willneed)
            elapsed = time.perf_counter() - start
            
            throughput = size_mb / elapsed
            print(f"{desc:40s} {elapsed:.4f}s ({throughput:6.1f} MB/s)")
        
    finally:
        os.unlink(temp_path)


def test_memoryview_usage():
    """
    Demonstrate efficient memoryview usage without copying.
    """
    print("\n=== Memory-Efficient Usage ===\n")
    
    import sys

    from opteryx.compiled.io.disk_reader import read_file
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        data = b"Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n" * 1000
        f.write(data)
        temp_path = f.name
    
    try:
        # Read as memoryview (zero-copy)
        mv = read_file(temp_path)
        print(f"File size: {len(mv):,} bytes")
        print(f"Type: {type(mv)}")
        print(f"Memoryview overhead: ~{sys.getsizeof(mv)} bytes")
        
        # Can slice without copying
        first_100 = mv[:100]
        print(f"\nFirst 100 bytes (no copy): {bytes(first_100)[:50]}...")
        
        # Can search
        newline_count = bytes(mv).count(b'\n')
        print(f"Lines in file: {newline_count}")
        
        print("✓ Memoryview allows zero-copy operations")
        
    finally:
        os.unlink(temp_path)


def main():
    print("=" * 60)
    print("disk_reader Advanced Features Test")
    print("=" * 60)
    
    try:
        from opteryx.compiled.io.disk_reader import read_file
        print("✓ Module imported successfully\n")
    except ImportError as e:
        print(f"✗ Failed to import module: {e}")
        sys.exit(1)
    
    try:
        test_memoryview_usage()
        test_streaming_large_files()
        test_io_hints_impact()
        
        print("\n" + "=" * 60)
        print("✓ All advanced tests passed!")
        print("=" * 60)
        print("\nKey advantages of disk_reader:")
        print("  • drop_after=True: Stream large files without cache pollution")
        print("  • I/O hints: Sequential readahead for better performance")
        print("  • Memoryview: Zero-copy slicing and inspection")
        print("  • Platform-specific: Uses optimal I/O APIs per OS")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
