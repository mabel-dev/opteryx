"""
Detailed profiling of SIMD vs memchr for newline finding.
"""

import time


def benchmark_newline_finding():
    """Compare pure newline finding performance."""
    import platform
    print("=" * 80)
    print("Detailed Newline Finding Benchmark")
    print("=" * 80)
    print(f"Platform: {platform.machine()}\n")
    
    # Test with different data sizes
    test_cases = [
        {'name': '1MB (10K lines)', 'lines': 10000, 'line_size': 100},
        {'name': '10MB (100K lines)', 'lines': 100000, 'line_size': 100},
        {'name': '50MB (500K lines)', 'lines': 500000, 'line_size': 100},
    ]
    
    for test_case in test_cases:
        print(f"\nTest: {test_case['name']}")
        print("-" * 80)
        
        # Generate data
        lines = []
        for _ in range(test_case['lines']):
            line = 'x' * test_case['line_size']
            lines.append(line)
        
        buffer = '\n'.join(lines).encode('utf-8')
        data_size_mb = len(buffer) / (1024 * 1024)
        expected_newlines = test_case['lines'] - 1
        
        print(f"Data size: {data_size_mb:.2f} MB")
        print(f"Expected newlines: {expected_newlines}")
        
        # Test Python count (baseline)
        times_py = []
        for _ in range(20):
            start = time.perf_counter()
            count = buffer.count(b'\n')
            end = time.perf_counter()
            times_py.append(end - start)
        
        mean_py = sum(times_py) / len(times_py)
        print(f"\nPython count():")
        print(f"  Time: {mean_py*1000:.3f} ms")
        print(f"  Throughput: {data_size_mb/mean_py:.2f} MB/s")
        print(f"  Found: {count} newlines")
        
        # Test using the Cython wrapper
        try:
            # Import and test just the newline finding part
            print(f"\nCython SIMD (via jsonl_decoder):")
            from opteryx.compiled.structures.jsonl_decoder import fast_jsonl_decode_columnar_simd

            # Use minimal columns to isolate newline finding overhead
            column_names = ['x']  # Just one column to minimize processing
            column_types = {'x': 'str'}
            
            times_simd = []
            for _ in range(20):
                start = time.perf_counter()
                result = fast_jsonl_decode_columnar_simd(buffer, column_names, column_types)
                end = time.perf_counter()
                times_simd.append(end - start)
            
            mean_simd = sum(times_simd) / len(times_simd)
            print(f"  Time: {mean_simd*1000:.3f} ms")
            print(f"  Throughput: {data_size_mb/mean_simd:.2f} MB/s")
            print(f"  Lines: {result[0]}")
            
            # Also test memchr version
            from opteryx.compiled.structures.jsonl_decoder import fast_jsonl_decode_columnar
            times_memchr = []
            for _ in range(20):
                start = time.perf_counter()
                result = fast_jsonl_decode_columnar(buffer, column_names, column_types)
                end = time.perf_counter()
                times_memchr.append(end - start)
            
            mean_memchr = sum(times_memchr) / len(times_memchr)
            print(f"\nCython memchr (via jsonl_decoder):")
            print(f"  Time: {mean_memchr*1000:.3f} ms")
            print(f"  Throughput: {data_size_mb/mean_memchr:.2f} MB/s")
            print(f"  Lines: {result[0]}")
            
            speedup = mean_memchr / mean_simd
            print(f"\nSIMD speedup: {speedup:.3f}x")
            
        except ImportError as e:
            print(f"  Could not import: {e}")
    
    print("\n" + "=" * 80)


if __name__ == '__main__':
    benchmark_newline_finding()
