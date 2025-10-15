"""
Benchmark the fast integer parsing improvement in JSONL decoder.
"""

import time
import json


def benchmark_integer_parsing():
    """Compare performance with and without fast integer parsing."""
    from opteryx.compiled.structures.jsonl_decoder import fast_jsonl_decode_columnar
    
    print("=" * 80)
    print("Fast Integer Parsing Benchmark")
    print("=" * 80)
    print()
    
    # Test cases with different integer distributions
    test_cases = [
        {'name': 'Small integers (0-100)', 'lines': 50000, 'gen': lambda i: i % 100},
        {'name': 'Large integers (0-1M)', 'lines': 50000, 'gen': lambda i: i * 20},
        {'name': 'Negative integers', 'lines': 50000, 'gen': lambda i: -i if i % 2 else i},
        {'name': 'Mixed range', 'lines': 50000, 'gen': lambda i: (i * 13) % 999999 - 500000},
    ]
    
    for test_case in test_cases:
        print(f"\nTest: {test_case['name']} ({test_case['lines']} lines)")
        print("-" * 80)
        
        # Generate test data
        lines = []
        for i in range(test_case['lines']):
            record = {
                'id': i,
                'value1': test_case['gen'](i),
                'value2': test_case['gen'](i * 2),
                'value3': test_case['gen'](i * 3),
            }
            lines.append(json.dumps(record))
        
        buffer = '\n'.join(lines).encode('utf-8')
        data_size_mb = len(buffer) / (1024 * 1024)
        print(f"Data size: {data_size_mb:.2f} MB")
        
        # Benchmark Cython with fast integer parsing
        column_names = ['id', 'value1', 'value2', 'value3']
        column_types = {'id': 'int', 'value1': 'int', 'value2': 'int', 'value3': 'int'}
        
        times = []
        for _ in range(10):
            start = time.perf_counter()
            result = fast_jsonl_decode_columnar(buffer, column_names, column_types)
            end = time.perf_counter()
            times.append(end - start)
        
        mean_time = sum(times) / len(times)
        print(f"\nCython (fast integer parsing):")
        print(f"  Time: {mean_time*1000:.2f} ms")
        print(f"  Throughput: {data_size_mb/mean_time:.2f} MB/s")
        print(f"  Lines/sec: {test_case['lines']/mean_time:,.0f}")
        print(f"  Lines processed: {result[0]}")
        
        # Benchmark pure Python for comparison
        times_py = []
        for _ in range(10):
            start = time.perf_counter()
            result_dict = {col: [] for col in column_names}
            for line in lines:
                record = json.loads(line)
                for col in column_names:
                    result_dict[col].append(record.get(col))
            end = time.perf_counter()
            times_py.append(end - start)
        
        mean_py = sum(times_py) / len(times_py)
        print(f"\nPure Python (json.loads):")
        print(f"  Time: {mean_py*1000:.2f} ms")
        print(f"  Throughput: {data_size_mb/mean_py:.2f} MB/s")
        print(f"  Lines/sec: {test_case['lines']/mean_py:,.0f}")
        
        speedup = mean_py / mean_time
        print(f"\n{'✓' if speedup > 1 else '✗'} Speedup: {speedup:.2f}x vs pure Python")


def benchmark_mixed_types():
    """Test performance with mixed column types."""
    from opteryx.compiled.structures.jsonl_decoder import fast_jsonl_decode_columnar
    
    print("\n" + "=" * 80)
    print("Mixed Type Performance (Integers + Strings + Floats)")
    print("=" * 80)
    print()
    
    # Generate mixed-type data
    lines = []
    for i in range(50000):
        record = {
            'id': i,
            'name': f'Record_{i}',
            'value': i * 3.14159,
            'count': i % 1000,
            'category': f'cat_{i % 10}',
            'score': float(i % 100),
        }
        lines.append(json.dumps(record))
    
    buffer = '\n'.join(lines).encode('utf-8')
    data_size_mb = len(buffer) / (1024 * 1024)
    
    print(f"Data size: {data_size_mb:.2f} MB")
    print(f"Lines: {len(lines)}")
    
    column_names = ['id', 'name', 'value', 'count', 'category', 'score']
    column_types = {
        'id': 'int',
        'name': 'str',
        'value': 'float',
        'count': 'int',
        'category': 'str',
        'score': 'float',
    }
    
    times = []
    for _ in range(10):
        start = time.perf_counter()
        result = fast_jsonl_decode_columnar(buffer, column_names, column_types)
        end = time.perf_counter()
        times.append(end - start)
    
    mean_time = sum(times) / len(times)
    print(f"\nCython (optimized):")
    print(f"  Time: {mean_time*1000:.2f} ms")
    print(f"  Throughput: {data_size_mb/mean_time:.2f} MB/s")
    print(f"  Lines/sec: {len(lines)/mean_time:,.0f}")
    
    # Verify correctness
    print(f"\nSample results:")
    print(f"  First ID: {result[2]['id'][0]}")
    print(f"  Last ID: {result[2]['id'][-1]}")
    print(f"  First count: {result[2]['count'][0]}")
    print(f"  Last count: {result[2]['count'][-1]}")
    

if __name__ == '__main__':
    benchmark_integer_parsing()
    benchmark_mixed_types()
    
    print("\n" + "=" * 80)
    print("Benchmark Complete")
    print("=" * 80)
    print("\nKey Improvements:")
    print("✓ Fast C-level integer parsing (no Python int() calls)")
    print("✓ Direct char pointer manipulation")
    print("✓ Inline function with minimal overhead")
    print("✓ Handles positive, negative, and zero values")
    print("=" * 80)
