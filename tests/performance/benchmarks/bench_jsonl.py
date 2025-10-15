"""
Benchmark script for JSONL decoder performance.

This script tests the performance of the JSONL decoder with different optimizations.

Key findings:
- memchr (standard C library) is optimal for newline detection in JSONL with typical line lengths
- SIMD find_all has overhead from vector allocation that outweighs benefits for frequent patterns
- Pure Python json.loads is still faster for small datasets due to Cython call overhead

Recommendation: Use memchr for newline detection (current default implementation)
"""

import os
import pytest
import sys

os.environ["OPTERYX_DEBUG"] = ""

sys.path.insert(1, os.path.join(sys.path[0], "../../../../draken"))
sys.path.insert(1, os.path.join(sys.path[0], "../../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../../../rugo"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import time

import orjson as json


def generate_test_jsonl(num_lines=10000, num_fields=20):
    """Generate test JSONL data with realistic content."""
    lines = []
    for i in range(num_lines):
        record = {
            'id': i,
            'name': f'Record_{i}',
            'value': i * 3.14159,
            'active': i % 2 == 0,
            'category': f'cat_{i % 10}',
            'description': f'This is a longer description field with some text content for record {i}',
            'tags': ['tag1', 'tag2', 'tag3'],
            'metadata': {'key1': 'value1', 'key2': 'value2'},
            'timestamp': f'2025-10-{(i % 28) + 1:02d}T12:00:00Z',
            'score': float(i % 100),
        }
        
        # Add extra fields to reach desired field count
        for j in range(10, num_fields):
            record[f'field_{j}'] = f'value_{j}_{i}'
        
        lines.append(json.dumps(record))
    
    return b'\n'.join(lines)


def benchmark_current_decoder(buffer, column_names, column_types, iterations=5):
    """Benchmark the current JSONL decoder implementation."""
    from opteryx.compiled.structures.jsonl_decoder import fast_jsonl_decode_columnar
    
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = fast_jsonl_decode_columnar(buffer, column_names, column_types)
        end = time.perf_counter()
        times.append(end - start)
    
    return {
        'mean': sum(times) / len(times),
        'min': min(times),
        'max': max(times),
        'result': result
    }

def benchmark_pure_python(buffer, column_names, column_types, iterations=5):
    """Benchmark pure Python JSON parsing for comparison."""
    times = []
    lines = buffer.decode('utf-8').strip().split('\n')
    
    for _ in range(iterations):
        start = time.perf_counter()
        result = {col: [] for col in column_names}
        num_lines = 0
        
        for line in lines:
            if not line.strip():
                for col in column_names:
                    result[col].append(None)
                continue
                
            try:
                record = json.loads(line)
                num_lines += 1
                for col in column_names:
                    result[col].append(record.get(col))
            except json.JSONDecodeError:
                for col in column_names:
                    result[col].append(None)
        
        end = time.perf_counter()
        times.append(end - start)
    
    return {
        'mean': sum(times) / len(times),
        'min': min(times),
        'max': max(times),
        'result': (num_lines, len(column_names), result)
    }


def run_benchmarks():
    """Run comprehensive benchmarks on JSONL decoder."""
    print("=" * 80)
    print("JSONL Decoder Benchmark")
    print("=" * 80)
    print()
    
    # Test configurations
    configs = [
        {'name': 'Small (1K lines, 10 cols)', 'lines': 1000, 'fields': 10},
        {'name': 'Medium (10K lines, 10 cols)', 'lines': 10000, 'fields': 10},
        {'name': 'Large (50K lines, 10 cols)', 'lines': 50000, 'fields': 10},
        {'name': 'Wide (10K lines, 50 cols)', 'lines': 10000, 'fields': 50},
        {'name': 'Very Wide (50K lines, 100 cols)', 'lines': 50000, 'fields': 100},
    ]
    
    for config in configs:
        print(f"\nTest: {config['name']}")
        print("-" * 80)
        
        # Generate test data
        buffer = generate_test_jsonl(config['lines'], config['fields'])
        data_size_mb = len(buffer) / (1024 * 1024)
        print(f"Data size: {data_size_mb:.2f} MB")
        
        # Define columns to extract
        column_names = ['id', 'name', 'value', 'active', 'category', 
                       'description', 'tags', 'metadata', 'timestamp', 'score']
        column_types = {
            'id': 'int',
            'name': 'str',
            'value': 'float',
            'active': 'bool',
            'category': 'str',
            'description': 'str',
            'tags': 'other',
            'metadata': 'other',
            'timestamp': 'str',
            'score': 'float',
        }
        
        # Benchmark Cython implementation (memchr)
        print("\nCython Implementation (memchr):")
        cython_result = benchmark_current_decoder(buffer, column_names, column_types)
        print(f"  Mean time: {cython_result['mean']*1000:.2f} ms")
        print(f"  Min time:  {cython_result['min']*1000:.2f} ms")
        print(f"  Max time:  {cython_result['max']*1000:.2f} ms")
        print(f"  Throughput: {data_size_mb/cython_result['mean']:.2f} MB/s")
        print(f"  Lines processed: {cython_result['result'][0]}")
        
        # Benchmark pure Python implementation
        print("\nPure Python (json.loads):")
        python_result = benchmark_pure_python(buffer, column_names, column_types)
        print(f"  Mean time: {python_result['mean']*1000:.2f} ms")
        print(f"  Min time:  {python_result['min']*1000:.2f} ms")
        print(f"  Max time:  {python_result['max']*1000:.2f} ms")
        print(f"  Throughput: {data_size_mb/python_result['mean']:.2f} MB/s")
        print(f"  Lines processed: {python_result['result'][0]}")
        
        # Calculate speedups
        speedup_vs_cython = python_result['mean'] / cython_result['mean']
        print(f"Cython vs Pure Python: {speedup_vs_cython:.2f}x faster")
        
        # Verify results match
        cython_lines = cython_result['result'][0]
        python_lines = python_result['result'][0]
        if cython_lines == python_lines:
            print("✓ Results verified: all line counts match")
        else:
            print(f"✗ Warning: line count mismatch (memchr: {cython_lines}, Python: {python_lines})")
    
    print("\n" + "=" * 80)
    print("Benchmark Complete")
    print("=" * 80)



if __name__ == '__main__':
    # Run main benchmarks
    run_benchmarks()
