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


def benchmark_pyarrow(buffer, column_names, column_types, iterations=5):
    """Benchmark reading JSONL via pyarrow from an in-memory bytes buffer.

    The function attempts to import pyarrow and uses a BufferReader so the
    JSONL data is read from memory rather than disk. If pyarrow is not
    available the function returns an object with mean=None and an error
    string in 'error'.
    """
    # column_types is accepted for API parity but not used by pyarrow reader
    _ = column_types

    try:
        import pyarrow as pa
        import pyarrow.json as paj
    except ImportError as e:
        return {
            'mean': None,
            'min': None,
            'max': None,
            'result': None,
            'error': str(e)
        }

    times = []
    table = None
    for _ in range(iterations):
        start = time.perf_counter()
        # Use a BufferReader so pyarrow reads from memory
        buf = pa.py_buffer(buffer)
        reader = pa.BufferReader(buf)
        # Read JSON (JSON Lines) into a pyarrow Table
        table = paj.read_json(reader)
        end = time.perf_counter()
        times.append(end - start)

    mean = sum(times) / len(times)
    num_rows = None
    # table.num_rows should exist for a pyarrow.Table, but be defensive
    if hasattr(table, 'num_rows'):
        num_rows = table.num_rows
    else:
        try:
            num_rows = len(table)
        except TypeError:
            num_rows = None

    return {
        'mean': mean,
        'min': min(times),
        'max': max(times),
        'result': (num_rows, len(column_names), table),
        'error': None
    }


def print_benchmark_table(rows):
    """Print a compact table of benchmark rows.

    Each row is a dict with keys: impl, mean_ms, min_ms, max_ms, throughput, lines, (optional) error
    """
    # Determine column widths
    impl_w = max(len(r['impl']) for r in rows) + 2
    mean_w = 12
    min_w = 12
    max_w = 12
    thr_w = 12
    lines_w = 8

    header = f"{'Impl'.ljust(impl_w)} {'Mean (ms)'.rjust(mean_w)} {'Min (ms)'.rjust(min_w)} {'Max (ms)'.rjust(max_w)} {'MB/s'.rjust(thr_w)} {'Lines'.rjust(lines_w)}"
    print('\n' + header)
    print('-' * (impl_w + mean_w + min_w + max_w + thr_w + lines_w + 10))

    for r in rows:
        mean = f"{r['mean_ms']:.2f}" if r.get('mean_ms') is not None else 'n/a'
        minv = f"{r['min_ms']:.2f}" if r.get('min_ms') is not None else 'n/a'
        maxv = f"{r['max_ms']:.2f}" if r.get('max_ms') is not None else 'n/a'
        thr = f"{r['throughput']:.2f}" if r.get('throughput') is not None else 'n/a'
        lines = str(r.get('lines')) if r.get('lines') is not None else 'n/a'
        print(f"{r['impl'].ljust(impl_w)} {mean.rjust(mean_w)} {minv.rjust(min_w)} {maxv.rjust(max_w)} {thr.rjust(thr_w)} {lines.rjust(lines_w)}")
        if r.get('error'):
            print(f"{'':{impl_w}} Error: {r.get('error')}")



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
        
        # Run benchmarks for this config
        cython_result = benchmark_current_decoder(buffer, column_names, column_types)
        python_result = benchmark_pure_python(buffer, column_names, column_types)
        pyarrow_result = benchmark_pyarrow(buffer, column_names, column_types)

        # Prepare rows for a neat table of results
        rows = []

        def _safe_lines(res):
            try:
                return res['result'][0] if res and res.get('result') is not None else None
            except (KeyError, TypeError, IndexError):
                return None

        rows.append({
            'impl': 'Cython (memchr)',
            'mean_ms': cython_result['mean'] * 1000 if cython_result.get('mean') is not None else None,
            'min_ms': cython_result.get('min') * 1000 if cython_result.get('min') is not None else None,
            'max_ms': cython_result.get('max') * 1000 if cython_result.get('max') is not None else None,
            'throughput': (data_size_mb / cython_result['mean']) if cython_result.get('mean') else None,
            'lines': _safe_lines(cython_result)
        })

        rows.append({
            'impl': 'Pure Python',
            'mean_ms': python_result['mean'] * 1000 if python_result.get('mean') is not None else None,
            'min_ms': python_result.get('min') * 1000 if python_result.get('min') is not None else None,
            'max_ms': python_result.get('max') * 1000 if python_result.get('max') is not None else None,
            'throughput': (data_size_mb / python_result['mean']) if python_result.get('mean') else None,
            'lines': _safe_lines(python_result)
        })

        rows.append({
            'impl': 'PyArrow',
            'mean_ms': pyarrow_result['mean'] * 1000 if pyarrow_result.get('mean') is not None else None,
            'min_ms': pyarrow_result.get('min') * 1000 if pyarrow_result.get('min') is not None else None,
            'max_ms': pyarrow_result.get('max') * 1000 if pyarrow_result.get('max') is not None else None,
            'throughput': (data_size_mb / pyarrow_result['mean']) if pyarrow_result.get('mean') else None,
            'lines': _safe_lines(pyarrow_result),
            'error': pyarrow_result.get('error')
        })

        # Print a compact table for this config
        print_benchmark_table(rows)

        # Verify results match (line counts)
        cython_lines = rows[0]['lines']
        python_lines = rows[1]['lines']
        pyarrow_lines = rows[2]['lines']
        if cython_lines is not None and python_lines is not None and pyarrow_lines is not None:
            if cython_lines == python_lines == pyarrow_lines:
                print("✓ Results verified across all implementations")
            else:
                print(f"✗ Warning: mismatch across implementations (memchr: {cython_lines}, Python: {python_lines}, PyArrow: {pyarrow_lines})")

        # Benchmark pyarrow (in-memory)
        print("\nPyArrow (in-memory JSONL):")
        pyarrow_result = benchmark_pyarrow(buffer, column_names, column_types)
        if pyarrow_result.get('mean') is None:
            print(f"  Skipped pyarrow: {pyarrow_result.get('error')}")
        else:
            print(f"  Mean time: {pyarrow_result['mean']*1000:.2f} ms")
            print(f"  Min time:  {pyarrow_result['min']*1000:.2f} ms")
            print(f"  Max time:  {pyarrow_result['max']*1000:.2f} ms")
            print(f"  Throughput: {data_size_mb/pyarrow_result['mean']:.2f} MB/s")
            pyarrow_lines = pyarrow_result['result'][0]
            print(f"  Rows read (pyarrow): {pyarrow_lines}")

            # Compare pyarrow line counts to others when available
            if pyarrow_lines is not None:
                if cython_lines == pyarrow_lines == python_lines:
                    print("✓ Results verified across Cython, Python and PyArrow")
                else:
                    print(f"✗ Warning: mismatch across implementations (memchr: {cython_lines}, Python: {python_lines}, PyArrow: {pyarrow_lines})")
    
    print("\n" + "=" * 80)
    print("Benchmark Complete")
    print("=" * 80)



if __name__ == '__main__':
    # Run main benchmarks
    run_benchmarks()
