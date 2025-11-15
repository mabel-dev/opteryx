"""
Test and benchmark the align_tables Cython implementation.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

import numpy as np
import pyarrow as pa
from opteryx.draken import Morsel
from opteryx.draken import align_tables, align_tables_pyarray


def create_test_morsels(n_rows=100_000):
    """Create test morsels for benchmarking."""
    # Source morsel with many columns
    source_data = {
        "id": pa.array(np.arange(n_rows, dtype=np.int64)),
        "a": pa.array(np.random.rand(n_rows)),
        "b": pa.array(np.random.rand(n_rows)),
        "c": pa.array(np.random.rand(n_rows)),
        "d": pa.array(np.random.rand(n_rows)),
        "e": pa.array(np.random.rand(n_rows)),
        "f": pa.array(np.random.rand(n_rows)),
        "g": pa.array(np.random.rand(n_rows)),
        "h": pa.array(np.random.rand(n_rows)),
        "i": pa.array(np.random.rand(n_rows)),
        "j": pa.array(np.random.rand(n_rows)),
    }
    source_table = pa.table(source_data)
    source_morsel = Morsel.from_arrow(source_table)
    
    # Append morsel with fewer columns
    append_data = {
        "id": pa.array(np.arange(n_rows, 2 * n_rows, dtype=np.int64)),
        "extra1": pa.array(np.random.randint(0, 1000, size=n_rows, dtype=np.int64)),
        "extra2": pa.array(np.random.rand(n_rows)),
        "extra3": pa.array(np.random.rand(n_rows)),
    }
    append_table = pa.table(append_data)
    append_morsel = Morsel.from_arrow(append_table)
    
    # Create random indices
    sample_size = n_rows // 2
    indices = np.random.choice(n_rows, size=sample_size, replace=True).astype(np.int32)
    
    return source_morsel, append_morsel, indices, indices


def test_basic_functionality():
    """Test basic alignment functionality."""
    print("Testing basic functionality...")
    
    # Small test case
    source_table = pa.table({
        "a": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "b": pa.array([10.0, 20.0, 30.0, 40.0, 50.0]),
    })
    
    append_table = pa.table({
        "c": pa.array([100, 200, 300, 400, 500], type=pa.int64()),
        "d": pa.array([1.1, 2.2, 3.3, 4.4, 5.5]),
    })
    
    source_morsel = Morsel.from_arrow(source_table)
    append_morsel = Morsel.from_arrow(append_table)
    
    # Test case 1: Simple alignment
    source_indices = np.array([0, 2, 4], dtype=np.int32)
    append_indices = np.array([1, 3, 4], dtype=np.int32)
    
    result = align_tables_pyarray(source_morsel, append_morsel, source_indices, append_indices)
    result_table = result.to_arrow()
    
    print(f"  Result shape: {result.shape}")
    print(f"  Result columns: {result.column_names}")
    assert result.num_rows == 3
    assert result.num_columns == 4
    assert set(result.column_names) == {b"a", b"b", b"c", b"d"}
    
    # Verify values
    result_arrow = result.to_arrow()
    assert result_arrow["a"].to_pylist() == [1, 3, 5]
    assert result_arrow["b"].to_pylist() == [10.0, 30.0, 50.0]
    assert result_arrow["c"].to_pylist() == [200, 400, 500]
    assert result_arrow["d"].to_pylist() == [2.2, 4.4, 5.5]
    
    print("  ✓ Basic functionality test passed")


def test_empty_indices():
    """Test with empty index arrays."""
    print("Testing empty indices...")
    
    source_table = pa.table({"a": pa.array([1, 2, 3], type=pa.int64())})
    append_table = pa.table({"b": pa.array([4, 5, 6], type=pa.int64())})
    
    source_morsel = Morsel.from_arrow(source_table)
    append_morsel = Morsel.from_arrow(append_table)
    
    source_indices = np.array([], dtype=np.int32)
    append_indices = np.array([], dtype=np.int32)
    
    result = align_tables_pyarray(source_morsel, append_morsel, source_indices, append_indices)
    
    assert result.num_rows == 0
    assert result.num_columns == 2
    print("  ✓ Empty indices test passed")


def test_duplicate_columns():
    """Test that duplicate column names are handled correctly."""
    print("Testing duplicate column handling...")
    
    source_table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "value": pa.array([10.0, 20.0, 30.0]),
    })
    
    append_table = pa.table({
        "id": pa.array([4, 5, 6], type=pa.int64()),  # Duplicate column name
        "extra": pa.array([100, 200, 300], type=pa.int64()),
    })
    
    source_morsel = Morsel.from_arrow(source_table)
    append_morsel = Morsel.from_arrow(append_table)
    
    source_indices = np.array([0, 1, 2], dtype=np.int32)
    append_indices = np.array([0, 1, 2], dtype=np.int32)
    
    result = align_tables_pyarray(source_morsel, append_morsel, source_indices, append_indices)
    
    # Should only include 'id' from source, 'value' from source, and 'extra' from append
    assert result.num_columns == 3
    assert set(result.column_names) == {b"id", b"value", b"extra"}
    print("  ✓ Duplicate columns test passed")


def benchmark_align():
    """Benchmark the align_tables function."""
    print("\nBenchmarking align_tables...")
    
    source, append, s_idx, a_idx = create_test_morsels(n_rows=10_000)
    
    print(f"  Source morsel: {source.num_rows} rows, {source.num_columns} columns")
    print(f"  Append morsel: {append.num_rows} rows, {append.num_columns} columns")
    print(f"  Index arrays: {len(s_idx)} indices")
    
    # Warmup
    _ = align_tables_pyarray(source, append, s_idx, a_idx)
    
    # Benchmark
    num_iterations = 10
    times = []
    
    for i in range(num_iterations):
        start = time.perf_counter()
        result = align_tables_pyarray(source, append, s_idx, a_idx)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    
    avg_time = np.mean(times)
    std_time = np.std(times)
    min_time = np.min(times)
    
    print(f"\n  Results ({num_iterations} iterations):")
    print(f"    Average: {avg_time*1000:.2f} ms")
    print(f"    Std dev: {std_time*1000:.2f} ms")
    print(f"    Min:     {min_time*1000:.2f} ms")
    print(f"    Max:     {np.max(times)*1000:.2f} ms")
    print(f"    Throughput: {len(s_idx) / avg_time / 1e6:.2f} M rows/sec")
    
    return result


def compare_with_pyarrow():
    """Compare performance with PyArrow-based implementation."""
    print("\nComparing with PyArrow baseline...")
    
    def align_tables_pyarrow_baseline(source_morsel, append_morsel, source_indices, append_indices):
        """PyArrow-based implementation for comparison."""
        source_table = source_morsel.to_arrow()
        append_table = append_morsel.to_arrow()
        
        # Take operations
        source_table = source_table.take(source_indices)
        append_table = append_table.take(append_indices)
        
        # Get non-overlapping columns
        source_names = set(source_table.schema.names)
        new_cols = []
        new_fields = []
        
        for field in append_table.schema:
            if field.name not in source_names:
                new_cols.append(append_table.column(field.name))
                new_fields.append(field)
        
        # Combine
        result_table = pa.table(
            list(source_table.columns) + new_cols,
            schema=pa.schema(list(source_table.schema) + new_fields)
        )
        
        return Morsel.from_arrow(result_table)
    
    source, append, s_idx, a_idx = create_test_morsels(n_rows=500_000)
    
    # Warmup
    _ = align_tables_pyarray(source, append, s_idx, a_idx)
    _ = align_tables_pyarrow_baseline(source, append, s_idx, a_idx)
    
    # Benchmark Cython version
    num_iterations = 5
    cython_times = []
    for i in range(num_iterations):
        start = time.perf_counter()
        _ = align_tables_pyarray(source, append, s_idx, a_idx)
        cython_times.append(time.perf_counter() - start)
    
    # Benchmark PyArrow version
    pyarrow_times = []
    for i in range(num_iterations):
        start = time.perf_counter()
        _ = align_tables_pyarrow_baseline(source, append, s_idx, a_idx)
        pyarrow_times.append(time.perf_counter() - start)
    
    cython_avg = np.mean(cython_times) * 1000
    pyarrow_avg = np.mean(pyarrow_times) * 1000
    speedup = pyarrow_avg / cython_avg
    
    print(f"  Cython implementation: {cython_avg:.2f} ms")
    print(f"  PyArrow baseline:      {pyarrow_avg:.2f} ms")
    print(f"  Speedup:               {speedup:.2f}x")


if __name__ == "__main__":
    print("=" * 60)
    print("Draken Morsel align_tables Test Suite")
    print("=" * 60)
    
    test_basic_functionality()
    test_empty_indices()
    test_duplicate_columns()
    
    result = benchmark_align()
    print(f"\nFinal result shape: {result.shape}")
    print(f"Final result columns: {result.column_names}")
    
    compare_with_pyarrow()
    
    print("\n" + "=" * 60)
    print("All tests passed! ✓")
    print("=" * 60)
