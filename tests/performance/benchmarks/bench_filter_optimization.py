import time
import statistics
import numpy as np
import pyarrow as pa
from typing import Tuple


def current_filter_approach(table: pa.Table, mask: np.ndarray) -> Tuple[float, int]:
    """Current approach with multiple array conversions"""
    t0 = time.perf_counter()
    mask_pa = pa.array(mask, type=pa.bool_()) if not isinstance(mask, pa.BooleanArray) else mask
    indices = np.asarray(mask_pa).nonzero()[0]
    _ = table.take(indices) if indices.size > 0 else table.slice(0, 0)
    t1 = time.perf_counter()
    return (t1 - t0), len(indices)


def optimized_filter_approach(table: pa.Table, mask: np.ndarray) -> Tuple[float, int]:
    """Optimized approach with direct path"""
    t0 = time.perf_counter()
    if isinstance(mask, np.ndarray) and mask.dtype == np.bool_:
        indices = np.nonzero(mask)[0]
    elif isinstance(mask, list):
        indices = np.array([i for i, v in enumerate(mask) if v], dtype=np.int64)
    elif isinstance(mask, pa.BooleanArray):
        indices = np.asarray(mask).nonzero()[0]
    else:
        indices = np.asarray(mask, dtype=np.bool_).nonzero()[0]
    _ = table.take(indices) if indices.size > 0 else table.slice(0, 0)
    t1 = time.perf_counter()
    return (t1 - t0), len(indices)


def create_benchmark_table(rows: int, cols: int = 10) -> pa.Table:
    """Create a test table"""
    data = {f"col_{i}": np.random.randint(0, 1000, rows) for i in range(cols)}
    return pa.table(data)


def create_benchmark_mask(rows: int, select_prob: float = 0.5) -> np.ndarray:
    """Create a boolean mask"""
    return np.random.rand(rows) > (1.0 - select_prob)


def run_benchmark(n_rows: int, selectivity: float = 0.5, iterations: int = 10):
    """Run benchmark"""
    print(f"\n  n_rows={n_rows:,}, selectivity={selectivity:.0%}")
    
    table = create_benchmark_table(n_rows, cols=10)
    mask = create_benchmark_mask(n_rows, select_prob=selectivity)
    
    for _ in range(2):
        current_filter_approach(table, mask)
        optimized_filter_approach(table, mask)
    
    times_current = []
    for _ in range(iterations):
        elapsed, _ = current_filter_approach(table, mask)
        times_current.append(elapsed)
    
    times_optimized = []
    for _ in range(iterations):
        elapsed, _ = optimized_filter_approach(table, mask)
        times_optimized.append(elapsed)
    
    current_mean = statistics.mean(times_current)
    optimized_mean = statistics.mean(times_optimized)
    improvement = (current_mean - optimized_mean) / current_mean * 100
    
    print(f"    Current:   {current_mean*1000:7.3f} ms")
    print(f"    Optimized: {optimized_mean*1000:7.3f} ms")
    print(f"    Improvement: {improvement:6.2f}%")
    
    return improvement


if __name__ == "__main__":
    print("FILTER MASK ARRAY CONVERSION OPTIMIZATION")
    
    improvements = []
    
    print("\nTesting with different row counts (50% selectivity):")
    for n_rows in [10_000, 100_000, 1_000_000]:
        imp = run_benchmark(n_rows, selectivity=0.5, iterations=10)
        improvements.append(imp)
    
    print("\nTesting with different selectivities (100k rows):")
    for selectivity in [0.1, 0.3, 0.5, 0.7, 0.9]:
        imp = run_benchmark(100_000, selectivity=selectivity, iterations=10)
        improvements.append(imp)
    
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Average improvement: {statistics.mean(improvements):.2f}%")
    print(f"Min: {min(improvements):.2f}%, Max: {max(improvements):.2f}%")
