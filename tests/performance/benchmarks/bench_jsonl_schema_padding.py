import time
import statistics


def benchmark_schema_padding_strategies(n_rows=100_000, n_keys=20):
    """Benchmark different schema padding strategies"""
    # Generate test data with occasional missing keys
    rows = []
    for i in range(n_rows):
        row = {f"key_{j}": i * j for j in range(n_keys)}
        # Randomly omit 20% of keys
        for j in range(n_keys):
            if (i + j) % 5 == 0:  # 20% sparse
                del row[f"key_{j}"]
        rows.append(row)
    
    # Strategy 1: Current approach (post-process)
    times_current = []
    for _ in range(10):
        t0 = time.perf_counter()
        rows_copy1 = [r.copy() for r in rows]
        keys_union = set().union(*[r.keys() for r in rows_copy1])
        missing_keys = keys_union - set(rows_copy1[0].keys())
        if missing_keys:
            for row in rows_copy1:
                for key in missing_keys:
                    row.setdefault(key, None)
        t1 = time.perf_counter()
        times_current.append(t1 - t0)
    
    # Strategy 2: Optimized (build schema from sample, fill on parse)
    times_optimized = []
    for _ in range(10):
        t0 = time.perf_counter()
        rows_copy2 = []
        schema_keys = {f"key_{j}": None for j in range(n_keys)}
        for row in rows:
            complete_row = schema_keys.copy()
            complete_row.update(row)
            rows_copy2.append(complete_row)
        t1 = time.perf_counter()
        times_optimized.append(t1 - t0)
    
    current_mean = statistics.mean(times_current)
    optimized_mean = statistics.mean(times_optimized)
    improvement = (current_mean - optimized_mean) / current_mean * 100
    
    print(f"n_rows={n_rows:,}, n_keys={n_keys}")
    print(f"  Current approach:  {current_mean*1000:.2f}ms")
    print(f"  Optimized approach: {optimized_mean*1000:.2f}ms")
    print(f"  Improvement: {improvement:.1f}%\n")
    
    return improvement


if __name__ == "__main__":
    print("JSONL SCHEMA PADDING OPTIMIZATION")
    print("="*70 + "\n")
    
    improvements = []
    
    print("Testing with different row counts (20 keys):")
    for n_rows in [10_000, 100_000, 1_000_000]:
        imp = benchmark_schema_padding_strategies(n_rows=n_rows, n_keys=20)
        improvements.append(imp)
    
    print("Testing with different key counts (100k rows):")
    for n_keys in [5, 10, 20, 50, 100]:
        imp = benchmark_schema_padding_strategies(n_rows=100_000, n_keys=n_keys)
        improvements.append(imp)
    
    print("="*70)
    print(f"Average improvement: {statistics.mean(improvements):.2f}%")
    print(f"Min: {min(improvements):.2f}%, Max: {max(improvements):.2f}%")
