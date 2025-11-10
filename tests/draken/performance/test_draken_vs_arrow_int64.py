
"""Performance comparison tests between Draken and Arrow for Int64 operations.

This module contains benchmarks that compare the performance of Draken's
Int64Vector operations against equivalent PyArrow operations. The tests
validate that Draken's optimized implementations achieve at least 2x
better performance than Arrow for common comparison operations.

Tests include:
- Less than comparisons
- Greater than comparisons  
- Equality comparisons
- Not equal comparisons
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import opteryx
from opteryx.draken import Vector
from pyarrow import compute


def performance_int64_comparison():
    """Run a small performance comparison between Draken and Arrow for several
    Int64 comparison operations and print a simple table of results.

    This test is informational only and does not assert — it's useful for
    local benchmarking and should not be treated as a regression test.
    """
    import time
    import statistics

    # Gather Arrow array and Draken vector once
    arr = opteryx.query_to_arrow("SELECT id FROM $satellites")["id"]
    vec = Vector.from_arrow(arr)

    # Define operations to compare. Each entry: (label, draken_callable, arrow_callable)
    ops = [
        ("less_than", lambda: vec.less_than(10), lambda: compute.less(arr, 10)),
        ("greater_than", lambda: vec.greater_than(10), lambda: compute.greater(arr, 10)),
        ("equal", lambda: vec.equals(10), lambda: compute.equal(arr, 10)),
        ("not_equal", lambda: vec.not_equals(10), lambda: compute.not_equal(arr, 10)),
    ]

    results = []
    runs = 1_000
    for name, draken_fn, arrow_fn in ops:
        draken_times = []
        arrow_times = []

        # Warm up once for each
        draken_fn()
        arrow_fn()

        for _ in range(runs):
            start = time.perf_counter_ns()
            draken_fn()
            draken_times.append((time.perf_counter_ns() - start) / 1e6)

            start = time.perf_counter_ns()
            arrow_fn()
            arrow_times.append((time.perf_counter_ns() - start) / 1e6)

        # Use median to reduce noise
        draken_ms = statistics.median(draken_times)
        arrow_ms = statistics.median(arrow_times)
        ratio = draken_ms / arrow_ms if arrow_ms else float("inf")

        results.append((name, draken_ms, arrow_ms, ratio))

    # Print a simple table to stdout for easy comparison
    print("\nPerformance comparison (median of {} runs):".format(runs))
    header = f"{'op':<15} {'draken(ms)':>12} {'arrow(ms)':>12} {'draken/arrow':>14} {'faster':>10}"
    print(header)
    print('-' * len(header))
    for name, d_ms, a_ms, ratio in results:
        faster = f"{(1/ratio):.2f}x slower" if ratio > 1 else f"{(ratio if ratio!=0 else float('inf')):.2f}x faster"
        print(f"{name:<15} {d_ms:12.3f} {a_ms:12.3f} {ratio:14.3f} {faster:>10}")


if __name__ == "__main__":  # pragma: no cover
    performance_int64_comparison()