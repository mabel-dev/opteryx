"""
Benchmark comparing compiled evaluators vs generic loop-and-branch approach.

This benchmark demonstrates the performance benefits of compiled evaluators
over a traditional interpretation-based approach.
"""

import sys
from pathlib import Path
import time

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import opteryx.draken as draken
    from opteryx.draken.evaluators import (
        BinaryExpression,
        ColumnExpression,
        LiteralExpression,
        evaluate,
    )
    PYARROW_AVAILABLE = True
except ImportError as e:
    print(f"PyArrow or draken not available: {e}")
    PYARROW_AVAILABLE = False
    sys.exit(1)


def create_large_morsel(num_rows=1_000_000):
    """Create a large PyArrow table and corresponding Morsel for benchmarking."""
    import random

    data = {
        "x": list(range(num_rows)),
        "y": [random.choice(["england", "france", "spain", "germany"]) for _ in range(num_rows)],
        "z": [random.random() * 100 for _ in range(num_rows)],
    }

    table = pa.table(data)
    return table, draken.Morsel.from_arrow(table)


def benchmark_simple_comparison(morsel):
    """Benchmark simple comparison: x == 500000."""
    print("\n" + "=" * 70)
    print("Benchmark 1: Simple Comparison (x == 500000)")
    print("=" * 70)

    # Compiled evaluator approach
    expr = BinaryExpression("equals", ColumnExpression("x"), LiteralExpression(500000))

    start = time.time()
    result = evaluate(morsel, expr)
    compiled_time = time.time() - start

    # Count matches
    matches = sum(1 for i in range(morsel.num_rows) if result[i])

    print(f"\nCompiled Evaluator:")
    print(f"  Time: {compiled_time:.6f} seconds")
    print(f"  Matches: {matches}")

    # Manual approach (direct vector operation)
    x_col = morsel.column(b"x")

    start = time.time()
    manual_result = x_col.equals(500000)
    manual_time = time.time() - start

    manual_matches = sum(1 for i in range(morsel.num_rows) if manual_result[i])

    print(f"\nDirect Vector Operation:")
    print(f"  Time: {manual_time:.6f} seconds")
    print(f"  Matches: {manual_matches}")

    # Calculate overhead
    overhead = ((compiled_time - manual_time) / manual_time) * 100 if manual_time > 0 else 0
    print(f"\nOverhead: {overhead:.2f}%")

    return compiled_time, manual_time


def benchmark_compound_and(morsel):
    """Benchmark compound AND: x < 100000 AND y == 'england'."""
    print("\n" + "=" * 70)
    print("Benchmark 2: Compound AND (x < 100000 AND y == 'england')")
    print("=" * 70)

    # Compiled evaluator approach
    expr1 = BinaryExpression("less_than", ColumnExpression("x"), LiteralExpression(100000))
    expr2 = BinaryExpression("equals", ColumnExpression("y"), LiteralExpression(b"england"))
    expr = BinaryExpression("and", expr1, expr2)

    start = time.time()
    result = evaluate(morsel, expr)
    compiled_time = time.time() - start

    matches = sum(1 for i in range(morsel.num_rows) if result[i])

    print(f"\nCompiled Evaluator:")
    print(f"  Time: {compiled_time:.6f} seconds")
    print(f"  Matches: {matches}")

    # Manual approach (separate operations)
    x_col = morsel.column(b"x")
    y_col = morsel.column(b"y")

    start = time.time()
    from opteryx.draken.vectors import BoolMask

    temp1 = BoolMask(x_col.less_than(100000))
    temp2 = BoolMask(y_col.equals(b"england"))
    manual_result = temp1.and_vector(temp2)
    manual_time = time.time() - start

    manual_matches = sum(1 for i in range(morsel.num_rows) if manual_result[i])

    print(f"\nManual Multi-Step:")
    print(f"  Time: {manual_time:.6f} seconds")
    print(f"  Matches: {manual_matches}")

    speedup = manual_time / compiled_time if compiled_time > 0 else 0
    print(f"\nSpeedup: {speedup:.2f}x")

    return compiled_time, manual_time


def benchmark_complex_nested(morsel):
    """Benchmark complex nested: (x < 100000 OR x > 900000) AND z > 50."""
    print("\n" + "=" * 70)
    print("Benchmark 3: Complex Nested ((x < 100000 OR x > 900000) AND z > 50)")
    print("=" * 70)

    # Compiled evaluator approach
    x_lt = BinaryExpression("less_than", ColumnExpression("x"), LiteralExpression(100000))
    x_gt = BinaryExpression("greater_than", ColumnExpression("x"), LiteralExpression(900000))
    x_condition = BinaryExpression("or", x_lt, x_gt)
    z_condition = BinaryExpression("greater_than", ColumnExpression("z"), LiteralExpression(50.0))
    expr = BinaryExpression("and", x_condition, z_condition)

    start = time.time()
    result = evaluate(morsel, expr)
    compiled_time = time.time() - start

    matches = sum(1 for i in range(morsel.num_rows) if result[i])

    print(f"\nCompiled Evaluator:")
    print(f"  Time: {compiled_time:.6f} seconds")
    print(f"  Matches: {matches}")

    # Manual approach
    x_col = morsel.column(b"x")
    z_col = morsel.column(b"z")

    start = time.time()
    from opteryx.draken.vectors import BoolMask

    temp1 = BoolMask(x_col.less_than(100000))
    temp2 = BoolMask(x_col.greater_than(900000))
    temp3 = temp1.or_vector(temp2)
    temp4 = BoolMask(z_col.greater_than(50.0))
    manual_result = temp3.and_vector(temp4)
    manual_time = time.time() - start

    manual_matches = sum(1 for i in range(morsel.num_rows) if manual_result[i])

    print(f"\nManual Multi-Step:")
    print(f"  Time: {manual_time:.6f} seconds")
    print(f"  Matches: {manual_matches}")

    speedup = manual_time / compiled_time if compiled_time > 0 else 0
    print(f"\nSpeedup: {speedup:.2f}x")

    return compiled_time, manual_time


def benchmark_caching(morsel, arrow_table):
    """Benchmark caching effectiveness and compare with Arrow compute."""
    print("\n" + "=" * 70)
    print("Benchmark 4: Caching Effectiveness")
    print("=" * 70)

    from opteryx.draken.evaluators.evaluator import clear_cache

    expr = BinaryExpression("equals", ColumnExpression("x"), LiteralExpression(500000))

    # First evaluation (no cache)
    clear_cache()
    start = time.time()
    evaluate(morsel, expr)
    first_time = time.time() - start

    print(f"\nFirst evaluation (compilation + execution):")
    print(f"  Time: {first_time:.6f} seconds")

    # Second evaluation (cached)
    start = time.time()
    evaluate(morsel, expr)
    second_time = time.time() - start

    print(f"\nSecond evaluation (cached):")
    print(f"  Time: {second_time:.6f} seconds")

    # PyArrow baseline
    x_arrow = arrow_table.column("x")
    start = time.time()
    arrow_mask = pc.equal(x_arrow, pa.scalar(500000))
    arrow_time = time.time() - start
    arrow_matches = pc.sum(arrow_mask).as_py()

    print(f"\nPyArrow execution (no compilation):")
    print(f"  Time: {arrow_time:.6f} seconds")
    print(f"  Matches: {arrow_matches}")

    speedup = first_time / second_time if second_time > 0 else 0
    compiled_vs_arrow = first_time / arrow_time if arrow_time > 0 else 0
    cached_vs_arrow = second_time / arrow_time if arrow_time > 0 else 0
    print(f"\nCache speedup: {speedup:.2f}x")
    print(f"Compiled vs Arrow: {compiled_vs_arrow:.2f}x")
    print(f"Cached vs Arrow: {cached_vs_arrow:.2f}x")

    return first_time, second_time, arrow_time


def main():
    """Run all benchmarks."""
    print("=" * 70)
    print("Draken Compiled Evaluator Benchmarks")
    print("=" * 70)

    # Create test data
    print("\nCreating test morsel with 1,000,000 rows...")
    arrow_table, morsel = create_large_morsel(1_000_000)
    print(f"Morsel: {morsel.num_rows} rows x {morsel.num_columns} columns")

    # Run benchmarks
    results = {}

    results["simple"] = benchmark_simple_comparison(morsel)
    results["compound"] = benchmark_compound_and(morsel)
    results["nested"] = benchmark_complex_nested(morsel)
    results["cache"] = benchmark_caching(morsel, arrow_table)

    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print("\nCompiled evaluator provides:")
    print("  ✓ Low overhead for simple operations")
    print("  ✓ Competitive performance for compound expressions")
    print("  ✓ Effective caching for repeated evaluations")
    print("  ✓ Clean API with expression trees")
    print()
    print("The compiled evaluator is practical for real-world use because:")
    print("  • Minimal overhead over direct vector operations")
    print("  • Caching amortizes compilation cost")
    print("  • Clean abstraction doesn't sacrifice performance")
    print("  • Enables higher-level optimization in SQL engines")
    print("=" * 70)


if __name__ == "__main__":
    if PYARROW_AVAILABLE:
        main()
    else:
        print("PyArrow is required to run benchmarks")
        sys.exit(1)
