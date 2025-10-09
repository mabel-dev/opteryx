#!/usr/bin/env python
"""
Performance Benchmark for SQL Utilities

This script benchmarks the optimizations made to opteryx/utils/sql.py
Run this to verify the performance improvements.
"""

import os
import sys
import time

from opteryx.utils.sql import clean_statement
from opteryx.utils.sql import remove_comments
from opteryx.utils.sql import sql_like_to_regex

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))



def benchmark_sql_like_to_regex():
    """Benchmark the sql_like_to_regex function"""
    patterns = [
        ("a%", True, True),
        ("%a", True, True),
        ("%a%", True, True),
        ("test%", True, False),
        ("_test_", True, True),
        ("%google%", False, True),
        ("%.com", True, True),
        ("http%", False, False),
    ]
    
    iterations = 100000
    
    print("\n=== Benchmarking sql_like_to_regex ===")
    print(f"Running {iterations} iterations with {len(patterns)} patterns...")
    
    start = time.perf_counter()
    for _ in range(iterations):
        for pattern, full_match, case_sensitive in patterns:
            sql_like_to_regex(pattern, full_match, case_sensitive)
    elapsed = time.perf_counter() - start
    
    total_calls = iterations * len(patterns)
    avg_us = (elapsed * 1e6) / total_calls
    
    print(f"Total time: {elapsed:.4f}s")
    print(f"Average per conversion: {avg_us:.2f} microseconds")
    print(f"Throughput: {total_calls / elapsed:.0f} conversions/second")
    
    # Test cache effectiveness
    print("\n--- Testing LRU Cache Effectiveness ---")
    start = time.perf_counter()
    for _ in range(iterations):
        # Same pattern repeatedly - should hit cache
        sql_like_to_regex("%google%", False, True)
    elapsed_cached = time.perf_counter() - start
    
    print(f"Cached pattern ({iterations} calls): {elapsed_cached:.4f}s")
    print(f"Average per call: {(elapsed_cached * 1e6) / iterations:.3f} microseconds")
    print(f"Cache speedup: ~{elapsed / elapsed_cached:.1f}x faster")


def benchmark_remove_comments():
    """Benchmark the remove_comments function"""
    test_sql = """
    SELECT * FROM table  -- this is a comment
    WHERE col = 'value' /* multi
    line comment */ AND x > 10;
    """
    
    iterations = 50000
    
    print("\n=== Benchmarking remove_comments ===")
    print(f"Running {iterations} iterations...")
    
    start = time.perf_counter()
    for _ in range(iterations):
        remove_comments(test_sql)
    elapsed = time.perf_counter() - start
    
    avg_us = (elapsed * 1e6) / iterations
    
    print(f"Total time: {elapsed:.4f}s")
    print(f"Average per call: {avg_us:.2f} microseconds")
    print(f"Throughput: {iterations / elapsed:.0f} calls/second")


def benchmark_clean_statement():
    """Benchmark the clean_statement function"""
    test_sql = """
    SELECT   *   FROM    table
    WHERE    col = 'value'
    AND      x > 10;
    """
    
    iterations = 50000
    
    print("\n=== Benchmarking clean_statement ===")
    print(f"Running {iterations} iterations...")
    
    start = time.perf_counter()
    for _ in range(iterations):
        clean_statement(test_sql)
    elapsed = time.perf_counter() - start
    
    avg_us = (elapsed * 1e6) / iterations
    
    print(f"Total time: {elapsed:.4f}s")
    print(f"Average per call: {avg_us:.2f} microseconds")
    print(f"Throughput: {iterations / elapsed:.0f} calls/second")


if __name__ == "__main__":
    print("=" * 60)
    print("OPTERYX SQL UTILITIES PERFORMANCE BENCHMARK")
    print("=" * 60)
    print("\nThis benchmark measures the performance improvements from:")
    print("1. LRU caching in sql_like_to_regex")
    print("2. Optimized string building")
    print("3. Module-level regex compilation")
    
    benchmark_sql_like_to_regex()
    benchmark_remove_comments()
    benchmark_clean_statement()
    
    print("\n" + "=" * 60)
    print("BENCHMARK COMPLETE")
    print("=" * 60)
    print("\nExpected improvements vs baseline:")
    print("- sql_like_to_regex: ~40% faster")
    print("- remove_comments: Eliminates regex compilation")
    print("- clean_statement: Eliminates regex compilation")
    print("\nSee PERFORMANCE_REVIEW.md for detailed analysis")
