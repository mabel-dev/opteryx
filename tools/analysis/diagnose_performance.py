#!/usr/bin/env python3
"""
Performance Diagnosis Tool

Identifies and reports on performance issues in Opteryx.
"""

import gc
import os
import sys
import time
from typing import List, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import opteryx


def test_cold_start_performance():
    """Test performance of first query (cold start)."""
    print("\n" + "="*80)
    print("COLD START PERFORMANCE TEST")
    print("="*80 + "\n")
    
    print("Testing first query execution (cold start)...")
    
    # Simple query
    query = "SELECT COUNT(*) FROM $planets"
    
    start = time.perf_counter()
    result = opteryx.query_to_arrow(query)
    cold_time = (time.perf_counter() - start) * 1000
    
    print(f"  Cold start: {cold_time:.2f}ms")
    
    # Warm queries
    warm_times = []
    for i in range(5):
        start = time.perf_counter()
        result = opteryx.query_to_arrow(query)
        warm_time = (time.perf_counter() - start) * 1000
        warm_times.append(warm_time)
    
    avg_warm = sum(warm_times) / len(warm_times)
    print(f"  Warm average: {avg_warm:.2f}ms")
    print(f"  Ratio: {cold_time/avg_warm:.1f}x")
    
    if cold_time / avg_warm > 10:
        print("\n⚠️  WARNING: Cold start is >10x slower than warm queries")
        print("   This suggests significant initialization or caching overhead")
        return cold_time, avg_warm, True
    else:
        print("\n✅ Cold start performance is reasonable")
        return cold_time, avg_warm, False


def test_repeated_query_performance():
    """Test if there are caching issues."""
    print("\n" + "="*80)
    print("REPEATED QUERY TEST")
    print("="*80 + "\n")
    
    query = "SELECT * FROM $planets WHERE gravity > 10"
    
    print("Testing query executed 10 times in sequence...")
    times = []
    
    for i in range(10):
        gc.collect()
        start = time.perf_counter()
        result = opteryx.query_to_arrow(query)
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)
        print(f"  Run {i+1:2d}: {elapsed:6.2f}ms")
    
    first = times[0]
    avg_rest = sum(times[1:]) / len(times[1:])
    
    print(f"\n  First run: {first:.2f}ms")
    print(f"  Average of remaining: {avg_rest:.2f}ms")
    print(f"  Ratio: {first/avg_rest:.1f}x")
    
    if first / avg_rest > 3:
        print("\n⚠️  First query significantly slower - likely initialization overhead")
        return True
    else:
        print("\n✅ Consistent performance across runs")
        return False


def test_different_operations():
    """Test performance of different SQL operations."""
    print("\n" + "="*80)
    print("OPERATION PERFORMANCE TEST")
    print("="*80 + "\n")
    
    operations = [
        ("COUNT", "SELECT COUNT(*) FROM $planets"),
        ("SELECT *", "SELECT * FROM $planets"),
        ("WHERE", "SELECT * FROM $planets WHERE gravity > 10"),
        ("AVG/MAX/MIN", "SELECT AVG(gravity), MAX(mass), MIN(mass) FROM $planets"),
        ("GROUP BY", "SELECT name, COUNT(*) FROM $satellites GROUP BY name"),
        ("JOIN", "SELECT p.name, s.name FROM $planets p JOIN $satellites s ON p.id = s.planetId LIMIT 10"),
        ("ORDER BY", "SELECT * FROM $planets ORDER BY mass DESC"),
        ("DISTINCT", "SELECT DISTINCT name FROM $planets"),
    ]
    
    print(f"{'Operation':<15} {'1st Run':<12} {'2nd Run':<12} {'3rd Run':<12} {'Avg 2-3':<12}")
    print("-" * 75)
    
    slow_ops = []
    
    for name, query in operations:
        times = []
        for i in range(3):
            gc.collect()
            start = time.perf_counter()
            try:
                result = opteryx.query_to_arrow(query)
                elapsed = (time.perf_counter() - start) * 1000
                times.append(elapsed)
            except Exception as e:
                print(f"{name:<15} ERROR: {str(e)[:40]}")
                break
        
        if len(times) == 3:
            avg_warm = (times[1] + times[2]) / 2
            print(f"{name:<15} {times[0]:>7.2f}ms   {times[1]:>7.2f}ms   "
                  f"{times[2]:>7.2f}ms   {avg_warm:>7.2f}ms")
            
            if avg_warm > 50:
                slow_ops.append((name, avg_warm))
    
    if slow_ops:
        print(f"\n⚠️  Slow operations (>50ms warm):")
        for name, time_ms in slow_ops:
            print(f"    • {name}: {time_ms:.2f}ms")
        return True
    else:
        print(f"\n✅ All operations performing well")
        return False


def test_data_size_scaling():
    """Test how performance scales with data size."""
    print("\n" + "="*80)
    print("DATA SIZE SCALING TEST")
    print("="*80 + "\n")
    
    # Test with different LIMIT sizes
    limits = [1, 10, 100]
    base_query = "SELECT * FROM $satellites LIMIT "
    
    print("Testing query performance with different result sizes...")
    print(f"{'Rows':<10} {'Time (ms)':<15} {'Time/Row (ms)'}")
    print("-" * 50)
    
    times_per_row = []
    
    for limit in limits:
        query = base_query + str(limit)
        
        # Warm up
        opteryx.query_to_arrow(query)
        
        # Measure
        measurements = []
        for _ in range(3):
            gc.collect()
            start = time.perf_counter()
            result = opteryx.query_to_arrow(query)
            elapsed = (time.perf_counter() - start) * 1000
            measurements.append(elapsed)
        
        avg_time = sum(measurements) / len(measurements)
        time_per_row = avg_time / limit if limit > 0 else 0
        times_per_row.append(time_per_row)
        
        print(f"{limit:<10} {avg_time:>10.2f}   {time_per_row:>10.4f}")
    
    # Check if scaling is roughly linear
    if len(times_per_row) >= 2:
        ratio = times_per_row[-1] / times_per_row[0]
        if ratio > 2:
            print(f"\n⚠️  Non-linear scaling detected (ratio: {ratio:.1f}x)")
            print("    Performance degrades with larger result sets")
            return True
        else:
            print(f"\n✅ Scaling is roughly linear (ratio: {ratio:.1f}x)")
            return False
    
    return False


def diagnose_issues():
    """Run all diagnostic tests and provide recommendations."""
    print("\n" + "#"*80)
    print("# OPTERYX PERFORMANCE DIAGNOSIS")
    print(f"# Version: {opteryx.__version__}")
    print("#"*80)
    
    issues = []
    
    # Run tests
    cold_time, warm_time, has_cold_start_issue = test_cold_start_performance()
    if has_cold_start_issue:
        issues.append("cold_start")
    
    has_repeated_issue = test_repeated_query_performance()
    if has_repeated_issue:
        issues.append("repeated_query")
    
    has_slow_ops = test_different_operations()
    if has_slow_ops:
        issues.append("slow_operations")
    
    has_scaling_issue = test_data_size_scaling()
    if has_scaling_issue:
        issues.append("scaling")
    
    # Summary and recommendations
    print("\n" + "="*80)
    print("DIAGNOSIS SUMMARY")
    print("="*80 + "\n")
    
    if not issues:
        print("✅ No significant performance issues detected!")
        print("\nOverall performance appears normal.")
        return
    
    print(f"⚠️  {len(issues)} issue(s) detected:\n")
    
    if "cold_start" in issues:
        print("1. COLD START OVERHEAD")
        print(f"   First query: {cold_time:.2f}ms")
        print(f"   Warm queries: {warm_time:.2f}ms")
        print(f"   Ratio: {cold_time/warm_time:.1f}x\n")
        print("   Likely causes:")
        print("   • Heavy module initialization")
        print("   • Lazy loading of components")
        print("   • First-time compilation of query patterns")
        print("   • Cache warming overhead\n")
        print("   Recommendations:")
        print("   • Investigate module initialization code")
        print("   • Consider pre-warming caches")
        print("   • Profile import time: python -X importtime -c 'import opteryx'")
        print()
    
    if "repeated_query" in issues:
        print("2. REPEATED QUERY INCONSISTENCY")
        print("   First execution of identical queries slower than subsequent ones\n")
        print("   Likely causes:")
        print("   • Query plan caching not working effectively")
        print("   • Per-query initialization overhead")
        print("   • Metadata loading on first access\n")
        print("   Recommendations:")
        print("   • Review query plan caching logic")
        print("   • Check for unnecessary reinitialization")
        print()
    
    if "slow_operations" in issues:
        print("3. SLOW OPERATIONS")
        print("   Some operations are slower than expected\n")
        print("   Recommendations:")
        print("   • Use detailed_profiler.py to identify bottlenecks")
        print("   • Check if Cython extensions are compiled and used")
        print("   • Compare with previous versions")
        print()
    
    if "scaling" in issues:
        print("4. SCALING ISSUES")
        print("   Performance degrades non-linearly with data size\n")
        print("   Likely causes:")
        print("   • Inefficient algorithms (O(n²) instead of O(n))")
        print("   • Memory allocation issues")
        print("   • Inefficient data structure usage\n")
        print("   Recommendations:")
        print("   • Profile with larger datasets")
        print("   • Review algorithms for complexity")
        print()
    
    print("="*80)
    print("NEXT STEPS")
    print("="*80 + "\n")
    print("1. Run detailed profiler:")
    print("   python tools/analysis/detailed_profiler.py")
    print()
    print("2. Compare with previous version:")
    print("   git checkout <previous-version>")
    print("   python tools/analysis/diagnose_performance.py")
    print()
    print("3. Check compiled extensions:")
    print("   find opteryx/compiled -name '*.so' | wc -l")
    print()
    print("4. Review recent commits:")
    print("   git log --oneline -20")


if __name__ == "__main__":
    diagnose_issues()
