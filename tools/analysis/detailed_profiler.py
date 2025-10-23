#!/usr/bin/env python3
"""
Detailed Query Profiler for Opteryx

This tool uses Python's cProfile to identify bottlenecks in query execution.
"""

import argparse
import cProfile
import io
import os
import pstats
import sys
import time
from typing import Dict, List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import opteryx


def profile_query(query: str, sort_by: str = 'cumulative', limit: int = 30) -> Dict:
    """
    Profile a single query execution.
    
    Args:
        query: SQL query to profile
        sort_by: How to sort the profiling results
        limit: Number of top functions to display
        
    Returns:
        Dictionary with profiling data
    """
    print(f"\n{'='*80}")
    print(f"Profiling query: {query[:70]}...")
    print(f"{'='*80}\n")
    
    # Create profiler
    profiler = cProfile.Profile()
    
    # Profile the query execution
    start_time = time.perf_counter()
    profiler.enable()
    
    try:
        result = opteryx.query_to_arrow(query)
        row_count = len(result)
        col_count = len(result.schema)
    except Exception as e:
        profiler.disable()
        print(f"❌ Query failed: {e}")
        return {'error': str(e)}
    
    profiler.disable()
    end_time = time.perf_counter()
    
    execution_time = (end_time - start_time) * 1000  # ms
    
    # Get statistics
    stats_stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stats_stream)
    stats.strip_dirs()
    stats.sort_stats(sort_by)
    
    print(f"Query completed in {execution_time:.2f}ms")
    print(f"Returned {row_count} rows, {col_count} columns\n")
    
    print(f"Top {limit} functions by {sort_by} time:")
    print('-' * 80)
    stats.print_stats(limit)
    
    # Also print callers for the top 5 most time-consuming functions
    print(f"\nTop 10 functions with their callers:")
    print('-' * 80)
    stats.sort_stats('cumulative')
    stats.print_callers(10)
    
    return {
        'query': query,
        'execution_time_ms': execution_time,
        'row_count': row_count,
        'col_count': col_count,
        'stats': stats_stream.getvalue()
    }


def profile_operations():
    """Profile different types of operations to identify bottlenecks."""
    print("\n" + "="*80)
    print("DETAILED OPTERYX PROFILING")
    print(f"Version: {opteryx.__version__}")
    print("="*80)
    
    test_queries = [
        ("Simple COUNT", "SELECT COUNT(*) FROM $planets"),
        ("Simple SELECT", "SELECT * FROM $planets"),
        ("Simple WHERE", "SELECT * FROM $planets WHERE gravity > 10"),
        ("Simple aggregation", "SELECT AVG(gravity), MAX(mass), MIN(mass) FROM $planets"),
        ("GROUP BY", "SELECT name, COUNT(*) FROM $satellites GROUP BY name"),
        ("JOIN", "SELECT p.name, s.name FROM $planets p JOIN $satellites s ON p.id = s.planetId"),
        ("String operations", "SELECT UPPER(name), LOWER(name), LENGTH(name) FROM $planets"),
        ("ORDER BY", "SELECT * FROM $planets ORDER BY mass DESC"),
    ]
    
    results = []
    
    for name, query in test_queries:
        print(f"\n{'#'*80}")
        print(f"# Test: {name}")
        print(f"{'#'*80}")
        
        result = profile_query(query, sort_by='cumulative', limit=20)
        results.append((name, result))
        
        # Small delay between queries
        time.sleep(0.5)
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}\n")
    
    print(f"{'Operation':<30} {'Time (ms)':<15} {'Rows':<10} {'Cols'}")
    print('-' * 80)
    
    for name, result in results:
        if 'error' not in result:
            time_ms = f"{result['execution_time_ms']:.2f}"
            rows = result['row_count']
            cols = result['col_count']
            print(f"{name:<30} {time_ms:<15} {rows:<10} {cols}")
    
    print("\n" + "="*80)
    print("RECOMMENDATIONS")
    print("="*80 + "\n")
    
    # Analyze results
    slow_queries = [(name, r) for name, r in results 
                    if 'error' not in r and r['execution_time_ms'] > 100]
    
    if slow_queries:
        print("⚠️  Slow operations detected (>100ms):")
        for name, result in slow_queries:
            print(f"  • {name}: {result['execution_time_ms']:.2f}ms")
            print(f"    Consider investigating the profiling output above for bottlenecks")
    else:
        print("✅ All operations completed in reasonable time")
    
    print("\n📊 Performance Tips:")
    print("  • Look for high 'cumtime' (cumulative time) in the profiling output")
    print("  • Check for functions called many times ('ncalls' column)")
    print("  • Focus on non-library code for optimization opportunities")
    print("  • Compare with previous versions to identify regressions")


def compare_with_baseline():
    """Compare current performance with expected baseline."""
    print("\n" + "="*80)
    print("BASELINE COMPARISON")
    print("="*80 + "\n")
    
    # Expected baseline timings (in ms) for reference
    # These are rough estimates - adjust based on your environment
    baseline = {
        "Simple COUNT": 5.0,
        "Simple SELECT": 5.0,
        "Simple WHERE": 7.0,
        "Simple aggregation": 5.0,
        "GROUP BY": 10.0,
        "JOIN": 10.0,
        "String operations": 8.0,
        "ORDER BY": 6.0,
    }
    
    print("Running quick benchmark against baseline expectations...\n")
    
    queries = {
        "Simple COUNT": "SELECT COUNT(*) FROM $planets",
        "Simple SELECT": "SELECT * FROM $planets",
        "Simple WHERE": "SELECT * FROM $planets WHERE gravity > 10",
        "Simple aggregation": "SELECT AVG(gravity), MAX(mass), MIN(mass) FROM $planets",
        "GROUP BY": "SELECT name, COUNT(*) FROM $satellites GROUP BY name",
        "JOIN": "SELECT p.name, s.name FROM $planets p JOIN $satellites s ON p.id = s.planetId",
        "String operations": "SELECT UPPER(name), LOWER(name), LENGTH(name) FROM $planets",
        "ORDER BY": "SELECT * FROM $planets ORDER BY mass DESC",
    }
    
    regressions = []
    
    print(f"{'Operation':<30} {'Current':<15} {'Baseline':<15} {'Ratio'}")
    print('-' * 80)
    
    for name, query in queries.items():
        # Run query multiple times and take average
        times = []
        for _ in range(3):
            start = time.perf_counter()
            try:
                opteryx.query_to_arrow(query)
                elapsed = (time.perf_counter() - start) * 1000
                times.append(elapsed)
            except Exception as e:
                print(f"{name:<30} {'ERROR':<15} -")
                continue
        
        if times:
            avg_time = sum(times) / len(times)
            baseline_time = baseline.get(name, 10.0)
            ratio = avg_time / baseline_time
            
            status = ""
            if ratio > 3.0:
                status = " ⚠️ SLOW"
                regressions.append((name, ratio))
            elif ratio > 2.0:
                status = " ⚠️"
            
            print(f"{name:<30} {avg_time:>6.2f}ms{'':>6} {baseline_time:>6.2f}ms{'':>6} {ratio:>6.2f}x{status}")
    
    print("\n" + "="*80)
    
    if regressions:
        print("\n⚠️  PERFORMANCE REGRESSIONS DETECTED:\n")
        for name, ratio in regressions:
            print(f"  • {name}: {ratio:.1f}x slower than baseline")
        print("\nLikely causes:")
        print("  1. Recent code changes introducing inefficiencies")
        print("  2. Missing compilation of Cython extensions")
        print("  3. Changed default configuration")
        print("  4. Increased overhead in query processing pipeline")
        print("\nRecommendations:")
        print("  • Review recent commits for performance impact")
        print("  • Verify all Cython extensions are properly compiled")
        print("  • Use the detailed profiler above to identify specific bottlenecks")
        print("  • Compare with git history to find when regression was introduced")
    else:
        print("\n✅ Performance is within expected range")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Detailed Opteryx Query Profiler"
    )
    parser.add_argument(
        '--query', '-q',
        type=str,
        help='Specific query to profile'
    )
    parser.add_argument(
        '--sort',
        type=str,
        default='cumulative',
        choices=['cumulative', 'time', 'calls'],
        help='How to sort profiling results'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=30,
        help='Number of functions to display'
    )
    parser.add_argument(
        '--baseline',
        action='store_true',
        help='Compare against baseline expectations'
    )

    args = parser.parse_args()

    if args.query:
        # Profile a specific query
        profile_query(args.query, args.sort, args.limit)
    elif args.baseline:
        # Compare with baseline
        compare_with_baseline()
    else:
        # Run full profiling suite
        profile_operations()

    return 0


if __name__ == "__main__":
    sys.exit(main())
