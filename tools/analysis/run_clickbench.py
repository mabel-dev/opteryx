#!/usr/bin/env python3
"""
ClickBench Performance Runner

Runs the ClickBench benchmark suite and measures warm query performance.
This addresses the concern that warm queries may also be slower than expected.
"""

import gc
import os
import sys
import time
from typing import List, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import opteryx


# ClickBench queries from the test suite
CLICKBENCH_QUERIES = [
    ("Q01", "SELECT COUNT(*) FROM testdata.clickbench_tiny;"),
    ("Q02", "SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE AdvEngineID <> 0;"),
    ("Q03", "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM testdata.clickbench_tiny;"),
    ("Q04", "SELECT AVG(UserID) FROM testdata.clickbench_tiny;"),
    ("Q05", "SELECT COUNT(DISTINCT UserID) FROM testdata.clickbench_tiny;"),
    ("Q06", "SELECT COUNT(DISTINCT SearchPhrase) FROM testdata.clickbench_tiny;"),
    ("Q07", "SELECT MIN(EventDate), MAX(EventDate) FROM testdata.clickbench_tiny;"),
    ("Q08", "SELECT AdvEngineID, COUNT(*) FROM testdata.clickbench_tiny WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;"),
    ("Q09", "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM testdata.clickbench_tiny GROUP BY RegionID ORDER BY u DESC LIMIT 10;"),
    ("Q10", "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM testdata.clickbench_tiny GROUP BY RegionID ORDER BY c DESC LIMIT 10;"),
    ("Q11", "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM testdata.clickbench_tiny WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;"),
    ("Q12", "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM testdata.clickbench_tiny WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;"),
    ("Q13", "SELECT SearchPhrase, COUNT(*) AS c FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"),
    ("Q14", "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;"),
    ("Q15", "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;"),
    ("Q16", "SELECT UserID, COUNT(*) FROM testdata.clickbench_tiny GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;"),
    ("Q17", "SELECT UserID, SearchPhrase, COUNT(*) FROM testdata.clickbench_tiny GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;"),
    ("Q18", "SELECT UserID, SearchPhrase, COUNT(*) FROM testdata.clickbench_tiny GROUP BY UserID, SearchPhrase LIMIT 10;"),
    ("Q20", "SELECT UserID FROM testdata.clickbench_tiny WHERE UserID = 435090932899640449;"),
    ("Q21", "SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE URL LIKE '%google%';"),
]


def run_clickbench_benchmark(iterations: int = 3) -> List[Tuple[str, List[float]]]:
    """
    Run ClickBench queries multiple times to measure warm performance.
    
    Args:
        iterations: Number of times to run each query
        
    Returns:
        List of (query_name, times_list) tuples
    """
    print(f"\n{'='*80}")
    print("CLICKBENCH WARM PERFORMANCE BENCHMARK")
    print(f"Version: {opteryx.__version__}")
    print(f"Iterations per query: {iterations}")
    print(f"{'='*80}\n")
    
    # Do a cold start query first
    print("Warming up...")
    start = time.perf_counter()
    try:
        opteryx.query_to_arrow("SELECT 1")
        cold_time = (time.perf_counter() - start) * 1000
        print(f"Cold start: {cold_time:.2f}ms\n")
    except Exception as e:
        print(f"Cold start failed: {e}\n")
    
    results = []
    
    print(f"{'Query':<8} {'Run 1':<12} {'Run 2':<12} {'Run 3':<12} {'Avg':<12} {'Min':<12} {'Max':<12}")
    print("-" * 80)
    
    for name, query in CLICKBENCH_QUERIES:
        times = []
        failed = False
        
        for i in range(iterations):
            gc.collect()
            start = time.perf_counter()
            
            try:
                result = opteryx.query_to_arrow(query)
                elapsed = (time.perf_counter() - start) * 1000
                times.append(elapsed)
            except Exception as e:
                print(f"{name:<8} ERROR: {str(e)[:60]}")
                failed = True
                break
        
        if not failed and times:
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            
            # Format times
            run_times = [f"{t:.2f}ms" for t in times]
            while len(run_times) < 3:
                run_times.append("-")
            
            print(f"{name:<8} {run_times[0]:<12} {run_times[1]:<12} {run_times[2]:<12} "
                  f"{avg_time:>7.2f}ms   {min_time:>7.2f}ms   {max_time:>7.2f}ms")
            
            results.append((name, times))
    
    return results


def analyze_results(results: List[Tuple[str, List[float]]]):
    """Analyze benchmark results and identify slow queries."""
    print(f"\n{'='*80}")
    print("ANALYSIS")
    print(f"{'='*80}\n")
    
    if not results:
        print("No results to analyze.")
        return
    
    # Calculate statistics
    all_times = []
    for name, times in results:
        all_times.extend(times)
    
    avg_overall = sum(all_times) / len(all_times)
    
    print(f"Total queries executed: {len(results)}")
    print(f"Total measurements: {len(all_times)}")
    print(f"Overall average time: {avg_overall:.2f}ms")
    
    # Find slow queries (>1000ms)
    very_slow = []
    slow = []
    medium = []
    
    for name, times in results:
        avg_time = sum(times) / len(times)
        if avg_time > 1000:
            very_slow.append((name, avg_time))
        elif avg_time > 500:
            slow.append((name, avg_time))
        elif avg_time > 100:
            medium.append((name, avg_time))
    
    if very_slow:
        print(f"\n⚠️  VERY SLOW queries (>1000ms):")
        for name, avg_time in sorted(very_slow, key=lambda x: x[1], reverse=True):
            print(f"  {name}: {avg_time:.2f}ms")
    
    if slow:
        print(f"\n⚠️  Slow queries (>500ms):")
        for name, avg_time in sorted(slow, key=lambda x: x[1], reverse=True):
            print(f"  {name}: {avg_time:.2f}ms")
    
    if medium:
        print(f"\n⚠️  Moderate queries (>100ms):")
        for name, avg_time in sorted(medium, key=lambda x: x[1], reverse=True):
            print(f"  {name}: {avg_time:.2f}ms")
    
    if not (very_slow or slow or medium):
        print("\n✅ All queries completed in good time (<100ms)")
    
    # Check for high variance
    high_variance = []
    for name, times in results:
        if len(times) >= 2:
            min_t = min(times)
            max_t = max(times)
            if min_t > 0 and max_t / min_t > 2.0:
                high_variance.append((name, min_t, max_t, max_t / min_t))
    
    if high_variance:
        print(f"\n⚠️  High variance queries (max/min > 2x):")
        for name, min_t, max_t, ratio in sorted(high_variance, key=lambda x: x[3], reverse=True):
            print(f"  {name}: {min_t:.2f}ms - {max_t:.2f}ms (ratio: {ratio:.1f}x)")


def main():
    """Main entry point."""
    print("\nClickBench Performance Test")
    print("This tests WARM query performance on real-world queries")
    print("=" * 80)
    
    try:
        results = run_clickbench_benchmark(iterations=3)
        analyze_results(results)
        
        print(f"\n{'='*80}")
        print("CONCLUSION")
        print(f"{'='*80}\n")
        print("This benchmark tests warm query performance on actual ClickBench queries.")
        print("If queries are slow even when warm, there may be algorithmic issues")
        print("beyond the cold start overhead identified in the initial analysis.")
        print()
        
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
        return 1
    except Exception as e:
        print(f"\n\nError running benchmark: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
