#!/usr/bin/env python3
"""
Performance Comparison Tool for Opteryx

This tool measures and analyzes the performance of Opteryx queries,
helping identify performance regressions and bottlenecks.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

import psutil

# Add opteryx to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import opteryx


class PerformanceAnalyzer:
    """Analyzes Opteryx query performance."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.process = psutil.Process(os.getpid())
        self.results: List[Dict[str, Any]] = []

    def measure_query(
        self, query: str, name: str, iterations: int = 3
    ) -> Dict[str, Any]:
        """
        Measure query execution time and resource usage.
        
        Args:
            query: SQL query to execute
            name: Descriptive name for the query
            iterations: Number of times to run the query
            
        Returns:
            Dictionary with performance metrics
        """
        times = []
        memory_deltas = []
        
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"Testing: {name}")
            print(f"Query: {query[:100]}...")
            print(f"{'='*60}")

        for i in range(iterations):
            # Force garbage collection before measurement
            import gc
            gc.collect()
            
            # Capture initial state
            start_time = time.perf_counter()
            start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
            
            try:
                # Execute query
                result = opteryx.query_to_arrow(query)
                row_count = len(result)
                col_count = len(result.schema)
                
                # Capture end state
                end_time = time.perf_counter()
                end_memory = self.process.memory_info().rss / 1024 / 1024  # MB
                
                execution_time = (end_time - start_time) * 1000  # Convert to ms
                memory_delta = end_memory - start_memory
                
                times.append(execution_time)
                memory_deltas.append(memory_delta)
                
                if self.verbose:
                    print(f"  Iteration {i+1}: {execution_time:.2f}ms, "
                          f"Memory Δ: {memory_delta:+.1f}MB, "
                          f"Rows: {row_count}, Cols: {col_count}")
                    
            except Exception as e:
                print(f"  ❌ Error in iteration {i+1}: {e}")
                return {
                    'name': name,
                    'query': query,
                    'error': str(e),
                    'status': 'failed'
                }

        # Calculate statistics
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        avg_memory = sum(memory_deltas) / len(memory_deltas)

        result_dict = {
            'name': name,
            'query': query,
            'status': 'success',
            'iterations': iterations,
            'avg_time_ms': round(avg_time, 2),
            'min_time_ms': round(min_time, 2),
            'max_time_ms': round(max_time, 2),
            'avg_memory_delta_mb': round(avg_memory, 2),
            'row_count': row_count,
            'col_count': col_count,
        }

        if self.verbose:
            print(f"\n  Summary:")
            print(f"    Average: {avg_time:.2f}ms")
            print(f"    Min: {min_time:.2f}ms")
            print(f"    Max: {max_time:.2f}ms")
            print(f"    Avg Memory: {avg_memory:+.2f}MB")

        self.results.append(result_dict)
        return result_dict

    def run_benchmark_suite(self) -> List[Dict[str, Any]]:
        """
        Run a comprehensive benchmark suite covering various query patterns.
        
        Returns:
            List of performance results
        """
        print(f"\n{'#'*70}")
        print(f"# Opteryx Performance Benchmark Suite")
        print(f"# Version: {opteryx.__version__}")
        print(f"# Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#'*70}\n")

        # Define benchmark queries
        benchmarks = [
            # Simple queries
            (
                "Simple COUNT",
                "SELECT COUNT(*) FROM $planets"
            ),
            (
                "Simple SELECT with WHERE",
                "SELECT * FROM $planets WHERE gravity > 10"
            ),
            (
                "Simple aggregation",
                "SELECT AVG(gravity), MAX(mass) FROM $planets"
            ),
            
            # GROUP BY queries
            (
                "GROUP BY with aggregation",
                "SELECT name, COUNT(*) FROM $satellites GROUP BY name"
            ),
            (
                "Multiple GROUP BY columns",
                "SELECT planet, COUNT(*) as cnt FROM $satellites GROUP BY planet ORDER BY cnt DESC"
            ),
            
            # JOIN queries
            (
                "Simple JOIN",
                "SELECT p.name, s.name FROM $planets p JOIN $satellites s ON p.id = s.planetId"
            ),
            
            # String operations
            (
                "String functions",
                "SELECT UPPER(name), LENGTH(name) FROM $planets WHERE name LIKE 'M%'"
            ),
            
            # Sorting
            (
                "ORDER BY single column",
                "SELECT * FROM $planets ORDER BY mass DESC"
            ),
            (
                "ORDER BY multiple columns",
                "SELECT * FROM $planets ORDER BY gravity DESC, mass ASC"
            ),
            
            # DISTINCT
            (
                "DISTINCT count",
                "SELECT COUNT(DISTINCT planet) FROM $satellites"
            ),
        ]

        print("Running benchmark queries...\n")
        for name, query in benchmarks:
            try:
                self.measure_query(query, name, iterations=5)
            except Exception as e:
                print(f"❌ Failed to run {name}: {e}")
                self.results.append({
                    'name': name,
                    'query': query,
                    'error': str(e),
                    'status': 'failed'
                })

        return self.results

    def print_summary(self):
        """Print a summary of benchmark results."""
        print(f"\n{'#'*70}")
        print(f"# Performance Summary")
        print(f"{'#'*70}\n")

        # Calculate overall statistics
        successful = [r for r in self.results if r.get('status') == 'success']
        failed = [r for r in self.results if r.get('status') == 'failed']

        print(f"Total queries: {len(self.results)}")
        print(f"Successful: {len(successful)}")
        print(f"Failed: {len(failed)}")

        if successful:
            print(f"\n{'Query':<40} {'Avg Time':<12} {'Min Time':<12} {'Max Time':<12}")
            print('-' * 76)
            
            for result in successful:
                name = result['name'][:38]
                avg = f"{result['avg_time_ms']:.2f}ms"
                min_t = f"{result['min_time_ms']:.2f}ms"
                max_t = f"{result['max_time_ms']:.2f}ms"
                print(f"{name:<40} {avg:<12} {min_t:<12} {max_t:<12}")

            # Identify slow queries (>1000ms)
            slow_queries = [r for r in successful if r['avg_time_ms'] > 1000]
            if slow_queries:
                print(f"\n⚠️  Slow queries (>1000ms):")
                for r in slow_queries:
                    print(f"  - {r['name']}: {r['avg_time_ms']:.2f}ms")

            # Calculate percentiles
            times = sorted([r['avg_time_ms'] for r in successful])
            total_time = sum(times)
            print(f"\nTotal execution time: {total_time:.2f}ms")
            print(f"Average query time: {total_time/len(times):.2f}ms")
            
            if len(times) >= 2:
                print(f"Median query time: {times[len(times)//2]:.2f}ms")
                print(f"Fastest query: {times[0]:.2f}ms")
                print(f"Slowest query: {times[-1]:.2f}ms")

        if failed:
            print(f"\n❌ Failed queries:")
            for result in failed:
                print(f"  - {result['name']}: {result.get('error', 'Unknown error')}")

    def save_results(self, filename: str):
        """Save results to a JSON file."""
        output_data = {
            'version': opteryx.__version__,
            'timestamp': datetime.now().isoformat(),
            'total_queries': len(self.results),
            'successful': len([r for r in self.results if r.get('status') == 'success']),
            'failed': len([r for r in self.results if r.get('status') == 'failed']),
            'results': self.results
        }
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\n✅ Results saved to: {filename}")

    def analyze_performance_issues(self):
        """Analyze results to identify potential performance issues."""
        print(f"\n{'#'*70}")
        print(f"# Performance Analysis")
        print(f"{'#'*70}\n")

        successful = [r for r in self.results if r.get('status') == 'success']
        
        if not successful:
            print("No successful queries to analyze.")
            return

        # Identify patterns
        issues = []

        # Check for queries with high memory usage
        high_memory = [r for r in successful if r.get('avg_memory_delta_mb', 0) > 50]
        if high_memory:
            issues.append(("High memory usage (>50MB)", high_memory))

        # Check for slow aggregations
        slow_group_by = [r for r in successful 
                        if 'GROUP BY' in r['query'].upper() and r['avg_time_ms'] > 500]
        if slow_group_by:
            issues.append(("Slow GROUP BY operations (>500ms)", slow_group_by))

        # Check for slow JOINs
        slow_joins = [r for r in successful 
                     if 'JOIN' in r['query'].upper() and r['avg_time_ms'] > 500]
        if slow_joins:
            issues.append(("Slow JOIN operations (>500ms)", slow_joins))

        # Check for high variability (max/min ratio > 2)
        high_variance = [r for r in successful 
                        if r['max_time_ms'] / r['min_time_ms'] > 2.0]
        if high_variance:
            issues.append(("High execution time variance (max/min > 2)", high_variance))

        if issues:
            print("⚠️  Potential performance issues detected:\n")
            for issue_name, queries in issues:
                print(f"  {issue_name}:")
                for q in queries:
                    print(f"    - {q['name']}: {q['avg_time_ms']:.2f}ms")
                print()
        else:
            print("✅ No significant performance issues detected.")

        # Provide recommendations
        print("\n📋 Recommendations:")
        if high_memory:
            print("  • High memory usage detected - consider optimizing data structures or batch processing")
        if slow_group_by:
            print("  • Slow GROUP BY operations - check if grouping columns are properly indexed")
        if slow_joins:
            print("  • Slow JOIN operations - verify join conditions and data sizes")
        if high_variance:
            print("  • High execution variance - may indicate caching effects or external factors")
        
        if not issues:
            print("  • Performance appears stable across all queries")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Opteryx Performance Analysis Tool"
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='performance_results.json',
        help='Output file for results (default: performance_results.json)'
    )
    parser.add_argument(
        '--iterations', '-i',
        type=int,
        default=5,
        help='Number of iterations per query (default: 5)'
    )

    args = parser.parse_args()

    # Create analyzer
    analyzer = PerformanceAnalyzer(verbose=args.verbose)

    # Run benchmark suite
    try:
        analyzer.run_benchmark_suite()
        analyzer.print_summary()
        analyzer.analyze_performance_issues()
        analyzer.save_results(args.output)
    except KeyboardInterrupt:
        print("\n\n⚠️  Benchmark interrupted by user")
        if analyzer.results:
            analyzer.print_summary()
    except Exception as e:
        print(f"\n❌ Error during benchmark: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
