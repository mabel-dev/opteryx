#!/usr/bin/env python3
"""
Query Performance Profiler

This tool helps analyze query performance in Opteryx by providing
detailed timing information, memory usage, and optimization suggestions.
"""

import argparse
import os
import time
from contextlib import contextmanager
from typing import Any
from typing import Dict

import psutil
import pyarrow

import opteryx


class QueryProfiler:
    """
    Profiles Opteryx query execution to identify performance bottlenecks.
    """
    
    def __init__(self):
        self.metrics = {}
        self.process = psutil.Process(os.getpid())
    
    @contextmanager
    def profile_query(self, query_name: str):
        """Context manager to profile a query execution."""
        print(f"\n--- Profiling: {query_name} ---")
        
        # Capture initial state
        start_time = time.perf_counter()
        start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        start_cpu_percent = self.process.cpu_percent()
        
        try:
            yield
        finally:
            # Capture final state
            end_time = time.perf_counter()
            end_memory = self.process.memory_info().rss / 1024 / 1024  # MB
            end_cpu_percent = self.process.cpu_percent()
            
            # Calculate metrics
            execution_time = end_time - start_time
            memory_delta = end_memory - start_memory
            
            # Store metrics
            self.metrics[query_name] = {
                'execution_time': execution_time,
                'memory_start': start_memory,
                'memory_end': end_memory,
                'memory_delta': memory_delta,
                'cpu_percent': end_cpu_percent
            }
            
            # Print immediate results
            print(f"Execution time: {execution_time:.4f} seconds")
            print(f"Memory usage: {start_memory:.1f} MB → {end_memory:.1f} MB (Δ{memory_delta:+.1f} MB)")
            print(f"CPU usage: {end_cpu_percent:.1f}%")
    
    def run_performance_benchmark(self):
        """Run a series of benchmark queries to test performance."""
        print("=== Opteryx Performance Benchmark ===")
        
        # Create sample data
        large_dataset = []
        for i in range(10000):
            large_dataset.append({
                'id': i,
                'value': i * 2,
                'category': f'cat_{i % 10}',
                'timestamp': f'2024-01-{(i % 30) + 1:02d}',
                'amount': round((i * 3.14) % 1000, 2)
            })
        large_dataset = pyarrow.Table.from_pylist(large_dataset)
        opteryx.register_arrow("benchmark_data", large_dataset)
        
        # Benchmark 1: Simple SELECT
        with self.profile_query("Simple SELECT"):
            result = opteryx.query("SELECT COUNT(*) FROM benchmark_data")
            print(f"Result: {result.arrow().to_pylist()}")
        
        # Benchmark 2: Filtering
        with self.profile_query("Filtering"):
            result = opteryx.query("SELECT * FROM benchmark_data WHERE value > 5000 LIMIT 100")
            print(f"Rows returned: {len(result.arrow())}")
        
        # Benchmark 3: Aggregation
        with self.profile_query("Aggregation"):
            result = opteryx.query("""
                SELECT 
                    category,
                    COUNT(*) as count,
                    AVG(amount) as avg_amount,
                    SUM(value) as total_value
                FROM benchmark_data
                GROUP BY category
                ORDER BY total_value DESC
            """)
            print(f"Categories: {len(result.arrow())}")
        
        # Benchmark 4: Complex query
        with self.profile_query("Complex Query"):
            result = opteryx.query("""
                SELECT 
                    category,
                    EXTRACT(day FROM timestamp) as day,
                    COUNT(*) as daily_count,
                    AVG(amount) as avg_daily_amount
                FROM benchmark_data
                WHERE amount BETWEEN 100 AND 800
                GROUP BY category, EXTRACT(day FROM timestamp)
                HAVING COUNT(*) > 10
                ORDER BY category, day
            """)
            print(f"Result rows: {len(result.arrow())}")
    
    def analyze_query(self, sql: str, iterations: int = 3) -> Dict[str, Any]:
        """
        Analyze a specific query with multiple iterations.
        
        Args:
            sql: SQL query to analyze
            iterations: Number of times to run the query
            
        Returns:
            Analysis results dictionary
        """
        print(f"\n=== Analyzing Query ({iterations} iterations) ===")
        print(f"SQL: {sql}")
        
        execution_times = []
        memory_usages = []
        
        for i in range(iterations):
            with self.profile_query(f"Iteration {i+1}"):
                result = opteryx.query(sql)
                # Force materialization
                row_count = len(result.arrow())
                
            execution_times.append(self.metrics[f"Iteration {i+1}"]['execution_time'])
            memory_usages.append(self.metrics[f"Iteration {i+1}"]['memory_delta'])
        
        # Calculate statistics
        avg_time = sum(execution_times) / len(execution_times)
        min_time = min(execution_times)
        max_time = max(execution_times)
        
        avg_memory = sum(memory_usages) / len(memory_usages)
        
        analysis = {
            'avg_execution_time': avg_time,
            'min_execution_time': min_time,
            'max_execution_time': max_time,
            'time_variance': max_time - min_time,
            'avg_memory_delta': avg_memory,
            'row_count': row_count,
            'rows_per_second': row_count / avg_time if avg_time > 0 else 0
        }
        
        print(f"\n--- Analysis Results ---")
        print(f"Average execution time: {avg_time:.4f}s")
        print(f"Time range: {min_time:.4f}s - {max_time:.4f}s")
        print(f"Performance: {analysis['rows_per_second']:,.0f} rows/second")
        print(f"Average memory delta: {avg_memory:+.1f} MB")
        
        return analysis
    
    def suggest_optimizations(self, sql: str, analysis: Dict[str, Any]):
        """Provide optimization suggestions based on query analysis."""
        print(f"\n--- Optimization Suggestions ---")
        
        suggestions = []
        
        # Time-based suggestions
        if analysis['avg_execution_time'] > 1.0:
            suggestions.append("Consider adding column selection to reduce data transfer")
            suggestions.append("Check if indexes or partitioning could help")
        
        # Memory-based suggestions  
        if analysis['avg_memory_delta'] > 100:
            suggestions.append("High memory usage detected - consider streaming or chunking")
            suggestions.append("Review if all columns are necessary")
        
        # Performance-based suggestions
        if analysis['rows_per_second'] < 10000:
            suggestions.append("Performance seems low - check for full table scans")
            suggestions.append("Consider if predicate pushdown is working effectively")
        
        # SQL-specific suggestions
        sql_lower = sql.lower()
        if 'select *' in sql_lower:
            suggestions.append("Using SELECT * - specify only needed columns")
        
        if 'order by' in sql_lower and 'limit' not in sql_lower:
            suggestions.append("ORDER BY without LIMIT may be inefficient for large datasets")
        
        if suggestions:
            for i, suggestion in enumerate(suggestions, 1):
                print(f"{i}. {suggestion}")
        else:
            print("No specific optimization suggestions - query looks efficient!")
    
    def print_summary(self):
        """Print a summary of all profiled queries."""
        if not self.metrics:
            print("No queries have been profiled yet.")
            return
        
        print(f"\n=== Profiling Summary ({len(self.metrics)} queries) ===")
        print(f"{'Query':<20} {'Time (s)':<10} {'Memory (MB)':<12} {'CPU %':<8}")
        print("-" * 52)
        
        for query_name, metrics in self.metrics.items():
            print(f"{query_name:<20} {metrics['execution_time']:<10.4f} "
                  f"{metrics['memory_delta']:+<12.1f} {metrics['cpu_percent']:<8.1f}")


def main():
    """Main function to run the query profiler."""
    parser = argparse.ArgumentParser(description="Opteryx Query Performance Profiler")
    parser.add_argument("--query", "-q", help="SQL query to analyze")
    parser.add_argument("--iterations", "-i", type=int, default=3, 
                       help="Number of iterations for analysis")
    parser.add_argument("--benchmark", "-b", action="store_true",
                       help="Run performance benchmark")
    
    args = parser.parse_args()
    
    profiler = QueryProfiler()
    
    try:
        if args.benchmark:
            profiler.run_performance_benchmark()
        
        if args.query:
            analysis = profiler.analyze_query(args.query, args.iterations)
            profiler.suggest_optimizations(args.query, analysis)
        
        if not args.query and not args.benchmark:
            # Default: run benchmark
            profiler.run_performance_benchmark()
        
        profiler.print_summary()
        
    except Exception as e:
        print(f"❌ Error during profiling: {e}")


if __name__ == "__main__":
    main()
