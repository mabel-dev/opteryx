#!/usr/bin/env python3
"""
Version Comparison Tool

Helps compare performance between different Opteryx versions or commits.
Run this script on different versions to collect data, then compare the results.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from typing import Dict, List, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import opteryx


def get_git_info() -> Dict[str, str]:
    """Get current git commit information."""
    try:
        commit = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            cwd=os.path.dirname(__file__)
        ).decode().strip()
        
        branch = subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            cwd=os.path.dirname(__file__)
        ).decode().strip()
        
        # Get commit message
        message = subprocess.check_output(
            ['git', 'log', '-1', '--pretty=%B'],
            cwd=os.path.dirname(__file__)
        ).decode().strip()
        
        return {
            'commit': commit[:12],
            'branch': branch,
            'message': message.split('\n')[0][:80]
        }
    except Exception as e:
        return {
            'commit': 'unknown',
            'branch': 'unknown',
            'message': str(e)
        }


def run_benchmark_suite() -> Dict:
    """Run a standardized benchmark suite."""
    print("\nRunning benchmark suite...")
    
    benchmarks = [
        ("count", "SELECT COUNT(*) FROM $planets"),
        ("select_all", "SELECT * FROM $planets"),
        ("where", "SELECT * FROM $planets WHERE gravity > 10"),
        ("aggregation", "SELECT AVG(gravity), MAX(mass), MIN(mass) FROM $planets"),
        ("group_by", "SELECT name, COUNT(*) FROM $satellites GROUP BY name"),
        ("join", "SELECT p.name, s.name FROM $planets p JOIN $satellites s ON p.id = s.planetId LIMIT 10"),
        ("order_by", "SELECT * FROM $planets ORDER BY mass DESC"),
    ]
    
    results = {}
    
    # Test cold start
    import gc
    gc.collect()
    
    start = time.perf_counter()
    opteryx.query_to_arrow("SELECT 1")
    cold_start = (time.perf_counter() - start) * 1000
    results['cold_start_ms'] = round(cold_start, 2)
    
    print(f"  Cold start: {cold_start:.2f}ms")
    
    # Test each benchmark
    for name, query in benchmarks:
        times = []
        for i in range(5):
            gc.collect()
            start = time.perf_counter()
            try:
                result = opteryx.query_to_arrow(query)
                elapsed = (time.perf_counter() - start) * 1000
                times.append(elapsed)
            except Exception as e:
                print(f"  {name}: ERROR - {e}")
                times = None
                break
        
        if times:
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            results[name] = {
                'avg_ms': round(avg_time, 2),
                'min_ms': round(min_time, 2),
                'max_ms': round(max_time, 2),
            }
            print(f"  {name}: {avg_time:.2f}ms")
        else:
            results[name] = {'error': 'Failed'}
    
    return results


def save_benchmark_results(output_file: str):
    """Run benchmarks and save results to file."""
    git_info = get_git_info()
    
    print("="*70)
    print("OPTERYX BENCHMARK")
    print("="*70)
    print(f"Version: {opteryx.__version__}")
    print(f"Commit: {git_info['commit']}")
    print(f"Branch: {git_info['branch']}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    results = run_benchmark_suite()
    
    data = {
        'version': opteryx.__version__,
        'git': git_info,
        'timestamp': datetime.now().isoformat(),
        'benchmarks': results
    }
    
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\n✅ Results saved to: {output_file}")
    return data


def compare_results(file1: str, file2: str):
    """Compare two benchmark result files."""
    with open(file1, 'r') as f:
        data1 = json.load(f)
    
    with open(file2, 'r') as f:
        data2 = json.load(f)
    
    print("\n" + "="*70)
    print("PERFORMANCE COMPARISON")
    print("="*70)
    
    print(f"\nVersion 1: {data1['version']}")
    print(f"  Commit: {data1['git']['commit']}")
    print(f"  Date: {data1['timestamp']}")
    
    print(f"\nVersion 2: {data2['version']}")
    print(f"  Commit: {data2['git']['commit']}")
    print(f"  Date: {data2['timestamp']}")
    
    print("\n" + "="*70)
    print("RESULTS")
    print("="*70)
    
    # Compare cold start
    cold1 = data1['benchmarks'].get('cold_start_ms', 0)
    cold2 = data2['benchmarks'].get('cold_start_ms', 0)
    
    print(f"\nCold Start:")
    print(f"  Version 1: {cold1:.2f}ms")
    print(f"  Version 2: {cold2:.2f}ms")
    if cold1 > 0:
        ratio = cold2 / cold1
        change = ((cold2 - cold1) / cold1) * 100
        status = "📈" if ratio > 1.1 else "📉" if ratio < 0.9 else "➡️"
        print(f"  Change: {change:+.1f}% {status}")
    
    # Compare benchmarks
    print(f"\n{'Benchmark':<20} {'V1 (ms)':<12} {'V2 (ms)':<12} {'Change':<12} {'Ratio'}")
    print("-" * 70)
    
    regressions = []
    improvements = []
    
    benchmarks = set(data1['benchmarks'].keys()) & set(data2['benchmarks'].keys())
    benchmarks = sorted(benchmarks - {'cold_start_ms'})
    
    for bench in benchmarks:
        result1 = data1['benchmarks'][bench]
        result2 = data2['benchmarks'][bench]
        
        if isinstance(result1, dict) and isinstance(result2, dict):
            if 'error' in result1 or 'error' in result2:
                print(f"{bench:<20} {'ERROR':<12} {'ERROR':<12}")
                continue
            
            avg1 = result1.get('avg_ms', 0)
            avg2 = result2.get('avg_ms', 0)
            
            if avg1 > 0:
                change = ((avg2 - avg1) / avg1) * 100
                ratio = avg2 / avg1
                
                status = ""
                if ratio > 1.2:
                    status = "⚠️ SLOWER"
                    regressions.append((bench, ratio, change))
                elif ratio < 0.8:
                    status = "✅ FASTER"
                    improvements.append((bench, ratio, change))
                
                print(f"{bench:<20} {avg1:>8.2f}    {avg2:>8.2f}    "
                      f"{change:>+7.1f}%    {ratio:>5.2f}x  {status}")
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    
    if regressions:
        print("\n⚠️  Performance Regressions:")
        for bench, ratio, change in sorted(regressions, key=lambda x: x[1], reverse=True):
            print(f"  • {bench}: {change:+.1f}% ({ratio:.2f}x)")
    
    if improvements:
        print("\n✅ Performance Improvements:")
        for bench, ratio, change in sorted(improvements, key=lambda x: x[1]):
            print(f"  • {bench}: {change:+.1f}% ({ratio:.2f}x)")
    
    if not regressions and not improvements:
        print("\n➡️  Performance is similar between versions")
    
    # Overall assessment
    print("\n" + "="*70)
    
    if len(regressions) > len(improvements):
        print("⚠️  WARNING: Version 2 appears slower overall")
    elif len(improvements) > len(regressions):
        print("✅ Version 2 appears faster overall")
    else:
        print("➡️  Mixed results - review specific benchmarks")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Compare Opteryx performance between versions"
    )
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Benchmark command
    bench_parser = subparsers.add_parser('benchmark', help='Run benchmarks')
    bench_parser.add_argument(
        '--output', '-o',
        type=str,
        default=f'benchmark-{datetime.now().strftime("%Y%m%d-%H%M%S")}.json',
        help='Output file for results'
    )
    
    # Compare command
    compare_parser = subparsers.add_parser('compare', help='Compare two benchmark files')
    compare_parser.add_argument('file1', help='First benchmark file')
    compare_parser.add_argument('file2', help='Second benchmark file')
    
    args = parser.parse_args()
    
    if args.command == 'benchmark':
        save_benchmark_results(args.output)
    elif args.command == 'compare':
        if not os.path.exists(args.file1):
            print(f"Error: File not found: {args.file1}")
            return 1
        if not os.path.exists(args.file2):
            print(f"Error: File not found: {args.file2}")
            return 1
        compare_results(args.file1, args.file2)
    else:
        parser.print_help()
        print("\nExample usage:")
        print("  # Run benchmark on current version")
        print("  python compare_versions.py benchmark -o current.json")
        print()
        print("  # Switch to different version and benchmark")
        print("  git checkout v0.24.0")
        print("  pip install -e . --force-reinstall")
        print("  python compare_versions.py benchmark -o v0.24.json")
        print()
        print("  # Compare results")
        print("  python compare_versions.py compare v0.24.json current.json")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
