#!/usr/bin/env python3
"""
Compare two ClickBench benchmark runs and show differences.

Usage:
    python compare_runs.py baseline.txt comparison.txt [--threshold 10]
    
Example:
    python compare_runs.py 8500T@0.25.0b1444.txt 8500T@0.26.0b1710.txt
    python compare_runs.py old_run.txt new_run.txt --threshold 5
"""

import argparse
import re
import sys
from pathlib import Path


def parse_benchmark(filename):
    """Parse a benchmark file and extract version and timing data."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found", file=sys.stderr)
        sys.exit(1)
    
    # Extract version - try multiple patterns
    version = None
    for pattern in [
        r'0\.\d+\.\d+-beta\.\d+',  # 0.25.0-beta.1444
        r'v?\d+\.\d+\.\d+',          # v1.0.0 or 1.0.0
        r'\d+\.\d+\.\d+b\d+',        # 0.25.0b1444
    ]:
        version_match = re.search(pattern, content)
        if version_match:
            version = version_match.group(0)
            break
    
    if not version:
        # Use filename as fallback
        version = Path(filename).stem
    
    # Extract timing arrays
    times = []
    for line in content.split('\n'):
        match = re.match(r'\[([0-9.,null]+)\]', line)
        if match:
            values = match.group(1).split(',')
            # Take the median (3rd value, index 2) or first if less values
            try:
                if 'null' in values:
                    times.append(None)
                else:
                    times.append(float(values[2]) if len(values) >= 3 else float(values[0]))
            except (ValueError, IndexError):
                times.append(None)
    
    return version, times


def main():
    parser = argparse.ArgumentParser(
        description='Compare two ClickBench benchmark runs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('baseline', help='Baseline benchmark file')
    parser.add_argument('comparison', help='Comparison benchmark file')
    parser.add_argument('--threshold', '-t', type=float, default=10.0,
                       help='Percentage threshold for flagging regressions/improvements (default: 10%%)')
    parser.add_argument('--top', '-n', type=int, default=10,
                       help='Number of top regressions/improvements to show (default: 10)')
    
    args = parser.parse_args()
    
    baseline_version, baseline_times = parse_benchmark(args.baseline)
    comparison_version, comparison_times = parse_benchmark(args.comparison)
    
    if not baseline_times or not comparison_times:
        print("Error: Could not parse timing data from one or both files", file=sys.stderr)
        sys.exit(1)
    
    print(f'Comparing {baseline_version} (baseline) vs {comparison_version} (comparison)')
    print(f'Found {len(baseline_times)} queries in baseline and {len(comparison_times)} queries in comparison')
    print()
    print('Query # | Baseline | Compare | Delta   | Change')
    print('--------|----------|---------|---------|--------')
    
    regressions = []
    improvements = []
    
    for i, (t_base, t_comp) in enumerate(zip(baseline_times, comparison_times), 1):
        if t_base is None or t_comp is None:
            delta_str = 'N/A'
            change_str = 'N/A'
        else:
            delta = t_comp - t_base
            pct_change = ((t_comp / t_base) - 1) * 100 if t_base > 0 else 0
            delta_str = f'{delta:+.2f}s'
            change_str = f'{pct_change:+.1f}%'
            
            if pct_change > args.threshold:
                regressions.append((i, t_base, t_comp, pct_change))
            elif pct_change < -args.threshold:
                improvements.append((i, t_base, t_comp, pct_change))
        
        print(f'{i:7} | {t_base if t_base else "null":8} | {t_comp if t_comp else "null":7} | {delta_str:7} | {change_str:>7}')
    
    print()
    print(f'Total queries: {len(baseline_times)}')
    print(f'Regressions (>{args.threshold}% slower): {len(regressions)}')
    print(f'Improvements (>{args.threshold}% faster): {len(improvements)}')
    print()
    
    if regressions:
        print(f'Top {min(args.top, len(regressions))} Regressions:')
        regressions.sort(key=lambda x: x[3], reverse=True)
        for q, t_base, t_comp, pct in regressions[:args.top]:
            print(f'  Query {q:2}: {t_base:.2f}s → {t_comp:.2f}s ({pct:+.1f}%)')
    
    if improvements:
        print()
        print(f'Top {min(args.top, len(improvements))} Improvements:')
        improvements.sort(key=lambda x: x[3])
        for q, t_base, t_comp, pct in improvements[:args.top]:
            print(f'  Query {q:2}: {t_base:.2f}s → {t_comp:.2f}s ({pct:+.1f}%)')


if __name__ == '__main__':
    main()