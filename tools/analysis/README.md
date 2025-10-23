# Opteryx Performance Analysis Tools

This directory contains tools for analyzing and diagnosing performance issues in Opteryx.

## Tools Overview

### 1. diagnose_performance.py

**Purpose:** Comprehensive performance diagnostics to identify bottlenecks.

**Usage:**
```bash
python tools/analysis/diagnose_performance.py
```

**What it does:**
- Tests cold start performance (first query vs warm queries)
- Tests repeated query consistency
- Tests different SQL operation types
- Tests data size scaling
- Provides specific recommendations

**Example output:**
```
Cold start: 264.43ms
Warm average: 3.66ms
Ratio: 72.3x ⚠️
```

### 2. performance_comparison.py

**Purpose:** Run a standardized benchmark suite with detailed metrics.

**Usage:**
```bash
# Run with default settings
python tools/analysis/performance_comparison.py

# Run with verbose output
python tools/analysis/performance_comparison.py --verbose

# Run with custom iterations and output file
python tools/analysis/performance_comparison.py --iterations 10 --output my-results.json
```

**What it does:**
- Runs 10+ different query patterns
- Measures execution time and memory usage
- Identifies slow queries (>1000ms)
- Detects high memory usage (>50MB)
- Saves results to JSON for later analysis

### 3. detailed_profiler.py

**Purpose:** Deep profiling using Python's cProfile to identify specific bottleneck functions.

**Usage:**
```bash
# Profile all operations
python tools/analysis/detailed_profiler.py

# Profile a specific query
python tools/analysis/detailed_profiler.py --query "SELECT COUNT(*) FROM \$planets"

# Compare against baseline expectations
python tools/analysis/detailed_profiler.py --baseline

# Sort by different metrics
python tools/analysis/detailed_profiler.py --sort time
python tools/analysis/detailed_profiler.py --sort calls
```

**What it does:**
- Uses cProfile to identify hot spots in code
- Shows function-level timing
- Shows call counts and callers
- Compares against expected performance
- Identifies specific functions to optimize

### 4. compare_versions.py

**Purpose:** Compare performance between different Opteryx versions or commits.

**Usage:**
```bash
# Create benchmark for current version
python tools/analysis/compare_versions.py benchmark -o current.json

# Switch to a different version
git checkout v0.24.0
pip install -e . --force-reinstall

# Benchmark the other version
python tools/analysis/compare_versions.py benchmark -o v0.24.json

# Compare results
python tools/analysis/compare_versions.py compare v0.24.json current.json
```

**What it does:**
- Runs standardized benchmarks
- Saves results with git commit info
- Compares two benchmark files
- Identifies regressions and improvements
- Shows percentage changes and ratios

**Example output:**
```
Benchmark            V1 (ms)      V2 (ms)      Change      Ratio
----------------------------------------------------------------------
count                   3.50         3.74       +6.9%     1.07x
cold_start            50.00       247.63     +395.3%     4.95x ⚠️ SLOWER
```

### 5. ClickBench Performance Test (Existing, Enhanced)

**Location:** `tests/performance/benchmarks/clickbench.py`

**Purpose:** Test warm query performance on real ClickBench queries. This is the existing test suite adapted to support performance analysis with multiple iterations.

**Usage:**
```bash
# Original single-run mode
python tests/performance/benchmarks/clickbench.py

# Warm query testing mode (NEW - runs multiple iterations)
python tests/performance/benchmarks/clickbench.py --warm

# Custom iterations
python tests/performance/benchmarks/clickbench.py --warm --iterations 5
```

**What it does:**
- Runs ClickBench benchmark queries (real-world analytical queries)
- Measures warm performance (after cold start)
- Tests complex queries with GROUP BY, aggregations, JOINs
- Identifies slow queries (>500ms)
- Checks for performance variance

**When to use:**
- To verify warm query performance on realistic workloads
- When maintainer reports ClickBench queries are slow
- To identify algorithmic performance issues
- To compare with previous versions

**Example output:**
```
Query    Run 1        Run 2        Run 3        Avg         Min         Max
Q01      15.20ms      14.80ms      14.90ms      14.97ms     14.80ms     15.20ms
Q05      856.30ms     845.20ms     851.10ms     850.87ms    845.20ms    856.30ms ⚠️
```

### 6. query_profiler.py

**Purpose:** Profile individual queries with detailed metrics.

**Usage:**
```bash
# See the file for usage - it's more of a library than a CLI tool
```

This is an existing tool that provides query profiling capabilities.

## Quick Start Guide

### Diagnose Performance Issues

1. **First, run the diagnostic tool:**
   ```bash
   python tools/analysis/diagnose_performance.py
   ```
   This will identify if you have cold start, scaling, or other issues.

2. **If issues are found, run detailed profiler:**
   ```bash
   python tools/analysis/detailed_profiler.py --baseline
   ```
   This will show which specific operations are slow.

3. **For deep investigation:**
   ```bash
   python tools/analysis/detailed_profiler.py --query "YOUR_SLOW_QUERY"
   ```
   This will show exactly which functions are consuming time.

### Compare Versions

1. **Benchmark current version:**
   ```bash
   python tools/analysis/compare_versions.py benchmark -o after.json
   ```

2. **Make your changes** (or checkout a different commit)

3. **Benchmark again:**
   ```bash
   python tools/analysis/compare_versions.py benchmark -o before.json
   ```

4. **Compare:**
   ```bash
   python tools/analysis/compare_versions.py compare before.json after.json
   ```

## Current Known Issues (v0.26.0-beta.1676)

Based on analysis, the main issue is:

### Cold Start Overhead (72.3x slower)

**Symptoms:**
- First query takes ~260ms
- Subsequent queries take ~3-5ms
- Import takes ~127ms

**Affected scenarios:**
- CLI single-query operations
- Serverless/Lambda deployments
- Test suites
- Development iteration

**Root causes:**
1. Heavy module imports (orso, pandas, pyarrow)
2. All cache managers imported upfront
3. Virtual dataset initialization
4. Query plan cache setup

**Recommendations:**
- Implement lazy loading for cache managers
- Defer heavy imports to first use
- Create lightweight core module
- Add `opteryx.warmup()` for long-running processes

See `PERFORMANCE_ANALYSIS.md` in the root directory for detailed analysis.

## Interpreting Results

### Good Performance Indicators

✅ Warm query times < 10ms for simple queries  
✅ Warm query times < 50ms for complex queries  
✅ Cold start / warm ratio < 5x  
✅ Linear scaling with data size  
✅ Consistent times across runs  

### Warning Signs

⚠️ Cold start > 100ms  
⚠️ Warm queries > 50ms  
⚠️ Cold start / warm ratio > 10x  
⚠️ Non-linear scaling (O(n²))  
⚠️ High variance between runs  
⚠️ Memory usage > 100MB for small queries  

## Contributing

When adding new features or making changes:

1. **Run benchmarks before and after:**
   ```bash
   python tools/analysis/compare_versions.py benchmark -o before.json
   # Make your changes
   python tools/analysis/compare_versions.py benchmark -o after.json
   python tools/analysis/compare_versions.py compare before.json after.json
   ```

2. **Check for regressions:**
   - Cold start should not increase by >20%
   - Warm queries should not increase by >10%
   - Memory usage should not increase significantly

3. **Profile if needed:**
   ```bash
   python tools/analysis/detailed_profiler.py
   ```

4. **Update benchmarks in CI** if making performance-critical changes

## Troubleshooting

### Tool won't run

**Problem:** `ModuleNotFoundError: No module named 'opteryx'`  
**Solution:** Install opteryx first: `pip install -e .`

**Problem:** `No module named 'pytest'`  
**Solution:** Install test dependencies: `pip install -r tests/requirements.txt`

### Inconsistent results

**Problem:** Times vary significantly between runs  
**Solution:** 
- Close other applications
- Run multiple iterations
- Use the `--iterations` flag to increase sample size

### Compilation issues

**Problem:** Getting Python fallback instead of compiled code  
**Solution:** 
- Rebuild extensions: `python setup.py build_ext --inplace`
- Check compilation: `find opteryx/compiled -name '*.so' | wc -l`

## Further Reading

- `PERFORMANCE_ANALYSIS.md` - Detailed analysis of current performance
- `DEVELOPER_GUIDE.md` - General development guidelines
- Official docs: https://opteryx.dev/
