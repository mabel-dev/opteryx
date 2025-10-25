# Quick Start: Fixing the Performance Regression

This guide helps you quickly implement fixes for the identified cold start performance issue.

## The Problem

Current version has a **72.3x slowdown** on first query:
- Import: 127ms
- First query: 260ms
- Warm queries: 2-8ms (excellent)

## Quick Wins (Easy to Implement)

### 1. Lazy Load Cache Managers (HIGH IMPACT)

**Current code** (in `opteryx/managers/cache/__init__.py`):
```python
from .memcached import MemcachedCache
from .redis import RedisCache
from .valkey import ValkeyCache
from .null_cache import NullCache
```

**Fixed code:**
```python
# Only import the cache manager being used
def get_cache_manager(cache_type):
    if cache_type == 'memcached':
        from .memcached import MemcachedCache
        return MemcachedCache
    elif cache_type == 'redis':
        from .redis import RedisCache
        return RedisCache
    elif cache_type == 'valkey':
        from .valkey import ValkeyCache
        return ValkeyCache
    else:
        from .null_cache import NullCache
        return NullCache
```

**Expected improvement:** ~5-10ms import time savings

### 2. Defer Heavy Imports (MEDIUM IMPACT)

**Current pattern:**
```python
# At module level
import pandas
import pyarrow
```

**Better pattern:**
```python
# Inside functions where needed
def some_function():
    import pandas  # Only loaded when function is called
    # ... use pandas
```

**Expected improvement:** ~20-30ms import time savings

### 3. Lazy Virtual Dataset Registration (MEDIUM IMPACT)

**Current approach:** Register all virtual datasets at import time

**Better approach:**
```python
class VirtualDatasetManager:
    def __init__(self):
        self._datasets = {}
        self._registered = False
    
    def _ensure_loaded(self):
        if not self._registered:
            self._register_all_datasets()
            self._registered = True
    
    def get_dataset(self, name):
        self._ensure_loaded()
        return self._datasets.get(name)
```

**Expected improvement:** ~30-50ms first query savings

### 4. Add Warmup Function (LOW EFFORT)

Add a public API for explicitly warming up caches:

```python
# In opteryx/__init__.py
def warmup():
    """
    Pre-initialize caches and structures for better performance.
    Call this once at application startup for long-running processes.
    """
    # Execute a dummy query to trigger initialization
    query_to_arrow("SELECT 1")
```

**Usage:**
```python
import opteryx
opteryx.warmup()  # Do this once at startup

# Now all queries are fast
result = opteryx.query("SELECT * FROM ...")
```

## Testing Your Changes

### 1. Measure Before
```bash
python tools/analysis/compare_versions.py benchmark -o before-fix.json
```

### 2. Make Changes

Implement one or more of the fixes above.

### 3. Measure After
```bash
python tools/analysis/compare_versions.py benchmark -o after-fix.json
```

### 4. Compare
```bash
python tools/analysis/compare_versions.py compare before-fix.json after-fix.json
```

**Target improvements:**
- Cold start: < 100ms (currently 260ms)
- Import: < 50ms (currently 127ms)
- Warm queries: maintain current 2-8ms performance

## More Aggressive Fixes (Harder to Implement)

### 5. Split into Core and Extras

Create a lightweight core module:

```python
# opteryx/__init__.py
# Core functionality with minimal dependencies
from .core import query, query_to_arrow

# Optional - lazy load extras
def __getattr__(name):
    if name == 'advanced_features':
        from . import extras
        return extras
    raise AttributeError(f"module {__name__} has no attribute {name}")
```

**Expected improvement:** ~50-70ms import time

### 6. C Extension for Hot Paths

If profiling shows specific hot paths, consider:
- Adding them to setup.py for compilation
- Using Cython for performance-critical code
- Ensuring all compiled extensions are used

### 7. Connection Pooling Optimization

Defer connection pool initialization:
```python
class ConnectionManager:
    def __init__(self):
        self._pool = None
    
    @property
    def pool(self):
        if self._pool is None:
            self._pool = self._create_pool()
        return self._pool
```

## Validation Checklist

Before considering the fix complete:

- [ ] Cold start < 100ms (target: <50ms)
- [ ] Import time < 50ms (target: <30ms)
- [ ] Warm query performance maintained (2-8ms)
- [ ] All existing tests pass
- [ ] No regressions in functionality
- [ ] Documentation updated
- [ ] Benchmark results committed

## Measuring Success

Run the full diagnostic:
```bash
python tools/analysis/diagnose_performance.py
```

Look for:
```
Cold start: <100ms  ✅
Warm average: 2-5ms ✅
Ratio: <10x ✅
```

## Example PR Checklist

```markdown
## Performance Fix: Lazy Loading

### Changes Made
- Implemented lazy loading for cache managers
- Deferred pandas import to first use
- Added warmup() function for long-running processes

### Measurements
| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Cold start | 260ms | 80ms | 69% faster |
| Import | 127ms | 40ms | 69% faster |
| Warm query | 4ms | 4ms | No change |

### Testing
- [x] All tests pass
- [x] Benchmarks show improvement
- [x] No functionality regression
- [x] Documentation updated
```

## Common Pitfalls

### ❌ Don't Do This
```python
# Breaking change - removes feature
def expensive_operation():
    raise NotImplementedError("Removed for performance")
```

### ✅ Do This Instead
```python
# Lazy load - maintains feature, improves performance
def expensive_operation():
    import expensive_module  # Only loaded when actually used
    return expensive_module.do_work()
```

### ❌ Don't Micro-optimize
Focus on the big wins:
1. Lazy loading heavy imports (20-50ms savings)
2. Deferred initialization (30-50ms savings)
3. Cache manager optimization (5-15ms savings)

Don't spend time on:
- ❌ Micro-optimizing tight loops (unless profiler shows it's hot)
- ❌ Premature optimization of rarely-used code paths
- ❌ Sacrificing code clarity for 1-2ms savings

### ✅ Profile First
Always profile before and after:
```bash
python -X importtime -c 'import opteryx' 2>&1 | tail -30
```

## Questions?

1. Review `PERFORMANCE_ANALYSIS.md` for detailed analysis
2. Check `tools/analysis/README.md` for tool usage
3. Run diagnostics: `python tools/analysis/diagnose_performance.py`
4. Create an issue with your benchmark results

## Success Criteria

The fix is successful when:
1. **Cold start < 100ms** (80% improvement from 260ms)
2. **Import < 50ms** (60% improvement from 127ms)
3. **Warm performance maintained** (2-8ms unchanged)
4. **No functionality broken** (all tests pass)

Focus on these metrics and you'll eliminate the performance regression!
