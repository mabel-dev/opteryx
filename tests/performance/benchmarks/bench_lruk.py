"""
Benchmark for the compiled LRU-K implementation.

This follows the same style as other benchmarks in `tests/performance/benchmarks`.

Scenarios:
- set_only: repeatedly insert unique keys until eviction occurs
Usage:
    python -m benchmarks.bench_lruk

Note: the compiled Cython `LRU_K` is preferred; the benchmark will import
`opteryx.compiled.structures.lru_k.LRU_K` and run the scenarios.
"""
import os
import sys

# Ensure imports resolve when running from repository root
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import time
import random
import statistics
import argparse
import importlib


# Try to load the compiled Cython implementation; if not present leave as None
CompiledLRU = None
try:
    _mod = importlib.import_module("opteryx.compiled.structures.lru_k")
    CompiledLRU = getattr(_mod, "LRU_K", None)
except (ImportError, AttributeError):
    CompiledLRU = None


# Minimal Python reference implementation for side-by-side comparison.
class PythonLRU:
    def __init__(self, k=2):
        self.k = k
        self.slots = {}
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.inserts = 0

    def do_work(self, iterations: int) -> None:
        # small, deterministic CPU-bound loop (no imports of random)
        x = 0x9e3779b97f4a7c15
        for i in range(iterations):
            x = (x ^ (i + 0x9e3779b97f4a7c15)) * 0xbf58476d1ce4e5b9 & 0xFFFFFFFFFFFFFFFF
            # use x so loop isn't optimized away
            if x == 0xDEADBEEF:
                print()

    def set(self, key, value):
        self.inserts += 1
        self.slots[key] = value
        self.do_work(10)  # Simulate some overhead

    def get(self, key):
        v = self.slots.get(key)
        if v is None:
            self.misses += 1
        else:
            self.hits += 1
        return v


def timeit(func, *fargs, repeat=5):
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        func(*fargs)
        t1 = time.perf_counter()
        times.append(t1 - t0)
    return min(times), statistics.mean(times), (statistics.stdev(times) if len(times) > 1 else 0.0)


def bench_set_only(cache_cls, n_inserts, key_size=16, value_size=64):
    """Insert unique keys until n_inserts performed. Measures throughput."""
    cache_obj = cache_cls()

    def runner():
        for i in range(n_inserts):
            key = f"k-{i}".encode().ljust(key_size, b"_")[:key_size]
            val = (f"v-{i}".encode()).ljust(value_size, b"_")[:value_size]
            cache_obj.set(key, val)

    return runner, cache_obj


def bench_get_only(cache_cls, prepopulate, n_gets):
    """Populate the cache with `prepopulate` items, then randomly get keys."""
    cache_obj = cache_cls()
    keys = []
    for i in range(prepopulate):
        k = f"k-{i}".encode()
        cache_obj.set(k, b"v")
        keys.append(k)

    def runner():
        for _ in range(n_gets):
            k = random.choice(keys)
            _ = cache_obj.get(k)

    return runner, cache_obj


def bench_mixed(cache_cls, prepopulate, n_ops, write_ratio=0.1):
    cache_obj = cache_cls()
    keys = []
    for i in range(prepopulate):
        k = f"k-{i}".encode()
        cache_obj.set(k, b"v")
        keys.append(k)

    next_i = prepopulate

    def runner():
        nonlocal next_i
        for _ in range(n_ops):
            if random.random() < write_ratio:
                k = f"k-{next_i}".encode()
                cache_obj.set(k, b"v")
                keys.append(k)
                next_i += 1
            else:
                k = random.choice(keys)
                _ = cache_obj.get(k)

    return runner, cache_obj


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LRU-K microbenchmarks")
    parser.add_argument("--scenario", choices=["set_only", "get_only", "mixed"], default="mixed")
    parser.add_argument("--inserts", type=int, default=100000)
    parser.add_argument("--prepopulate", type=int, default=10000)
    parser.add_argument("--gets", type=int, default=100000)
    parser.add_argument("--ops", type=int, default=100000)
    parser.add_argument("--repeat", type=int, default=10)
    args = parser.parse_args()

    def run_and_report(cache_label, cache_cls):
        if args.scenario == 'set_only':
            run_fn, _ = bench_set_only(cache_cls, n_inserts=args.inserts)
            ops = args.inserts
        elif args.scenario == 'get_only':
            run_fn, _ = bench_get_only(cache_cls, prepopulate=args.prepopulate, n_gets=args.gets)
            ops = args.gets
        else:
            run_fn, _ = bench_mixed(cache_cls, prepopulate=args.prepopulate, n_ops=args.ops)
            ops = args.ops

        mn, mean, sd = timeit(run_fn, repeat=args.repeat)
        print(f"{cache_label}: min={mn:.4f}s mean={mean:.4f}s sd={sd:.4f}s ops/s={(ops/mn):.0f}")

    print()
    print("Benchmarking LRU-K implementations")
    print("---------------------------------")

    # Run compiled implementation if available
    if CompiledLRU is not None:
        print("Compiled LRU-K (Cython):")
        run_and_report("compiled", CompiledLRU)
    else:
        print("Compiled LRU-K not available (skipping)")

    # Run stable Python reference implementation
    print("Python reference LRU-K:")
    run_and_report("python", PythonLRU)
