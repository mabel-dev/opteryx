"""
Simple benchmark for opteryx compiled memory pool.

This script attempts to import the Cython-compiled MemoryPool from
`opteryx.compiled.structures.memory_pool` and falls back to a plain-Python
shim if the compiled extension isn't available. It runs a few scenarios:

- commit_only: repeatedly commit byte arrays and immediately release them
- commit_read_release: commit, read (zero-copy and copy), unlatch/release
- multi_threaded_reads: commits a pool of objects and spawns worker threads
  performing zero-copy reads with latching to measure concurrent read throughput

Usage:
    python -m benchmarks.bench_memory_pool

Note: For best results run against the compiled C extension built for this
repo (setup.py / build ext) so that the Cython `MemoryPool` is used.
"""

import time
import threading
import random
import statistics
import argparse
import math

from opteryx.compiled.structures.memory_pool import MemoryPool as CompiledMemoryPool

class MemorySegment:
    def __init__(self, start, length, latches=0):
        self.start = start
        self.length = length
        self.latches = latches

class PythonMemoryPool:
    def __init__(self, size):
        if size <= 0:
            raise ValueError("size must be > 0")
        self.size = size
        self.pool = bytearray(size)
        self.free_segments = [MemorySegment(0, size, 0)]
        self.used_segments = {}
        self.lock = threading.RLock()
        self.next_ref_id = 1

    def _find_free_segment(self, size):
        for i, seg in enumerate(self.free_segments):
            if seg.length >= size:
                return i
        return -1

    def _level1_compaction(self):
        self.free_segments.sort(key=lambda s: s.start)
        out = [self.free_segments[0]] if self.free_segments else []
        for seg in self.free_segments[1:]:
            last = out[-1]
            if last.start + last.length == seg.start:
                last.length += seg.length
            else:
                out.append(seg)
        self.free_segments = out

    def _level2_compaction(self):
        offset = 0
        items = sorted(self.used_segments.items(), key=lambda x: x[1].start)
        for _, seg in items:
            if seg.latches == 0 and seg.start != offset:
                self.pool[offset:offset+seg.length] = self.pool[seg.start:seg.start+seg.length]
                seg.start = offset
            offset = max(offset, seg.start + seg.length)
        free = []
        cur = 0
        for _, seg in sorted(self.used_segments.items(), key=lambda x: x[1].start):
            if seg.start > cur:
                free.append(MemorySegment(cur, seg.start - cur, 0))
            cur = seg.start + seg.length
        if cur < self.size:
            free.append(MemorySegment(cur, self.size - cur, 0))
        self.free_segments = free

    def commit(self, data):
        if isinstance(data, (bytes, bytearray)):
            raw = data
            length = len(raw)
        else:
            raise TypeError("Unsupported data type for commit")

        ref = self.next_ref_id
        self.next_ref_id += 1
        if length == 0:
            self.used_segments[ref] = MemorySegment(0, 0, 0)
            return ref

        total_free = sum(s.length for s in self.free_segments)
        if total_free < length:
            return -1

        with self.lock:
            idx = self._find_free_segment(length)
            if idx == -1:
                self._level1_compaction()
                idx = self._find_free_segment(length)
                if idx == -1:
                    self._level2_compaction()
                    idx = self._find_free_segment(length)
                    if idx == -1:
                        return -1

            seg = self.free_segments.pop(idx)
            if seg.length > length:
                self.free_segments.append(MemorySegment(seg.start + length, seg.length - length, 0))
            start = seg.start
            self.pool[start:start+length] = raw
            self.used_segments[ref] = MemorySegment(start, length, 0)

        return ref

    def read(self, ref_id, zero_copy=0, latch=0):
        with self.lock:
            if ref_id not in self.used_segments:
                raise ValueError("Invalid reference")
            seg = self.used_segments[ref_id]
            if latch:
                seg.latches += 1
            if zero_copy:
                return memoryview(self.pool)[seg.start:seg.start+seg.length]
            else:
                return bytes(self.pool[seg.start:seg.start+seg.length])

    def unlatch(self, ref_id):
        with self.lock:
            seg = self.used_segments[ref_id]
            if seg.latches == 0:
                raise RuntimeError("not latched")
            seg.latches -= 1

    def release(self, ref_id):
        with self.lock:
            seg = self.used_segments.pop(ref_id)
            self.free_segments.append(seg)

    def available_space(self):
        return sum(s.length for s in self.free_segments)

# Benchmark harness

def timeit(func, *fargs, repeat=5):
    times = []
    for _ in range(repeat):
        start_time = time.perf_counter()
        func(*fargs)
        end_time = time.perf_counter()
        times.append(end_time - start_time)
    return min(times), statistics.mean(times), (statistics.stdev(times) if len(times) > 1 else 0.0)

def bench_commit_immediate_release(memory_pool, size, payload_size, iterations):
    """Commit and immediately release on each iteration so the pool is reused."""
    pool = memory_pool(size)
    payload = b"x" * payload_size

    def run():
        for _ in range(iterations):
            r = pool.commit(payload)
            if r == -1:
                # retry once; if still out of space, stop
                r = pool.commit(payload)
            pool.release(r)

    return run


def bench_commit_read_release(memory_pool, size, payload_size, iterations, zero_copy=False, latch=False):
    pool = memory_pool(size)
    payload = b"y" * payload_size

    def run():
        refs = []
        for _ in range(iterations):
            r = pool.commit(payload)
            if r == -1:
                break
            # read
            data = pool.read(r, zero_copy=1 if zero_copy else 0, latch=1 if latch else 0)
            if latch and isinstance(data, memoryview):
                # unlatch immediately
                pool.unlatch(r)
            refs.append(r)
        for r in refs:
            pool.release(r)

    return run


def bench_commit_read_release_immediate(memory_pool, size, payload_size, iterations, zero_copy=False, latch=False):
    """Commit, read and immediately release on each iteration so the pool is reused."""
    pool = memory_pool(size)
    payload = b"y" * payload_size

    def run():
        for _ in range(iterations):
            r = pool.commit(payload)
            if r == -1:
                # retry once; if still out of space, stop
                r = pool.commit(payload)
                if r == -1:
                    break

            data = pool.read(r, zero_copy=1 if zero_copy else 0, latch=1 if latch else 0)
            if latch and isinstance(data, memoryview):
                pool.unlatch(r)
            pool.release(r)

    return run


def bench_multi_threaded_reads(memory_pool, size, payload_size, n_items, n_threads, iters_per_thread):
    pool = memory_pool(size)
    payload = b"m" * payload_size
    refs = []
    for _ in range(n_items):
        r = pool.commit(payload)
        if r == -1:
            break
        refs.append(r)

    stop = threading.Event()
    latched_counts = [0]

    def worker():
        local_reads = 0
        while not stop.is_set():
            r = random.choice(refs)
            data = pool.read(r, zero_copy=1, latch=1)
            # simulate small work
            _ = len(data)
            pool.unlatch(r)
            local_reads += 1
            if local_reads >= iters_per_thread:
                break
        latched_counts[0] += local_reads

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]

    def run():
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        stop.set()
        # no cleanup for brevity

    return run, pool, refs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MemoryPool stress benchmark")
    parser.add_argument("--scenario", choices=["commit","commit_read","commit_read_immediate","multi_read"], default="commit_read_immediate")
    parser.add_argument("--pool-size", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--payload-size", type=int, default=1024 * 1024)
    parser.add_argument("--iterations", type=int, default=5000)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--items", type=int, default=1024, help="number of items to pre-commit for multi_read")
    parser.add_argument("--iters-per-thread", type=int, default=1000)
    parser.add_argument("--sample-rate", type=int, default=1000, help="collect latency sample every N ops")
    parser.add_argument("--zero-copy", action="store_true")
    parser.add_argument("--latch", action="store_true")
    parser.add_argument("--warmup", type=int, default=1000, help="warmup iterations to skip from sampling")

    args = parser.parse_args()

    def measure_commit_read_release_immediate(memory_pool, size, payload_size, iterations, sample_rate=1000, zero_copy=False, latch=False, warmup=1000):
        pool = memory_pool(size)
        payload = b"y" * payload_size

        samples = []
        total_bytes = 0
        # warmup iterations
        for _ in range(min(warmup, iterations)):
            r = pool.commit(payload)
            if r == -1:
                r = pool.commit(payload)
                if r == -1:
                    break
            _ = pool.read(r, zero_copy=1 if zero_copy else 0, latch=1 if latch else 0)
            if latch:
                pool.unlatch(r)
            pool.release(r)

        start = time.perf_counter()
        for i in range(iterations - min(warmup, iterations)):
            op_t0 = time.perf_counter()
            r = pool.commit(payload)
            if r == -1:
                # retry once
                r = pool.commit(payload)
                if r == -1:
                    break
            data = pool.read(r, zero_copy=1 if zero_copy else 0, latch=1 if latch else 0)
            if latch:
                pool.unlatch(r)
            pool.release(r)
            op_t1 = time.perf_counter()

            total_bytes += payload_size
            if (i % sample_rate) == 0:
                samples.append((op_t1 - op_t0) * 1e6)  # microseconds

        stop = time.perf_counter()
        total_ops = (iterations - min(warmup, iterations))
        elapsed = stop - start
        ops_per_sec = total_ops / elapsed if elapsed > 0 else float('inf')
        mb_per_sec = (total_bytes / 1024 / 1024) / elapsed if elapsed > 0 else float('inf')

        # latency stats from samples
        lat_stats = {}
        if samples:
            samples_sorted = sorted(samples)
            lat_stats['p50'] = statistics.median(samples_sorted)
            lat_stats['p95'] = samples_sorted[min(len(samples_sorted)-1, math.floor(len(samples_sorted)*0.95))]
            lat_stats['p99'] = samples_sorted[min(len(samples_sorted)-1, math.floor(len(samples_sorted)*0.99))]
            lat_stats['mean'] = statistics.mean(samples_sorted)
            lat_stats['stddev'] = statistics.stdev(samples_sorted) if len(samples_sorted) > 1 else 0.0

        return {
            'total_ops': total_ops,
            'elapsed_s': elapsed,
            'ops_per_sec': ops_per_sec,
            'mb_per_sec': mb_per_sec,
            'latency_samples': len(samples),
            'latency_stats': lat_stats,
        }

    if args.scenario == 'commit_read_immediate':
        print(f"Running commit+read+release immediate: iterations={args.iterations} payload={args.payload_size} threads={args.threads}")
        if args.threads > 1:
            # simple multi-threaded executor that runs the same work per thread and aggregates
            results = []
            def thread_target():
                res = measure_commit_read_release_immediate(CompiledMemoryPool, args.pool_size, args.payload_size, args.iterations // args.threads, args.sample_rate, args.zero_copy, args.latch, args.warmup)
                results.append(res)

            ths = [threading.Thread(target=thread_target) for _ in range(args.threads)]
            t0 = time.perf_counter()
            for t in ths:
                t.start()
            for t in ths:
                t.join()
            t1 = time.perf_counter()

            # aggregate
            total_ops = sum(r['total_ops'] for r in results)
            elapsed = t1 - t0
            ops_per_sec = total_ops / elapsed if elapsed > 0 else float('inf')
            mb_per_sec = sum(r['mb_per_sec'] for r in results) / len(results) if results else 0.0
            print(f"Total ops: {total_ops} elapsed: {elapsed:.4f}s ops/s: {ops_per_sec:.0f} MB/s: {mb_per_sec:.2f}")
        else:
            print()
            print("Compiled MemoryPool results:")
            res = measure_commit_read_release_immediate(CompiledMemoryPool, args.pool_size, args.payload_size, args.iterations, args.sample_rate, args.zero_copy, args.latch, args.warmup)
            print(f"Total ops: {res['total_ops']} elapsed: {res['elapsed_s']:.4f}s ops/s: {res['ops_per_sec']:.0f} MB/s: {res['mb_per_sec']:.2f}")
            if res['latency_stats']:
                print("Latency (us): p50={p50:.2f} p95={p95:.2f} p99={p99:.2f} mean={mean:.2f} stddev={stddev:.2f}".format(**res['latency_stats']))

            print()
            print("Python MemoryPool results:")
            res = measure_commit_read_release_immediate(PythonMemoryPool, args.pool_size, args.payload_size, args.iterations, args.sample_rate, args.zero_copy, args.latch, args.warmup)
            print(f"Total ops: {res['total_ops']} elapsed: {res['elapsed_s']:.4f}s ops/s: {res['ops_per_sec']:.0f} MB/s: {res['mb_per_sec']:.2f}")
            if res['latency_stats']:
                print("Latency (us): p50={p50:.2f} p95={p95:.2f} p99={p99:.2f} mean={mean:.2f} stddev={stddev:.2f}".format(**res['latency_stats']))

    elif args.scenario == 'multi_read':
        print("Running multi-threaded read scenario")
        run_mt, pool_mt, refs_mt = bench_multi_threaded_reads(args.pool_size, args.payload_size, args.items, args.threads, args.iters_per_thread)
        t0 = time.perf_counter()
        run_mt()
        t1 = time.perf_counter()
        print(f"multi_read elapsed: {t1-t0:.4f}s")

    else:
        print("Unknown or unsupported scenario selected")
