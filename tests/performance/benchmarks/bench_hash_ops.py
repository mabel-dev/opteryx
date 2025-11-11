import os
import sys

os.environ.pop("OPTERYX_DEBUG", None)

sys.path.insert(1, os.path.join(sys.path[0], "../../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import time
import pyarrow as pa
import numpy as np
from opteryx.compiled.table_ops.hash_ops import compute_hashes


def make_int_table(n):
    arr = pa.array(np.random.randint(0, 1<<31, size=n), type=pa.int64())
    return pa.table({'a': arr})


def make_str_table(n, avg_len=20):
    import random, string
    def rand_str():
        return ''.join(random.choices(string.ascii_letters + string.digits, k=avg_len))
    data = [rand_str() for _ in range(n)]
    arr = pa.array(data)
    return pa.table({'s': arr})


def make_list_table(n, list_len=5):
    data = [[i % 5 for i in range(list_len)] for _ in range(n)]
    arr = pa.array(data)
    return pa.table({'l': arr})


def bench(table, cols, iterations=5):
    # warmup
    compute_hashes(table, cols)
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        compute_hashes(table, cols)
        t1 = time.perf_counter()
        times.append(t1 - t0)
    return min(times), sum(times) / len(times)


if __name__ == '__main__':
    N = 200000
    print('Preparing tables...')
    t_int = make_int_table(N)
    t_str = make_str_table(N, avg_len=30)
    t_list = make_list_table(N, list_len=3)

    print('Benchmarking int table...')
    best, mean = bench(t_int, ['a'])
    print(f'int table: best={best:.4f}s mean={mean:.4f}s')

    print('Benchmarking str table...')
    best, mean = bench(t_str, ['s'])
    print(f'str table: best={best:.4f}s mean={mean:.4f}s')

    print('Benchmarking list table...')
    best, mean = bench(t_list, ['l'])
    print(f'list table: best={best:.4f}s mean={mean:.4f}s')
