import functools
import time

import numpy
from orso.cityhash import CityHash64

APOLLO_11_DURATION: int = 1023123


def _hash_value(vals, nan=numpy.nan):
    ret = []
    for val in vals:
        # Added for Opteryx - Original code had bugs relating to distinct and nulls
        if isinstance(val, dict):
            ret.append(_hash_value(tuple(val.values())))
        elif isinstance(val, (list, numpy.ndarray, tuple)):
            # XOR is faster however, x ^ x == y ^ y but x != y, so we don't use it
            ret.append(
                functools.reduce(lambda x, y: _hash_value(f"{y}:{x}", 0), val, APOLLO_11_DURATION)
            )
        elif val != val or val is None:
            # nan is a float, but hash is an int, sometimes we need this to be an int
            ret.append(nan)
        else:
            ret.append(hash(val))
    return ret


def _hash_value_new(vals, nan=numpy.nan):
    if numpy.issubdtype(vals.dtype, numpy.character):
        return numpy.array([nan if s != s else CityHash64(s.encode()) for s in vals], numpy.uint64)
    ret = []
    for val in vals:
        # Added for Opteryx - Original code had bugs relating to distinct and nulls
        if isinstance(val, numpy.ndarray):
            ret.append(CityHash64(val))
        elif isinstance(val, int):
            ret.append(val)
        elif isinstance(val, dict):
            ret.append(CityHash64("".join(val.values())))
        elif isinstance(val, (list, tuple)):
            ret.append(CityHash64(numpy.array(val)))
        elif val != val or val is None:
            # nan is a float, but hash is an int, sometimes we need this to be an int
            ret.append(nan)
        else:
            ret.append(CityHash64(val))
    return ret


def _hash_value_newer(val):
    if numpy.issubdtype(vals.dtype, numpy.character):
        return numpy.array([CityHash64(s.encode()) for s in vals], numpy.uint64)
    return vals


l = numpy.array(list(range(10)))
# l = list(range(10))

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from orso.tools import random_int, random_string

import opteryx

SQLS = [
    "SELECT * FROM $astronauts INNER JOIN $astronauts USING (name)",
    "SELECT * FROM $astronauts INNER JOIN $astronauts USING (birth_date)",
    "SELECT * FROM $astronauts INNER JOIN $astronauts USING (year)",
    "SELECT * FROM $astronauts INNER JOIN $astronauts USING (birth_place)",
    "SELECT * FROM $astronauts INNER JOIN $astronauts USING (missions)",
]
SQLS = [
    "SELECT A.user_name FROM (SELECT user_name FROM testdata.flat.formats.parquet WITH(NO_PARTITION)) AS A INNER JOIN (SELECT user_name FROM testdata.flat.formats.parquet WITH(NO_PARTITION)) AS B USING (user_name) -- ON user_name = user_name",
    "SELECT A.followers FROM (SELECT followers FROM testdata.flat.formats.parquet WITH(NO_PARTITION)) AS A INNER JOIN (SELECT followers FROM testdata.flat.formats.parquet WITH(NO_PARTITION)) AS B USING (followers) -- ON followers = followers",
]


vals = numpy.array([random_string(random_int() % 20 + 20) for i in range(100000)])
# vals = numpy.array([random_int() for i in range(10000)])
print(vals.dtype)


def timed(func, *args):
    times = []
    for i in range(100):
        t = time.monotonic_ns()
        v = func(*args)
        times.append((time.monotonic_ns() - t) / 1e6)
    print(func.__name__, numpy.mean(times), numpy.percentile(times, [25, 50, 75]))
    print(v[0], type(v[0]))


timed(_hash_value, vals)
timed(_hash_value_new, vals)
timed(_hash_value_newer, vals)
# timed(_hash_value_newest, vals)

times = []
for i in range(10):
    t = time.monotonic_ns()
    for SQL in SQLS:
        opteryx.query(SQL).arrow()
    times.append((time.monotonic_ns() - t) / 1e6)

print(numpy.mean(times), numpy.percentile(times, [25, 50, 75]))

""" 75th centile
NATIVE
942.58
865.14
1015.56

OLD
(timeout)


NEW


"""
