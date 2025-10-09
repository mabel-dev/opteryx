import os
import sys

import pytest
import datetime
import random
import string
from decimal import Decimal

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.structures.relation_statistics import RelationStatistics
from opteryx.compiled.structures.relation_statistics import to_int

NULL_FLAG:int = -(1 << 63)
MIN_SIGNED_64BIT:int = NULL_FLAG + 1
MAX_SIGNED_64BIT:int = (1 << 63) - 1

def random_key(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def test_to_int_basic_types():
    assert to_int(123) == 123
    assert to_int(-123) == -123
    assert to_int(0) == 0
    assert to_int(MAX_SIGNED_64BIT) == MAX_SIGNED_64BIT
    assert to_int(MIN_SIGNED_64BIT) == MIN_SIGNED_64BIT
    assert to_int(NULL_FLAG) == MIN_SIGNED_64BIT
    assert to_int(MIN_SIGNED_64BIT - 1) == MIN_SIGNED_64BIT
    assert to_int(MAX_SIGNED_64BIT + 1) == MAX_SIGNED_64BIT

def test_to_int_bounds():
    assert to_int(2**80) == MAX_SIGNED_64BIT
    assert to_int(-(2**80)) == MIN_SIGNED_64BIT

def test_to_int_floats():
    assert to_int(123.456) == 123
    assert to_int(-123.456) == -123
    assert to_int(float("inf")) == MAX_SIGNED_64BIT
    assert to_int(float("-inf")) == MIN_SIGNED_64BIT
    assert to_int(float("nan")) == NULL_FLAG

def test_to_int_datetime():
    dt = datetime.datetime(2020, 1, 1, 12, 0)
    assert to_int(dt) == int(dt.timestamp())

def test_to_int_date():
    d = datetime.date(2020, 1, 1)
    assert to_int(d) == int(datetime.datetime(2020, 1, 1).timestamp())

def test_to_int_time():
    t = datetime.time(1, 2, 3)
    assert to_int(t) == 3723

def test_to_int_decimal():
    assert to_int(Decimal("123.9")) == 124
    assert to_int(Decimal("-123.4")) == -123

def test_to_int_str_bytes():
    assert to_int("abc") == to_int(b"abc")
    assert to_int("abcdefg") == to_int(b"abcdefg")
    assert to_int("abcdefghi") == to_int(b"abcdefgh")  # Truncate

def test_to_int_invalid():
    assert to_int(object()) == NULL_FLAG

def test_relation_statistics_update_bounds():
    stats = RelationStatistics()
    stats.update_lower("col", 100)
    stats.update_lower("col", 50)
    stats.update_upper("col", 200)
    stats.update_upper("col", 250)
    stats.update_upper("col", 220)
    assert to_int(50) == stats.lower_bounds[b"col"]
    assert to_int(250) == stats.upper_bounds[b"col"]

def test_relation_statistics_add_nulls():
    stats = RelationStatistics()
    stats.add_null("col", 3)
    stats.add_null("col", 2)
    assert stats.null_count[b"col"] == 5

def test_relation_statistics_cardinality():
    stats = RelationStatistics()
    stats.set_cardinality_estimate("col", 42)
    assert stats.cardinality_estimate[b"col"] == 42

def test_relation_statistics_serialization_round_trip():
    stats = RelationStatistics()
    stats.record_count = 123
    stats.record_count_estimate = 150
    stats.add_null("a", 5)
    stats.update_lower("a", 10)
    stats.update_upper("a", 100)
    stats.set_cardinality_estimate("a", 3)

    b = stats.to_bytes()
    restored = RelationStatistics.from_bytes(b)

    assert restored.record_count == 123
    assert restored.record_count_estimate == 150
    assert restored.null_count[b"a"] == 5
    assert restored.lower_bounds[b"a"] == to_int(10)
    assert restored.upper_bounds[b"a"] == to_int(100)
    assert restored.cardinality_estimate[b"a"] == 3

def test_serialization_stress_many_keys():
    stats = RelationStatistics()
    stats.record_count = 2**40
    stats.record_count_estimate = 2**41

    # Add hundreds of entries with edge-case values
    for _ in range(500):
        col = random_key()
        v = random.randint(MIN_SIGNED_64BIT, MAX_SIGNED_64BIT)
        stats.add_null(col, random.randint(0, 100))
        stats.update_lower(col, v)
        stats.update_upper(col, v + 100)
        stats.set_cardinality_estimate(col, random.randint(1, 1000))

    b = stats.to_bytes()
    restored = RelationStatistics.from_bytes(b)

    assert restored.record_count == stats.record_count
    assert restored.record_count_estimate == stats.record_count_estimate
    assert restored.null_count == stats.null_count
    assert restored.lower_bounds == stats.lower_bounds
    assert restored.upper_bounds == stats.upper_bounds
    assert restored.cardinality_estimate == stats.cardinality_estimate

def test_serialization_unicode_keys_and_zero_values():
    stats = RelationStatistics()
    keys = ["naïve", "Δcol", "ключ", "列", "🔥"]
    for i, k in enumerate(keys):
        stats.add_null(k, 0)
        stats.update_lower(k, 0)
        stats.update_upper(k, 0)
        stats.set_cardinality_estimate(k, 0)

    b = stats.to_bytes()
    restored = RelationStatistics.from_bytes(b)

    for k in keys:
        skey = k.encode()
        assert restored.null_count[skey] == 0
        assert restored.lower_bounds[skey] == 0
        assert restored.upper_bounds[skey] == 0
        assert restored.cardinality_estimate[skey] == 0

def test_serialization_round_trip_repeated():
    stats = RelationStatistics()
    stats.record_count = 987654321
    stats.record_count_estimate = 123456789
    stats.add_null("x", 1)
    stats.update_lower("x", -123)
    stats.update_upper("x", 123)
    stats.set_cardinality_estimate("x", 3)

    for _ in range(1000):
        b = stats.to_bytes()
        stats = RelationStatistics.from_bytes(b)

    assert stats.record_count == 987654321
    assert stats.lower_bounds[b"x"] == -123
    assert stats.upper_bounds[b"x"] == 123

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()

    quit()

    import time
    start_time = time.perf_counter_ns()
    for i in range(1000):
        run_tests()
    end_time = time.perf_counter_ns()
    print(f"Tests completed in {(end_time - start_time) / 1e9}")
