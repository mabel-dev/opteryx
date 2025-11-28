import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from types import SimpleNamespace

from opteryx.connectors.capabilities.statistics import Statistics
from opteryx.compiled.structures.relation_statistics import RelationStatistics
from opteryx.third_party.cyan4973.xxhash import hash_bytes
from opteryx.managers.expression import NodeType
from opteryx.models import Node, LogicalColumn
from orso.types import OrsoTypes
from decimal import Decimal
from opteryx.models import QueryStatistics
import numpy as np


def build_anyop_condition(operator: str, literal_value, column_name: str, literal_type=OrsoTypes.INTEGER):
    cond = Node(node_type=NodeType.COMPARISON_OPERATOR, value=operator)
    # left is literal
    cond.left = Node(node_type=NodeType.LITERAL)
    cond.left.value = literal_value
    # attach schema type for LITERAL
    cond.left.schema_column = SimpleNamespace(type=literal_type)
    # right is identifier
    cond.right = Node(node_type=NodeType.IDENTIFIER)
    cond.right.source_column = column_name
    cond.right.schema_column = SimpleNamespace(type=literal_type)
    return cond


def get_cache_key(blob_name: str) -> bytes:
    from opteryx.third_party.cyan4973.xxhash import hash_bytes

    return hex(hash_bytes(blob_name.encode())).encode()


def test_prune_anyop_eq_outside_range():
    stats = Statistics({})
    # reset the stats cache between tests
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob1.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 10)
    rs.update_upper("arr", 20)
    Stats.set(key, rs)

    # literal 5 is below min 10, should prune
    cond = build_anyop_condition("AnyOpEq", 5, "arr")
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-outside")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == []
    assert qs.blobs_pruned == 1


def test_prune_anyop_eq_inside_range_not_pruned():
    stats = Statistics({})
    # reset the stats cache between tests
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob2.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 10)
    rs.update_upper("arr", 20)
    Stats.set(key, rs)

    # literal 15 is inside [10,20], should not prune
    cond = build_anyop_condition("AnyOpEq", 15, "arr")
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-inside")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == [blob_name]
    assert qs.blobs_pruned == 0


def test_anyop_gt_does_not_prune_unless_supported():
    # Ensure we do not prune AnyOpGt because we only support AnyOpEq pruning via min/max
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob3.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 10)
    rs.update_upper("arr", 20)
    Stats.set(key, rs)

    # AnyOpGt should not be pruned by our new rule, as it's not implemented
    cond = build_anyop_condition("AnyOpGt", 25, "arr")
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-anyop-gt")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == [blob_name]
    assert qs.blobs_pruned == 0


def test_prune_anyop_eq_prunes_gt_max():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob4.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 10)
    rs.update_upper("arr", 20)
    Stats.set(key, rs)

    # literal 25 > 20 => prune
    cond = build_anyop_condition("AnyOpEq", 25, "arr")
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-gtmax")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == []
    assert qs.blobs_pruned == 1


def test_prune_anyop_eq_no_bounds_not_pruned():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob5.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    # Only set a lower bound
    rs.update_lower("arr", 10)
    Stats.set(key, rs)

    cond = build_anyop_condition("AnyOpEq", 5, "arr")
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-nobounds")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == [blob_name]
    assert qs.blobs_pruned == 0


def test_prune_anyop_eq_string_types():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob6.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", "alpha")
    rs.update_upper("arr", "omega")
    Stats.set(key, rs)

    # 'aardvark' < 'alpha' => prune
    cond = build_anyop_condition("AnyOpEq", "aardvark", "arr", OrsoTypes.VARCHAR)
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-string")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == []
    assert qs.blobs_pruned == 1


def test_prune_anyop_eq_bytes_types():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob7.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", b"alpha")
    rs.update_upper("arr", b"omega")
    Stats.set(key, rs)

    # bytes literal below range should prune
    cond = build_anyop_condition("AnyOpEq", b"aardvark", "arr", OrsoTypes.BLOB)
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-bytes")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == []
    assert qs.blobs_pruned == 1


def test_prune_anyop_eq_numpy_numeric_types():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob8.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 10)
    rs.update_upper("arr", 20)
    Stats.set(key, rs)

    cond = build_anyop_condition("AnyOpEq", np.int64(5), "arr")
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-numpy")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == []
    assert qs.blobs_pruned == 1


def test_prune_anyop_eq_epoch_int_timestamp():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob9.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    # epoch integer bounds for dates Jan 1 to Feb 1 2021
    rs.update_lower("arr", 1609459200)
    rs.update_upper("arr", 1612137600)
    Stats.set(key, rs)

    # literal before lower bound
    literal = 1609372800
    cond = build_anyop_condition("AnyOpEq", literal, "arr", OrsoTypes.INTEGER)
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-numpy-datetime64")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == []
    assert qs.blobs_pruned == 1


def test_prune_anyop_eq_long_string_truncation_not_pruned():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob11.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    # 7-character bounds; long literal truncated to 7 bytes should not allow prune
    rs.update_lower("arr", "abcdefg")
    rs.update_upper("arr", "abcdefg")
    Stats.set(key, rs)

    literal = "abcdefgh"
    cond = build_anyop_condition("AnyOpEq", literal, "arr", OrsoTypes.VARCHAR)
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-long-truncation")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    # The literal truncates to the same as max 'abcdefg', so it should not be pruned
    assert new_blobs == [blob_name]
    assert qs.blobs_pruned == 0


def test_prune_anyop_eq_null_literal_not_pruned():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob10.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 10)
    rs.update_upper("arr", 20)
    Stats.set(key, rs)

    cond = build_anyop_condition("AnyOpEq", None, "arr")
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-null-literal")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == [blob_name]
    assert qs.blobs_pruned == 0


def test_prune_anyop_eq_negative_numbers():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob12.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", -100)
    rs.update_upper("arr", -10)
    Stats.set(key, rs)

    # literal -200 less than min (-100) => prune
    cond = build_anyop_condition("AnyOpEq", -200, "arr")
    reserved = [blob_name]
    qs = QueryStatistics(qid="test-prune-negative")
    new_blobs = stats.prune_blobs(reserved, qs, [cond])
    assert new_blobs == []
    assert qs.blobs_pruned == 1


def test_prune_anyop_eq_float_inf_nan():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob13.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 10)
    rs.update_upper("arr", 20)
    Stats.set(key, rs)

    # float('inf') should be prunable (converted to max int)
    cond_inf = build_anyop_condition("AnyOpEq", float("inf"), "arr")
    qs_inf = QueryStatistics(qid="test-prune-float-inf")
    res_inf = stats.prune_blobs([blob_name], qs_inf, [cond_inf])
    assert res_inf == []
    assert qs_inf.blobs_pruned == 1

    # float('nan') returns NULL_FLAG via to_int and should not be prunable
    cond_nan = build_anyop_condition("AnyOpEq", float("nan"), "arr")
    qs_nan = QueryStatistics(qid="test-prune-float-nan")
    res_nan = stats.prune_blobs([blob_name], qs_nan, [cond_nan])
    assert res_nan == [blob_name]
    assert qs_nan.blobs_pruned == 0


def test_prune_anyop_eq_decimal_rounding():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob14.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 100)
    rs.update_upper("arr", 200)
    Stats.set(key, rs)

    # Decimal 99.9 rounds to 100 => not pruned
    cond = build_anyop_condition("AnyOpEq", Decimal("99.9"), "arr")
    qs = QueryStatistics(qid="test-prune-decimal-round")
    new_blobs = stats.prune_blobs([blob_name], qs, [cond])
    assert new_blobs == [blob_name]
    assert qs.blobs_pruned == 0


def test_prune_anyop_eq_multiblob_selectors():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob1 = "testdata/blob15.parquet"
    blob2 = "testdata/blob16.parquet"
    key1 = get_cache_key(blob1)
    key2 = get_cache_key(blob2)

    rs1 = RelationStatistics()
    rs1.update_lower("arr", 10)
    rs1.update_upper("arr", 20)
    Stats.set(key1, rs1)

    rs2 = RelationStatistics()
    rs2.update_lower("arr", 0)
    rs2.update_upper("arr", 100)
    Stats.set(key2, rs2)

    # literal 25 will prune blob1 (10-20) but not blob2
    cond = build_anyop_condition("AnyOpEq", 25, "arr")
    qs = QueryStatistics(qid="test-prune-multi")
    new_blobs = stats.prune_blobs([blob1, blob2], qs, [cond])
    assert new_blobs == [blob2]
    assert qs.blobs_pruned == 1


def test_prune_anyop_not_eq_not_pruned():
    # AnyOpNotEq is not supported - ensure we don't prune
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob17.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 10)
    rs.update_upper("arr", 20)
    Stats.set(key, rs)

    cond = build_anyop_condition("AnyOpNotEq", 5, "arr")
    qs = QueryStatistics(qid="test-prune-not-eq")
    new_blobs = stats.prune_blobs([blob_name], qs, [cond])
    assert new_blobs == [blob_name]
    assert qs.blobs_pruned == 0


def test_prune_anyop_and_eq_mixed_conditions_prune():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob18.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr1", 10)
    rs.update_upper("arr1", 20)
    rs.update_lower("arr2", 100)
    rs.update_upper("arr2", 200)
    Stats.set(key, rs)

    # AnyOpEq prunes arr1; Eq for arr2 won't prune
    cond1 = build_anyop_condition("AnyOpEq", 25, "arr1")
    # Also include a matching Eq condition that doesn't prune (identifier left)
    eq_cond = Node(node_type=NodeType.COMPARISON_OPERATOR, value="Eq")
    eq_cond.left = Node(node_type=NodeType.IDENTIFIER)
    eq_cond.left.source_column = "arr2"
    eq_cond.left.schema_column = SimpleNamespace(type=OrsoTypes.INTEGER)
    eq_cond.right = Node(node_type=NodeType.LITERAL)
    eq_cond.right.value = 150
    eq_cond.right.schema_column = SimpleNamespace(type=OrsoTypes.INTEGER)

    qs = QueryStatistics(qid="test-prune-mixed")
    new_blobs = stats.prune_blobs([blob_name], qs, [cond1, eq_cond])
    assert new_blobs == []
    assert qs.blobs_pruned == 1


def test_prune_multiple_anyop_conditions():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob19.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr1", 0)
    rs.update_upper("arr1", 5)
    rs.update_lower("arr2", 10)
    rs.update_upper("arr2", 20)
    Stats.set(key, rs)

    # First AnyOpEq prunes arr1 but second doesn't for arr2
    cond1 = build_anyop_condition("AnyOpEq", 10, "arr1")
    cond2 = build_anyop_condition("AnyOpEq", 15, "arr2")
    qs = QueryStatistics(qid="test-prune-multiple-anyop")
    new_blobs = stats.prune_blobs([blob_name], qs, [cond1, cond2])
    assert new_blobs == []
    assert qs.blobs_pruned == 1


def test_prune_anyop_eq_unicode_multibyte_truncation():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob20.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", "α")
    rs.update_upper("arr", "ω")
    Stats.set(key, rs)

    # Greek letter 'β' in range, but multi-byte truncation shouldn't cause prune
    cond = build_anyop_condition("AnyOpEq", "β", "arr", OrsoTypes.VARCHAR)
    qs = QueryStatistics(qid="test-prune-unicode")
    new_blobs = stats.prune_blobs([blob_name], qs, [cond])
    assert new_blobs == [blob_name]
    assert qs.blobs_pruned == 0


def test_prune_anyop_eq_large_int_clamp():
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob21.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 10)
    rs.update_upper("arr", 20)
    Stats.set(key, rs)

    # Large int beyond 64-bit -> to_int clamps to MAX, so prune
    cond = build_anyop_condition("AnyOpEq", 2 ** 80, "arr")
    qs = QueryStatistics(qid="test-prune-largeint")
    new_blobs = stats.prune_blobs([blob_name], qs, [cond])
    assert new_blobs == []
    assert qs.blobs_pruned == 1


def test_prune_anyop_eq_timestamp_type_not_pruned():
    # Our code excludes timestamp types from pruning, make sure we skip
    stats = Statistics({})
    from opteryx.shared.stats_cache import StatsCache
    StatsCache.reset()
    Stats = stats.stats_cache

    blob_name = "testdata/blob22.parquet"
    key = get_cache_key(blob_name)

    rs = RelationStatistics()
    rs.update_lower("arr", 1609459200)  # epoch
    rs.update_upper("arr", 1612137600)
    Stats.set(key, rs)

    # Explicit TIMESTAMP type should be excluded from prune checks
    cond = build_anyop_condition("AnyOpEq", 1609372800, "arr", OrsoTypes.TIMESTAMP)
    qs = QueryStatistics(qid="test-prune-timestamp")
    new_blobs = stats.prune_blobs([blob_name], qs, [cond])
    assert new_blobs == [blob_name]
    assert qs.blobs_pruned == 0

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
