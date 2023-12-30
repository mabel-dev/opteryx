import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.exceptions import InvalidConfigurationError
from opteryx.managers.cache.cache_manager import CacheManager
from opteryx.managers.kvstores import BaseKeyValueStore


class DummyKVStore(BaseKeyValueStore):
    pass


# Test when cache_backend is None
def test_cache_manager_init_with_none_backend():
    cm = CacheManager(cache_backend=None)
    assert cm.cache_backend is None


# Test with a valid KeyValue Store
def test_cache_manager_init_with_valid_kvstore():
    kv = DummyKVStore(location=None)
    cm = CacheManager(cache_backend=kv)
    assert cm.cache_backend == kv


# Test with an invalid KeyValue Store
def test_cache_manager_init_with_invalid_kvstore():
    with pytest.raises(InvalidConfigurationError) as e:
        CacheManager(cache_backend="not_a_kv_store")
    assert e.value.config_item == "cache_backend"


# Test with invalid max_cacheable_item_size
def test_cache_manager_init_invalid_max_cacheable_item_size():
    kv = DummyKVStore(location=None)
    with pytest.raises(InvalidConfigurationError) as e:
        CacheManager(cache_backend=kv, max_cacheable_item_size=-1)
    assert e.value.config_item == "max_cacheable_item_size"


# Test with invalid max_evictions_per_query
def test_cache_manager_init_invalid_max_evictions_per_query():
    kv = DummyKVStore(location=None)
    with pytest.raises(InvalidConfigurationError) as e:
        CacheManager(cache_backend=kv, max_evictions_per_query=0)
    assert e.value.config_item == "max_evictions_per_query"


# Test with invalid max_local_buffer_capacity
def test_cache_manager_init_invalid_max_local_buffer_capacity():
    kv = DummyKVStore(location=None)
    with pytest.raises(InvalidConfigurationError) as e:
        CacheManager(cache_backend=kv, max_local_buffer_capacity="string_instead_of_int")
    assert e.value.config_item == "max_local_buffer_capacity"


# Test when all parameters are correctly set
def test_cache_manager_init_with_all_parameters():
    kv = DummyKVStore(location=None)
    cm = CacheManager(
        cache_backend=kv,
        max_cacheable_item_size=1000,
        max_evictions_per_query=10,
        max_local_buffer_capacity=50,
    )
    assert cm.cache_backend == kv
    assert cm.max_cacheable_item_size == 1000
    assert cm.max_evictions_per_query == 10
    assert cm.max_local_buffer_capacity == 50


# Test when max_cacheable_item_size is None
def test_cache_manager_init_with_max_cacheable_item_size_none():
    kv = DummyKVStore(location=None)
    cm = CacheManager(cache_backend=kv, max_cacheable_item_size=None)
    assert cm.max_cacheable_item_size is None


# Test when max_evictions_per_query is None
def test_cache_manager_init_with_max_evictions_per_query_none():
    kv = DummyKVStore(location=None)
    cm = CacheManager(cache_backend=kv, max_evictions_per_query=None)
    assert cm.max_evictions_per_query is None


# Test when max_local_buffer_capacity is zero or negative
def test_cache_manager_init_with_invalid_local_buffer_capacity():
    kv = DummyKVStore(location=None)
    with pytest.raises(InvalidConfigurationError) as e:
        CacheManager(cache_backend=kv, max_local_buffer_capacity=0)
    assert e.value.config_item == "max_local_buffer_capacity"
    with pytest.raises(InvalidConfigurationError) as e:
        CacheManager(cache_backend=kv, max_local_buffer_capacity=-1)
    assert e.value.config_item == "max_local_buffer_capacity"


# Test when an unknown parameter is passed
def test_cache_manager_init_with_unknown_param():
    kv = DummyKVStore(location=None)
    with pytest.raises(TypeError):
        CacheManager(cache_backend=kv, unknown_param=100)


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
