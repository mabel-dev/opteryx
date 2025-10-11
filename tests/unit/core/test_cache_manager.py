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


# Test when all parameters are correctly set
def test_cache_manager_init_with_all_parameters():
    kv = DummyKVStore(location=None)
    cm = CacheManager(
        cache_backend=kv,
    )
    assert cm.cache_backend == kv


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
