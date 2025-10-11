
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import psutil
import pytest

from opteryx.config import memory_allocation_calculation
from opteryx.config import parse_yaml
from opteryx.config import get

def test_memory_allocation_calculation_percentage():
    total_memory = psutil.virtual_memory().total
    assert memory_allocation_calculation(0.5) == int(total_memory * 0.5)

def test_memory_allocation_calculation_absolute():
    assert memory_allocation_calculation(32) == 32 * 1024 * 1024

def test_memory_allocation_calculation_invalid():
    with pytest.raises(ValueError):
        memory_allocation_calculation(-1)


def test_get_default_value():
    assert get("NON_EXISTENT_KEY", default="default_value") == "default_value"



if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
