
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

def test_parse_yaml_basic():
    yaml_str = """
    key1: value1
    key2: 123
    key3: true
    key4: 4.56
    key5: [one, two, three]
    """
    result = parse_yaml(yaml_str)
    assert result == {
        "key1": "value1",
        "key2": 123,
        "key3": True,
        "key4": 4.56,
        "key5": ["one", "two", "three"],
    }

def test_parse_yaml_with_comments():
    yaml_str = """
    key1: value1 # this is a comment
    key2: 123
    # this is a whole line comment
    key3: true
    """
    result = parse_yaml(yaml_str)
    assert result == {
        "key1": "value1",
        "key2": 123,
        "key3": True,
    }


def test_get_default_value():
    assert get("NON_EXISTENT_KEY", default="default_value") == "default_value"


def test_parse_yaml_boolean_true():
    yaml_str = "key: true"
    result = parse_yaml(yaml_str)
    assert result == {"key": True}

def test_parse_yaml_boolean_false():
    yaml_str = "key: false"
    result = parse_yaml(yaml_str)
    assert result == {"key": False}

def test_parse_yaml_none_value():
    yaml_str = "key: none"
    result = parse_yaml(yaml_str)
    assert result == {"key": None}

def test_parse_yaml_integer_value():
    yaml_str = "key: 123"
    result = parse_yaml(yaml_str)
    assert result == {"key": 123}

def test_parse_yaml_float_value():
    yaml_str = "key: 4.56"
    result = parse_yaml(yaml_str)
    assert result == {"key": 4.56}

def test_parse_yaml_list_square_brackets():
    yaml_str = "key: [one, two, three]"
    result = parse_yaml(yaml_str)
    assert result == {"key": ["one", "two", "three"]}

def test_parse_yaml_list_dash():
    yaml_str = """
    key:
      - item1
      - item2
      - item3
    """
    result = parse_yaml(yaml_str)
    assert result == {"key": ["item1", "item2", "item3"]}



def test_parse_yaml_mixed_case_booleans():
    yaml_str = """
    key1: TrUe
    key2: FaLsE
    """
    result = parse_yaml(yaml_str)
    assert result == {"key1": True, "key2": False}

def test_parse_yaml_list_with_comments():
    yaml_str = """
    key:
      - item1 # first item
      - item2 # second item
      # a comment line
      - item3 # third item
    """
    result = parse_yaml(yaml_str)
    assert result == {"key": ["item1", "item2", "item3"]}

def test_parse_yaml_string_value_with_special_characters():
    yaml_str = "key: some_special_value!@"
    result = parse_yaml(yaml_str)
    assert result == {"key": "some_special_value!@"}, result

def test_parse_yaml_boolean_like_strings():
    yaml_str = """
    key1: "true"
    key2: "false"
    key3: "none"
    """
    result = parse_yaml(yaml_str)
    assert result == {"key1": '"true"', "key2": '"false"', "key3": '"none"'}, result

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
