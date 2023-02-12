import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.config import parse_yaml


def test_parse_string():
    yaml_string = "name: John Doe"
    expected_output = {"name": "John Doe"}

    # Call the YAML parser function
    output = parse_yaml(yaml_string)

    assert output == expected_output, output


def test_parse_number():
    yaml_string = """
    age: 30
    height: 1.83
    ip: 10.10.10.10
    """
    expected_output = {"age": 30, "height": 1.83, "ip": "10.10.10.10"}

    # Call the YAML parser function
    output = parse_yaml(yaml_string)

    assert output == expected_output, output


def test_parse_list():
    yaml_string = "hobbies: [reading, writing, hiking]"
    expected_output = {"hobbies": ["reading", "writing", "hiking"]}

    # Call the YAML parser function
    output = parse_yaml(yaml_string)

    assert output == expected_output, output


def __test_parse_nested_structure():
    yaml_string = """
    name: John Doe
    age: 30
    details:
        hobbies: [reading, writing, hiking]
        location: New York
    """
    expected_output = {
        "name": "John Doe",
        "age": 30,
        "details": {
            "hobbies": ["reading", "writing", "hiking"],
            "location": "New York",
        },
    }

    # Call the YAML parser function
    output = parse_yaml(yaml_string)

    assert output == expected_output, output


if __name__ == "__main__":  # pragma: no cover
    test_parse_string()
    test_parse_list()
    #    test_parse_nested_structure()
    test_parse_number()
    print("✅ okay")
