import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.planner.sql_rewriter import sql_parts

# Define the test cases as a list of (input, expected_output) tuples
# fmt:off
test_cases = [
    ("This is a test string with r'abc' and R\"def\".", "This is a test string with BASE64_DECODE('YWJj') and BASE64_DECODE('ZGVm')."),
    ("r'123' should become BASE64_DECODE('F)}j')", "BASE64_DECODE('MTIz') should become BASE64_DECODE('F)}j')"),
    ('R"xyz" should become BASE64_DECODE(\'czJp\')', 'BASE64_DECODE(\'eHl6\') should become BASE64_DECODE(\'czJp\')'),
    ("Mix of r'one' and R\"two\"", "Mix of BASE64_DECODE('b25l') and BASE64_DECODE('dHdv')"),
    ("No prefixed strings here.", "No prefixed strings here."),
    ("R'' and r\"\" should be handled.", "BASE64_DECODE('') and BASE64_DECODE('') should be handled."),
    ("I am escaping r'\\1' and R'\\1'", "I am escaping BASE64_DECODE('XDE=') and BASE64_DECODE('XDE=')"),
    ("I am also escaping r'\1' and r'\1'", "I am also escaping BASE64_DECODE('AQ==') and BASE64_DECODE('AQ==')"),
]
# fmt:on


@pytest.mark.parametrize("input_text, expected_output", test_cases)
def test_replace_b_strings(input_text, expected_output):
    assert " ".join(sql_parts(input_text)).replace(" ", "") == expected_output.replace(" ", "")


if __name__ == "__main__":  # pragma: no cover
    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil
    import time

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(test_cases)} R(aw) STRINGS")
    for index, (case, expected) in enumerate(test_cases):
        start = time.monotonic_ns()
        print(
            f"\033[0;36m{(index + 1):04}\033[0m {case[0:width - 1].ljust(width)}",
            end="",
        )
        if " ".join(sql_parts(case)).replace(" ", "") == expected.replace(" ", ""):
            print(f"\033[0;32m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅")
        else:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ❌")
            print("Expected:", expected)
            print("Recieved:", " ".join(sql_parts(case)))

    print("--- ✅ \033[0;32mdone\033[0m")
