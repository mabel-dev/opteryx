import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.planner.sql_rewriter import sql_parts

# Define the test cases as a list of (input, expected_output) tuples
# fmt:off
test_cases = [
    # Contrived cases
    ("This is a test string with b'abc' and B\"def\".", "This is a test string with CAST('abc' AS VARBINARY) and CAST(\"def\" AS VARBINARY)."),
    ("b'123' should become CAST('123' AS VARBINARY)", "CAST('123' AS VARBINARY) should become CAST('123' AS VARBINARY)"),
    ('B"xyz" should become CAST("xyz" AS VARBINARY)', 'CAST("xyz" AS VARBINARY) should become CAST("xyz" AS VARBINARY)'),
    ("Mix of b'one' and B\"two\"", "Mix of CAST('one' AS VARBINARY) and CAST(\"two\" AS VARBINARY)"),
    ("No prefixed strings here.", "No prefixed strings here."),
    ("B'' and b\"\" should be handled.", "CAST('' AS VARBINARY) and CAST(\"\" AS VARBINARY) should be handled."),

    # Basic replacements
    ("SELECT * FROM table WHERE column = b'abc';", "SELECT * FROM table WHERE column = CAST('abc' AS VARBINARY);"),
    ("INSERT INTO table (column) VALUES (B\"def\");", "INSERT INTO table (column) VALUES (CAST(\"def\" AS VARBINARY));"),
    ("UPDATE table SET column = b'123' WHERE id = 1;", "UPDATE table SET column = CAST('123' AS VARBINARY) WHERE id = 1;"),
    
    # Mixed cases
    ("SELECT * FROM table WHERE column = B'xyz' OR column = b\"uvw\";", "SELECT * FROM table WHERE column = CAST('xyz' AS VARBINARY) OR column = CAST(\"uvw\" AS VARBINARY);"),
    ("INSERT INTO table (col1, col2) VALUES (b'val1', B\"val2\");", "INSERT INTO table (col1, col2) VALUES (CAST('val1' AS VARBINARY), CAST(\"val2\" AS VARBINARY));"),

    # Edge cases
    ("SELECT * FROM table WHERE column = b'';", "SELECT * FROM table WHERE column = CAST('' AS VARBINARY);"),
    ("SELECT * FROM table WHERE column = B\"\";", "SELECT * FROM table WHERE column = CAST(\"\" AS VARBINARY);"),
    ("SELECT b'abc' AS col1, B'def' AS col2 FROM table;", "SELECT CAST('abc' AS VARBINARY) AS col1, CAST('def' AS VARBINARY) AS col2 FROM table;"),

    # No replacements
    ("SELECT * FROM table WHERE column = 'abc';", "SELECT * FROM table WHERE column = 'abc';"),
    ("SELECT * FROM table WHERE column = \"def\";", "SELECT * FROM table WHERE column = \"def\";"),
    ("SELECT * FROM table WHERE column = '';", "SELECT * FROM table WHERE column = '';"),
    ("SELECT * FROM table WHERE column = \"\";", "SELECT * FROM table WHERE column = \"\";"),

    # Complex statements
    ("SELECT * FROM table1 JOIN table2 ON table1.col = table2.col WHERE table1.col = b'join' AND table2.col = B\"join\";", "SELECT * FROM table1 JOIN table2 ON table1.col = table2.col WHERE table1.col = CAST('join' AS VARBINARY) AND table2.col = CAST(\"join\" AS VARBINARY);"),
    ("WITH cte AS (SELECT b'cte' AS col FROM table) SELECT * FROM cte WHERE col = B'cte';", "WITH cte AS (SELECT CAST('cte' AS VARBINARY) AS col FROM table) SELECT * FROM cte WHERE col = CAST('cte' AS VARBINARY);"),

    # Specific cases
    ("SELECT * FROM table WHERE column = blob'a';", "SELECT * FROM table WHERE column = blob'a';"),
    ("SELECT * FROM table WHERE column = CAST(\"a\" AS VARBINARY);", "SELECT * FROM table WHERE column = CAST(\"a\" AS VARBINARY);"),
    ("SELECT * FROM table WHERE column = CAST('a' AS VARBINARY);", "SELECT * FROM table WHERE column = CAST('a' AS VARBINARY);"),
    ("SELECT * FROM table WHERE column = b'abc' AND function_call(b'xyz');", "SELECT * FROM table WHERE column = CAST('abc' AS VARBINARY) AND function_call(CAST('xyz' AS VARBINARY));"),

    # failed case
    ("SELECT * FROM $satellites WHERE (((id = 5 OR (10<11)) AND ('a'='b')) OR (name = 'Europa' AND (TRUE AND (11=11))));", "SELECT * FROM $satellites WHERE (((id = 5 OR (10<11)) AND ('a'='b')) OR (name = 'Europa' AND (TRUE AND (11=11)))) ;"),

    # complex quotes
    ("SELECT * FROM table WHERE column = 'This is a ''test'' string';", "SELECT * FROM table WHERE column = 'This is a ''test'' string';"),
    ("SELECT * FROM table WHERE column = \"He said, \\\"Hello, World!\\\"\";", "SELECT * FROM table WHERE column = \"He said, \\\"Hello, World!\\\"\";"),
    ("SELECT * FROM table WHERE column = 'Single quote within '' single quotes';", "SELECT * FROM table WHERE column = 'Single quote within '' single quotes';"),
    ("SELECT * FROM table WHERE column = \"Double quote within \\\" double quotes\";", "SELECT * FROM table WHERE column = \"Double quote within \\\" double quotes\";"),
    ("SELECT * FROM table WHERE column = `Backticks are used for column names`;", "SELECT * FROM table WHERE column = `Backticks are used for column names`;"),
    ("SELECT * FROM table WHERE column = 'Multiple ''single quotes'' in one string';", "SELECT * FROM table WHERE column = 'Multiple ''single quotes'' in one string';"),
    ("SELECT * FROM table WHERE column = \"Multiple \\\"double quotes\\\" in one string\";", "SELECT * FROM table WHERE column = \"Multiple \\\"double quotes\\\" in one string\";"),
    ("SELECT * FROM table WHERE column = 'Combination of ''single'' and \"double\" quotes';", "SELECT * FROM table WHERE column = 'Combination of ''single'' and \"double\" quotes';"),
    ("SELECT * FROM table WHERE column = 'String with newline\ncharacter';", "SELECT * FROM table WHERE column = 'String with newline\ncharacter';")
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

    print(f"RUNNING BATTERY OF {len(test_cases)} B STRINGS")
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
